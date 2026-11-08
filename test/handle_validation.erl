%%% -*- erlang -*-
%%%
%%% Copyright (c) 2026 Benoit Chesneau
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%% http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

%% Handles passed to the wrong place: released resources, and column
%% families of another database.
-module(handle_validation).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% release_statistics/1 destroyed the object while the handle stayed valid:
%% the next call aborted the VM
released_statistics_test() ->
  {ok, S} = rocksdb:new_statistics(),
  with_db("stats", [{statistics, S}], fun(Db) ->
    ok = rocksdb:release_statistics(S),
    ok = rocksdb:release_statistics(S),
    ?assertError(badarg, rocksdb:statistics_info(S)),
    ?assertError(badarg, rocksdb:statistics_ticker(S, keys_written)),
    %% the database keeps its own reference
    ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
    {ok, <<"v">>} = rocksdb:get(Db, <<"k">>, [])
  end).

%% release_rate_limiter/1 destroyed the object directly, an unreleased one
%% was never freed
released_rate_limiter_test() ->
  {ok, L} = rocksdb:new_rate_limiter(1024 * 1024, false),
  with_db("rate_limiter", [{rate_limiter, L}], fun(Db) ->
    ok = rocksdb:release_rate_limiter(L),
    ok = rocksdb:release_rate_limiter(L),
    ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
    ok = rocksdb:flush(Db, [])
  end),
  %% dropped without release: freed by garbage collection
  [begin {ok, _} = rocksdb:new_rate_limiter(1024, false) end || _ <- lists:seq(1, 100)],
  erlang:garbage_collect().

%% the release functions of caches and managers did nothing
released_cache_and_managers_test() ->
  {ok, Cache} = rocksdb:new_cache(lru, 1024 * 1024),
  {ok, Env} = rocksdb:new_env(),
  {ok, Sst} = rocksdb:new_sst_file_manager(Env),
  {ok, Wbm} = rocksdb:new_write_buffer_manager(1024 * 1024),
  with_db("managers", [{sst_file_manager, Sst}, {write_buffer_manager, Wbm},
                       {block_based_table_options, [{block_cache, Cache}]}],
    fun(Db) ->
      ok = rocksdb:release_cache(Cache),
      ok = rocksdb:release_sst_file_manager(Sst),
      ok = rocksdb:release_write_buffer_manager(Wbm),
      ?assertError(badarg, rocksdb:cache_info(Cache)),
      ?assertError(badarg, rocksdb:sst_file_manager_info(Sst)),
      ?assertError(badarg, rocksdb:write_buffer_manager_info(Wbm)),
      %% releasing twice is fine, the database keeps working
      ok = rocksdb:release_cache(Cache),
      ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
      ok = rocksdb:flush(Db, []),
      {ok, <<"v">>} = rocksdb:get(Db, <<"k">>, [])
    end).

%% release_sst_file_writer/1 and release_sst_file_reader/1 did nothing
released_sst_writer_and_reader_test() ->
  Dir = test_dir("sst"),
  Path = filename:join(Dir, "data.sst"),
  {ok, W} = rocksdb:sst_file_writer_open([], Path),
  ok = rocksdb:sst_file_writer_put(W, <<"a">>, <<"1">>),
  ok = rocksdb:sst_file_writer_finish(W),
  ok = rocksdb:release_sst_file_writer(W),
  ?assertError(badarg, rocksdb:sst_file_writer_put(W, <<"b">>, <<"2">>)),
  ok = rocksdb:release_sst_file_writer(W),

  {ok, R} = rocksdb:sst_file_reader_open([], Path),
  {ok, It} = rocksdb:sst_file_reader_iterator(R, []),
  ok = rocksdb:release_sst_file_reader(R),
  ?assertError(badarg, rocksdb:sst_file_reader_get_table_properties(R)),
  ?assertError(badarg, rocksdb:sst_file_reader_iterator(R, [])),
  %% an open iterator keeps reading: the reader goes when it is closed
  {ok, <<"a">>, <<"1">>} = rocksdb:sst_file_reader_iterator_move(It, first),
  ok = rocksdb:sst_file_reader_iterator_close(It),
  ok = rocksdb:release_sst_file_reader(R),
  ?rm_rf(Dir).

%% RocksDB finds column families by id: a handle of another database wrote
%% into the column family with the same id in this one
foreign_column_family_test() ->
  with_two_dbs(fun(A, [DefA, OtherA], B, [DefB, OtherB]) ->
    ?assertError(badarg, rocksdb:put(A, DefB, <<"k">>, <<"v">>, [])),
    ?assertError(badarg, rocksdb:put(A, OtherB, <<"k">>, <<"v">>, [])),
    ?assertError(badarg, rocksdb:get(A, OtherB, <<"k">>, [])),
    ?assertError(badarg, rocksdb:delete(A, OtherB, <<"k">>, [])),
    ?assertError(badarg, rocksdb:iterator(A, OtherB, [])),
    ?assertError(badarg, rocksdb:put_entity(A, OtherB, <<"k">>, [{<<"c">>, <<"v">>}], [])),
    not_found = rocksdb:get(A, OtherA, <<"k">>, []),
    not_found = rocksdb:get(A, DefA, <<"k">>, []),
    %% own column families still work
    ok = rocksdb:put(B, OtherB, <<"k">>, <<"v">>, []),
    {ok, <<"v">>} = rocksdb:get(B, OtherB, <<"k">>, []),
    ok
  end).

foreign_column_family_in_transaction_test() ->
  Dir = test_dir("tx_foreign"),
  {ok, A, [_, _]} = rocksdb:open_pessimistic_transaction_db(
                      filename:join(Dir, "a"),
                      [{create_if_missing, true}, {create_missing_column_families, true}],
                      [{"default", []}, {"other", []}]),
  {ok, B, [_, OtherB]} = rocksdb:open_pessimistic_transaction_db(
                           filename:join(Dir, "b"),
                           [{create_if_missing, true}, {create_missing_column_families, true}],
                           [{"default", []}, {"other", []}]),
  {ok, Tx} = rocksdb:pessimistic_transaction(A, []),
  ?assertError(badarg, rocksdb:pessimistic_transaction_put(Tx, OtherB, <<"k">>, <<"v">>)),
  ?assertError(badarg, rocksdb:pessimistic_transaction_get(Tx, OtherB, <<"k">>, [])),
  ok = rocksdb:release_pessimistic_transaction(Tx),
  ok = rocksdb:close(A),
  ok = rocksdb:close(B),
  ?rm_rf(Dir).

%% a batch must not mix databases nor be written to another one
foreign_column_family_in_batch_test() ->
  with_two_dbs(fun(A, [_, OtherA], B, [_, OtherB]) ->
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, OtherA, <<"k">>, <<"v">>),
    ?assertError(badarg, rocksdb:batch_put(Batch, OtherB, <<"k">>, <<"v">>)),
    ?assertError(badarg, rocksdb:write_batch(B, Batch, [])),
    ok = rocksdb:write_batch(A, Batch, []),
    {ok, <<"v">>} = rocksdb:get(A, OtherA, <<"k">>, []),
    not_found = rocksdb:get(B, OtherB, <<"k">>, []),
    %% once written the batch is empty and can be used with any database
    ok = rocksdb:batch_put(Batch, OtherB, <<"k2">>, <<"v">>),
    ok = rocksdb:write_batch(B, Batch, []),
    {ok, <<"v">>} = rocksdb:get(B, OtherB, <<"k2">>, []),
    ok = rocksdb:release_batch(Batch)
  end).

%% an invalid column family silently read the properties of the database
get_property_bad_column_family_test() ->
  with_two_dbs(fun(A, [_, OtherA], _B, [_, OtherB]) ->
    ?assertError(badarg, rocksdb:get_property(A, make_ref(), <<"rocksdb.estimate-num-keys">>)),
    ?assertError(badarg, rocksdb:get_property(A, OtherB, <<"rocksdb.estimate-num-keys">>)),
    {ok, _} = rocksdb:get_property(A, OtherA, <<"rocksdb.estimate-num-keys">>),
    ok = rocksdb:destroy_column_family(A, OtherA),
    ?assertError(badarg, rocksdb:get_property(A, OtherA, <<"rocksdb.estimate-num-keys">>))
  end).

%% number_files reported the backup id
backup_number_files_test() ->
  Dir = test_dir("backup"),
  {ok, Db} = rocksdb:open(filename:join(Dir, "db"), [{create_if_missing, true}]),
  [ok = rocksdb:put(Db, <<I:32>>, <<I:32>>, []) || I <- lists:seq(1, 100)],
  ok = rocksdb:flush(Db, []),
  {ok, Engine} = rocksdb:open_backup_engine(filename:join(Dir, "backup")),
  ok = rocksdb:create_new_backup(Engine, Db),
  {ok, [#{backup_id := 1, number_files := N}]} = rocksdb:get_backup_info(Engine),
  ?assert(N > 1),
  ok = rocksdb:close_backup_engine(Engine),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% helpers

with_two_dbs(Fun) ->
  Dir = test_dir("two_dbs"),
  Opts = [{create_if_missing, true}, {create_missing_column_families, true}],
  Cfs = [{"default", []}, {"other", []}],
  {ok, A, CfsA} = rocksdb:open(filename:join(Dir, "a"), Opts, Cfs),
  {ok, B, CfsB} = rocksdb:open(filename:join(Dir, "b"), Opts, Cfs),
  try
    Fun(A, CfsA, B, CfsB)
  after
    rocksdb:close(A),
    rocksdb:close(B),
    ?rm_rf(Dir)
  end.

with_db(Name, Opts, Fun) ->
  Dir = test_dir(Name),
  {ok, Db} = rocksdb:open(filename:join(Dir, "db"), [{create_if_missing, true} | Opts]),
  try
    Fun(Db)
  after
    rocksdb:close(Db),
    ?rm_rf(Dir)
  end.

test_dir(Name) ->
  Dir = "erocksdb.handle_validation." ++ Name ++ ".test",
  ?rm_rf(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "x")),
  Dir.
