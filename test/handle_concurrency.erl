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

%% Handles shared between processes: concurrent calls on one transaction,
%% iterator or SST writer, a column family destroyed while in use, and
%% handles created while their database closes.
-module(handle_concurrency).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).
-define(WORKERS, 8).

%% destroy_column_family freed the handle while other calls still used it
destroy_cf_while_in_use_test_() ->
  {timeout, 120, fun() ->
    [destroy_cf_while_in_use(N) || N <- lists:seq(1, 20)]
  end}.

destroy_cf_while_in_use(N) ->
  Dir = test_dir("destroy_cf_" ++ integer_to_list(N)),
  {ok, Db, [_, Cf]} = open_with_cf(Dir),
  Workers = run_workers(?WORKERS, fun(I) -> cf_worker(Db, Cf, I, 0) end),
  timer:sleep(N rem 4),
  ok = rocksdb:destroy_column_family(Db, Cf),
  wait_workers(Workers),
  ?assertError(badarg, rocksdb:put(Db, Cf, <<"k">>, <<"v">>, [])),
  ok = rocksdb:put(Db, <<"default_still_works">>, <<"v">>, []),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

cf_worker(_Db, _Cf, _I, 2000) -> ok;
cf_worker(Db, Cf, I, K) ->
  Key = <<I:32, K:32>>,
  case catch rocksdb:put(Db, Cf, Key, <<"v">>, []) of
    ok ->
      _ = catch rocksdb:get(Db, Cf, Key, []),
      cf_worker(Db, Cf, I, K + 1);
    {'EXIT', {badarg, _}} -> ok
  end.

%% a coalescing iterator keeps the column family handles it was given
coalescing_iterator_keeps_cfs_test() ->
  Dir = test_dir("coalescing"),
  {ok, Db, [Default, Cf]} = open_with_cf(Dir),
  [ok = rocksdb:put(Db, Default, <<I:32>>, <<"d">>, []) || I <- lists:seq(1, 100)],
  [ok = rocksdb:put(Db, Cf, <<I:32>>, <<"c">>, []) || I <- lists:seq(101, 200)],
  {ok, It} = rocksdb:coalescing_iterator(Db, [Default, Cf], []),
  ok = rocksdb:destroy_column_family(Db, Cf),
  erlang:garbage_collect(),
  ?assertEqual(200, count_forward(It, rocksdb:iterator_move(It, first), 0)),
  ok = rocksdb:iterator_close(It),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% iterators/3 dropped its column family references before RocksDB used them
multi_iterators_keep_cfs_test() ->
  Dir = test_dir("multi_iterators"),
  {ok, Db, [Default, Cf]} = open_with_cf(Dir),
  ok = rocksdb:put(Db, Cf, <<"k">>, <<"v">>, []),
  {ok, [_ItDefault, ItCf]} = rocksdb:iterators(Db, [Default, Cf], []),
  ok = rocksdb:destroy_column_family(Db, Cf),
  erlang:garbage_collect(),
  {ok, <<"k">>, <<"v">>} = rocksdb:iterator_move(ItCf, first),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% RocksDB leaves it to the caller to synchronize calls on a transaction
shared_optimistic_transaction_test_() ->
  {timeout, 120, fun() ->
    Dir = test_dir("shared_otx"),
    {ok, Db, _} = rocksdb:open_optimistic_transaction_db(
                    filename:join(Dir, "db"), [{create_if_missing, true}], [{"default", []}]),
    {ok, Tx} = rocksdb:transaction(Db, []),
    Workers = run_workers(?WORKERS, fun(I) ->
      [begin
         ok = rocksdb:transaction_put(Tx, <<I:32, K:32>>, <<K:32>>),
         {ok, <<K:32>>} = rocksdb:transaction_get(Tx, <<I:32, K:32>>, [])
       end || K <- lists:seq(1, 500)]
    end),
    %% an iterator over the transaction moved while the others write into it
    {ok, It} = rocksdb:transaction_iterator(Tx, []),
    [_ = rocksdb:iterator_move(It, first) || _ <- lists:seq(1, 200)],
    wait_workers(Workers),
    ok = rocksdb:iterator_close(It),
    ok = rocksdb:transaction_commit(Tx),
    ok = rocksdb:release_transaction(Tx),
    ?assertEqual(?WORKERS * 500, count_keys(Db)),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir)
  end}.

shared_pessimistic_transaction_test_() ->
  {timeout, 120, fun() ->
    Dir = test_dir("shared_ptx"),
    {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
                    filename:join(Dir, "db"), [{create_if_missing, true}]),
    {ok, Tx} = rocksdb:pessimistic_transaction(Db, []),
    Workers = run_workers(?WORKERS, fun(I) ->
      [begin
         ok = rocksdb:pessimistic_transaction_put(Tx, <<I:32, K:32>>, <<K:32>>),
         {ok, <<K:32>>} = rocksdb:pessimistic_transaction_get(Tx, <<I:32, K:32>>, [])
       end || K <- lists:seq(1, 500)]
    end),
    {ok, It} = rocksdb:pessimistic_transaction_iterator(Tx, []),
    [_ = rocksdb:iterator_move(It, first) || _ <- lists:seq(1, 200)],
    wait_workers(Workers),
    ok = rocksdb:iterator_close(It),
    ok = rocksdb:pessimistic_transaction_commit(Tx),
    ok = rocksdb:release_pessimistic_transaction(Tx),
    ?assertEqual(?WORKERS * 500, count_keys(Db)),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir)
  end}.

%% a transaction iterator kept its transaction only against garbage
%% collection, not against release
transaction_iterator_outlives_release_test() ->
  Dir = test_dir("tx_iter_release"),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
                  filename:join(Dir, "db"), [{create_if_missing, true}]),
  {ok, Tx} = rocksdb:pessimistic_transaction(Db, []),
  ok = rocksdb:pessimistic_transaction_put(Tx, <<"a">>, <<"1">>),
  {ok, It} = rocksdb:pessimistic_transaction_iterator(Tx, []),
  ok = rocksdb:release_pessimistic_transaction(Tx),
  erlang:garbage_collect(),
  {ok, <<"a">>, <<"1">>} = rocksdb:iterator_move(It, first),
  ok = rocksdb:iterator_close(It),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% one iterator moved by several processes
shared_iterator_test_() ->
  {timeout, 120, fun() ->
    Dir = test_dir("shared_iter"),
    {ok, Db} = rocksdb:open(filename:join(Dir, "db"), [{create_if_missing, true}]),
    [ok = rocksdb:put(Db, <<I:32>>, <<I:32>>, []) || I <- lists:seq(1, 1000)],
    {ok, It} = rocksdb:iterator(Db, []),
    Workers = run_workers(?WORKERS, fun(I) ->
      [_ = rocksdb:iterator_move(It, case K rem 3 of
                                       0 -> first;
                                       1 -> next;
                                       2 -> {seek, <<(I * K rem 1000):32>>}
                                     end) || K <- lists:seq(1, 2000)]
    end),
    wait_workers(Workers),
    {ok, <<1:32>>, <<1:32>>} = rocksdb:iterator_move(It, first),
    ok = rocksdb:iterator_close(It),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir)
  end}.

shared_wal_iterator_test() ->
  Dir = test_dir("shared_wal_iter"),
  {ok, Db} = rocksdb:open(filename:join(Dir, "db"), [{create_if_missing, true}]),
  [ok = rocksdb:put(Db, <<I:32>>, <<I:32>>, []) || I <- lists:seq(1, 2000)],
  {ok, It} = rocksdb:tlog_iterator(Db, 0),
  Workers = run_workers(?WORKERS, fun(_) ->
    [_ = rocksdb:tlog_next_binary_update(It) || _ <- lists:seq(1, 300)]
  end),
  wait_workers(Workers),
  ok = rocksdb:tlog_iterator_close(It),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% SstFileWriter is not thread-safe
shared_sst_writer_test() ->
  Dir = test_dir("shared_sst_writer"),
  Path = filename:join(Dir, "data.sst"),
  {ok, W} = rocksdb:sst_file_writer_open([], Path),
  Readers = run_workers(?WORKERS, fun(_) ->
    [_ = rocksdb:sst_file_writer_file_size(W) || _ <- lists:seq(1, 2000)]
  end),
  [ok = rocksdb:sst_file_writer_put(W, <<I:32>>, binary:copy(<<"v">>, 100))
   || I <- lists:seq(1, 5000)],
  wait_workers(Readers),
  ok = rocksdb:sst_file_writer_finish(W),
  ok = rocksdb:release_sst_file_writer(W),
  {ok, R} = rocksdb:sst_file_reader_open([], Path),
  {ok, #{num_entries := 5000}} = rocksdb:sst_file_reader_get_table_properties(R),
  ?rm_rf(Dir).

%% Handles created while the database closes were not closed with it, so
%% RocksDB stayed open (and locked) after close/1 returned. The handles are
%% kept alive until the reopen, so garbage collection cannot hide that.
children_of_closing_db_test_() ->
  {timeout, 120, fun() ->
    [children_of_closing_db(N) || N <- lists:seq(1, 20)]
  end}.

children_of_closing_db(N) ->
  Dir = test_dir("closing_children_" ++ integer_to_list(N)),
  Path = filename:join(Dir, "db"),
  {ok, Db} = rocksdb:open(Path, [{create_if_missing, true}]),
  ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
  Holder = self(),
  Workers = run_workers(?WORKERS, fun(I) -> create_children(Db, I, Holder, 0) end),
  timer:sleep(N rem 5),
  ok = rocksdb:close(Db),
  wait_workers(Workers),
  Kept = collect_children([]),
  %% fails with a lock error if any child still holds the database open
  {ok, Names} = rocksdb:list_column_families(Path, []),
  {ok, Db2, _} = rocksdb:open(Path, [], [{Name, []} || Name <- Names]),
  ok = rocksdb:close(Db2),
  _ = length(Kept),
  ?rm_rf(Dir).

create_children(_Db, _I, _Holder, 500) -> ok;
create_children(Db, I, Holder, K) ->
  R = case (I + K) rem 3 of
        0 -> catch rocksdb:snapshot(Db);
        1 -> catch rocksdb:iterator(Db, []);
        2 -> catch rocksdb:create_column_family(
                     Db, "cf_" ++ integer_to_list(I) ++ "_" ++ integer_to_list(K), [])
      end,
  case R of
    {ok, Child} -> Holder ! {child, Child}, create_children(Db, I, Holder, K + 1);
    {'EXIT', {badarg, _}} -> ok;
    {error, _} -> ok
  end.

collect_children(Acc) ->
  receive {child, C} -> collect_children([C | Acc])
  after 0 -> Acc
  end.

%% helpers

open_with_cf(Dir) ->
  rocksdb:open(filename:join(Dir, "db"),
               [{create_if_missing, true}, {create_missing_column_families, true}],
               [{"default", []}, {"other", []}]).

run_workers(N, Fun) ->
  Self = self(),
  [spawn_link(fun() -> Fun(I), Self ! {done, self()} end) || I <- lists:seq(1, N)].

wait_workers(Workers) ->
  [receive {done, W} -> ok end || W <- Workers],
  ok.

count_forward(_It, {error, invalid_iterator}, N) -> N;
count_forward(It, {ok, _, _}, N) -> count_forward(It, rocksdb:iterator_move(It, next), N + 1).

count_keys(Db) ->
  {ok, It} = rocksdb:iterator(Db, []),
  N = count_forward(It, rocksdb:iterator_move(It, first), 0),
  ok = rocksdb:iterator_close(It),
  N.

test_dir(Name) ->
  Dir = "erocksdb.handle_concurrency." ++ Name ++ ".test",
  ?rm_rf(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "x")),
  Dir.
