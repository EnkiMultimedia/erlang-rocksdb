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

%% Regression tests for memory safety issues in the NIF: each of these used
%% to crash the VM, corrupt data or stop the database.
-module(nif_safety).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% bitset merge: a bit just past the end was written out of bounds
bitset_out_of_range_test() ->
  with_db("bitset_range", [{merge_operator, {bitset_merge_operator, 64}}], fun(Db) ->
    ok = rocksdb:put(Db, <<"b">>, <<0:64>>, []),
    ok = rocksdb:merge(Db, <<"b">>, <<"+63">>, []),
    {ok, <<1:64>>} = rocksdb:get(Db, <<"b">>, []),
    ok = rocksdb:merge(Db, <<"b">>, <<"+64">>, []),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"b">>, [])),
    assert_still_writable(Db)
  end).

%% bitset merge: the merge changed the stored value in place, so a snapshot
%% taken before the merge saw the new bit
bitset_keeps_stored_value_test() ->
  with_db("bitset_snapshot", [{merge_operator, {bitset_merge_operator, 64}}], fun(Db) ->
    ok = rocksdb:put(Db, <<"b">>, <<0:64>>, []),
    {ok, Snap} = rocksdb:snapshot(Db),
    ok = rocksdb:merge(Db, <<"b">>, <<"+0">>, []),
    {ok, <<128, 0:56>>} = rocksdb:get(Db, <<"b">>, []),
    {ok, <<0:64>>} = rocksdb:get(Db, <<"b">>, [{snapshot, Snap}]),
    ok = rocksdb:release_snapshot(Snap)
  end).

%% a released batch was still accepted and dereferenced
released_batch_test() ->
  {ok, B} = rocksdb:batch(),
  ok = rocksdb:batch_put(B, <<"a">>, <<"1">>),
  ok = rocksdb:release_batch(B),
  ok = rocksdb:release_batch(B),
  ?assertError(badarg, rocksdb:batch_count(B)),
  ?assertError(badarg, rocksdb:batch_put(B, <<"a">>, <<"1">>)),
  ?assertError(badarg, rocksdb:batch_clear(B)),
  with_db("released_batch", [], fun(Db) ->
    ?assertError(badarg, rocksdb:write_batch(Db, B, []))
  end).

%% the env was freed when its term was garbage collected, while the database
%% still used it
env_outlives_its_term_test() ->
  Dir = test_dir("env_gc"),
  Self = self(),
  {Pid, Ref} = spawn_monitor(fun() ->
    {ok, Env} = rocksdb:new_env({encrypted, binary:copy(<<1>>, 32)}),
    {ok, Db} = rocksdb:open(filename:join(Dir, "db"),
                            [{create_if_missing, true}, {env, Env}]),
    Self ! {db, Db}
  end),
  Db = receive {db, D} -> D end,
  receive {'DOWN', Ref, process, Pid, _} -> ok end,
  erlang:garbage_collect(),
  [ok = rocksdb:put(Db, <<I:32>>, binary:copy(<<"v">>, 1000), []) || I <- lists:seq(1, 5000)],
  ok = rocksdb:flush(Db, []),
  {ok, _} = rocksdb:get(Db, <<1:32>>, []),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

%% memory envs created from options are owned by the database
in_memory_open_close_test() ->
  lists:foreach(
    fun(Opt) ->
      [begin
         {ok, Db} = rocksdb:open("/erocksdb_nif_safety_mem", [Opt]),
         ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
         {ok, <<"v">>} = rocksdb:get(Db, <<"k">>, []),
         ok = rocksdb:close(Db)
       end || _ <- lists:seq(1, 20)]
    end,
    [{in_memory, true}, {env, memenv}]).

%% reads with a released snapshot used to silently read the latest data;
%% iterators that refresh read the snapshot again after it was released
released_snapshot_test() ->
  with_db("snapshot", [], fun(Db) ->
    ok = rocksdb:put(Db, <<"k">>, <<"old">>, []),
    {ok, Snap} = rocksdb:snapshot(Db),
    {ok, It} = rocksdb:iterator(Db, [{snapshot, Snap},
                                     {auto_refresh_iterator_with_snapshot, true}]),
    ok = rocksdb:release_snapshot(Snap),
    ?assertError(badarg, rocksdb:get(Db, <<"k">>, [{snapshot, Snap}])),
    ?assertError(badarg, rocksdb:iterator(Db, [{snapshot, Snap}])),
    %% new data and a flush make the iterator refresh on its next move
    [ok = rocksdb:put(Db, <<I:32>>, <<"new">>, []) || I <- lists:seq(1, 1000)],
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, []),
    {ok, <<"k">>, <<"old">>} = rocksdb:iterator_move(It, {seek, <<"k">>}),
    ok = rocksdb:iterator_close(It)
  end).

%% the SST reader was freed before the iterator reading it, and never
%% released when the iterator was garbage collected
sst_reader_iterator_test() ->
  Dir = test_dir("sst_reader"),
  Path = filename:join(Dir, "data.sst"),
  {ok, W} = rocksdb:sst_file_writer_open([], Path),
  [ok = rocksdb:sst_file_writer_put(W, <<I:32>>, <<I:32>>) || I <- lists:seq(1, 100)],
  ok = rocksdb:sst_file_writer_finish(W),
  ok = rocksdb:release_sst_file_writer(W),
  It = make_sst_iterator(Path),
  erlang:garbage_collect(),
  {ok, <<1:32>>, <<1:32>>} = rocksdb:sst_file_reader_iterator_move(It, first),
  ok = rocksdb:sst_file_reader_iterator_close(It),
  ok = rocksdb:sst_file_reader_iterator_close(It),
  ?assertError(badarg, rocksdb:sst_file_reader_iterator_move(It, next)),
  %% iterators dropped without close release their reader
  [make_sst_iterator(Path) || _ <- lists:seq(1, 50)],
  erlang:garbage_collect(),
  ?rm_rf(Dir).

make_sst_iterator(Path) ->
  {ok, Reader} = rocksdb:sst_file_reader_open([], Path),
  {ok, It} = rocksdb:sst_file_reader_iterator(Reader, []),
  It.

%% sizes read from the binary were trusted
malformed_posting_list_test() ->
  false = rocksdb:posting_list_contains(<<2, 0:32, 16#ffffffff:32>>, <<"k">>),
  false = rocksdb:posting_list_contains(<<2, 16#ffffffff:32, 1:32>>, <<"k">>),
  false = rocksdb:posting_list_contains(<<2, 0:32, 1:32, 16#ffffffff:32>>, <<"k">>).

%% a malformed operand stopped flush and every later write
erlang_merge_bad_operand_test() ->
  with_db("erlang_merge", [{merge_operator, erlang_merge_operator}], fun(Db) ->
    ok = rocksdb:put(Db, <<"k">>, term_to_binary(1), []),
    ok = rocksdb:merge(Db, <<"k">>, term_to_binary({}), []),
    ok = rocksdb:merge(Db, <<"k2">>, term_to_binary({int_add}), []),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"k">>, [])),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"k2">>, [])),
    assert_still_writable(Db)
  end).

%% int_add wrapped around instead of failing
erlang_merge_int_overflow_test() ->
  with_db("erlang_merge_overflow", [{merge_operator, erlang_merge_operator}], fun(Db) ->
    ok = rocksdb:put(Db, <<"i">>, term_to_binary(16#7fffffffffffffff), []),
    ok = rocksdb:merge(Db, <<"i">>, term_to_binary({int_add, 1}), []),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"i">>, [])),
    ok = rocksdb:put(Db, <<"j">>, term_to_binary(-16#8000000000000000), []),
    ok = rocksdb:merge(Db, <<"j">>, term_to_binary({int_add, -1}), []),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"j">>, [])),
    assert_still_writable(Db)
  end).

counter_merge_overflow_test() ->
  with_db("counter_overflow", [{merge_operator, counter_merge_operator}], fun(Db) ->
    ok = rocksdb:merge(Db, <<"c">>, <<"2147483647">>, []),
    {ok, <<"2147483647">>} = rocksdb:get(Db, <<"c">>, []),
    ok = rocksdb:merge(Db, <<"c">>, <<"1">>, []),
    ?assertMatch({error, {corruption, _}}, rocksdb:get(Db, <<"c">>, [])),
    ok = rocksdb:merge(Db, <<"bad">>, <<"x">>, []),
    assert_still_writable(Db)
  end).

%% each repeated bound used to leak the previous one
repeated_iterator_bounds_test() ->
  with_db("bounds", [], fun(Db) ->
    [ok = rocksdb:put(Db, <<I>>, <<I>>, []) || I <- lists:seq(1, 9)],
    {ok, It} = rocksdb:iterator(Db, [{iterate_lower_bound, <<1>>},
                                     {iterate_lower_bound, <<3>>},
                                     {iterate_upper_bound, <<9>>},
                                     {iterate_upper_bound, <<5>>}]),
    {ok, <<3>>, <<3>>} = rocksdb:iterator_move(It, first),
    {ok, <<4>>, <<4>>} = rocksdb:iterator_move(It, next),
    {error, invalid_iterator} = rocksdb:iterator_move(It, next),
    ok = rocksdb:iterator_close(It)
  end).

%% a close racing with calls on the same handles used to reach destroyed objects
close_while_in_use_test_() ->
  {timeout, 120, fun() ->
    [close_while_in_use(N) || N <- lists:seq(1, 30)]
  end}.

close_while_in_use(N) ->
  Dir = test_dir("close_race_" ++ integer_to_list(N)),
  {ok, Db, [_, Cf]} =
    rocksdb:open(filename:join(Dir, "db"),
                 [{create_if_missing, true}, {create_missing_column_families, true}],
                 [{"default", []}, {"other", []}]),
  ok = rocksdb:put(Db, Cf, <<"k">>, <<"v">>, []),
  {ok, Snap} = rocksdb:snapshot(Db),
  Self = self(),
  Workers = [spawn_link(fun() -> hammer(Db, Cf, Snap), Self ! {done, self()} end)
             || _ <- lists:seq(1, 8)],
  timer:sleep(N rem 5),
  ok = rocksdb:close(Db),
  [receive {done, W} -> ok end || W <- Workers],
  ?rm_rf(Dir).

hammer(Db, Cf, Snap) ->
  hammer(Db, Cf, Snap, 2000).

hammer(_Db, _Cf, _Snap, 0) -> ok;
hammer(Db, Cf, Snap, K) ->
  R = (catch rocksdb:get(Db, Cf, <<"k">>, [{snapshot, Snap}])),
  case R of
    {ok, _} -> hammer(Db, Cf, Snap, K - 1);
    not_found -> hammer(Db, Cf, Snap, K - 1);
    {'EXIT', {badarg, _}} -> ok;
    {error, _} -> ok
  end.

%% helpers

assert_still_writable(Db) ->
  ok = rocksdb:put(Db, <<"after">>, <<"v">>, []),
  ok = rocksdb:flush(Db, []),
  ok = rocksdb:compact_range(Db, undefined, undefined, []),
  ok = rocksdb:put(Db, <<"after2">>, <<"v">>, []),
  {ok, <<"v">>} = rocksdb:get(Db, <<"after2">>, []).

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
  Dir = "erocksdb.nif_safety." ++ Name ++ ".test",
  ?rm_rf(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "x")),
  Dir.
