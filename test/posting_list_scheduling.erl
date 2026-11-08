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

%% Posting list functions run inline for small inputs and on a dirty CPU
%% scheduler for large ones. Both paths must give the same results.
-module(posting_list_scheduling).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).
-define(SMALL, 10).
-define(LARGE, 20000).  %% above both the byte and the key count limits

same_results_inline_and_dirty_test_() ->
  {timeout, 120, fun() ->
    {Small, Large, Other} = build_lists(),
    check(Small, ?SMALL),
    check(Large, ?LARGE),
    %% set operations with a large input
    ?assertEqual(?LARGE div 2, rocksdb:posting_list_intersection_count(Large, Other)),
    ?assertEqual(?LARGE div 2,
                 rocksdb:posting_list_count(rocksdb:posting_list_intersection(Large, Other))),
    ?assertEqual(?LARGE + ?LARGE div 2,
                 rocksdb:posting_list_count(rocksdb:posting_list_union(Large, Other))),
    ?assertEqual(?LARGE div 2,
                 rocksdb:posting_list_count(rocksdb:posting_list_difference(Large, Other)))
  end}.

large_inputs_use_dirty_schedulers_test_() ->
  {timeout, 120, fun() ->
    {Small, Large, _Other} = build_lists(),
    {ok, PSmall} = rocksdb:postings_open(Small),
    {ok, PLarge} = rocksdb:postings_open(Large),
    %% calls returning small terms, so that no large garbage collection
    %% (which also runs on dirty schedulers) blurs the measure
    Calls = fun(List, P, N) ->
      fun() ->
        [begin
           rocksdb:posting_list_count(List),
           rocksdb:postings_to_binary(P)
         end || _ <- lists:seq(1, N)]
      end
    end,
    %% small inputs stay on the normal schedulers
    ?assertEqual(0, dirty_cpu_nif_time(Calls(Small, PSmall, 200))),
    %% large ones run on a dirty CPU scheduler
    ?assert(dirty_cpu_nif_time(Calls(Large, PLarge, 20)) > 0)
  end}.

%% a postings resource with a few huge keys is as costly as one with many
%% small keys: it is scheduled by bytes as well as by key count
few_large_keys_use_dirty_schedulers_test_() ->
  {timeout, 120, fun() ->
    Dir = "erocksdb.posting_list_scheduling.big_keys.test",
    ?rm_rf(Dir),
    {ok, Db} = rocksdb:open(Dir, [{create_if_missing, true},
                                  {merge_operator, posting_list_merge_operator}]),
    [ok = rocksdb:merge(Db, <<"big">>, {posting_add, binary:copy(<<I>>, 100 * 1024)}, [])
     || I <- [1, 2, 3]],
    {ok, Big} = rocksdb:get(Db, <<"big">>, []),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir),
    {ok, P} = rocksdb:postings_open(Big),
    ?assertEqual(3, rocksdb:postings_count(P)),
    ?assert(dirty_cpu_nif_time(fun() ->
      [rocksdb:postings_to_binary(P) || _ <- lists:seq(1, 20)]
    end) > 0)
  end}.

check(List, N) ->
  Keys = [key(I) || I <- lists:seq(1, N)],
  ?assertEqual(lists:sort(Keys), rocksdb:posting_list_keys(List)),
  ?assertEqual(N, rocksdb:posting_list_count(List)),
  ?assert(rocksdb:posting_list_contains(List, key(N))),
  ?assertNot(rocksdb:posting_list_contains(List, <<"missing">>)),
  ?assertEqual({ok, false}, rocksdb:posting_list_find(List, key(1))),
  ?assertEqual(N, map_size(rocksdb:posting_list_to_map(List))),
  ?assert(rocksdb:posting_list_bitmap_contains(List, key(1))),
  {ok, P} = rocksdb:postings_open(List),
  ?assertEqual(lists:sort(Keys), rocksdb:postings_keys(P)),
  ?assertEqual(N, rocksdb:posting_list_count(rocksdb:postings_to_binary(P))).

build_lists() ->
  Dir = "erocksdb.posting_list_scheduling.test",
  ?rm_rf(Dir),
  {ok, Db} = rocksdb:open(Dir, [{create_if_missing, true},
                                {merge_operator, posting_list_merge_operator}]),
  try
    add(Db, <<"small">>, lists:seq(1, ?SMALL)),
    add(Db, <<"large">>, lists:seq(1, ?LARGE)),
    %% overlaps the second half of large
    add(Db, <<"other">>, lists:seq(?LARGE div 2 + 1, ?LARGE + ?LARGE div 2)),
    {ok, Small} = rocksdb:get(Db, <<"small">>, []),
    {ok, Large} = rocksdb:get(Db, <<"large">>, []),
    {ok, Other} = rocksdb:get(Db, <<"other">>, []),
    ?assert(byte_size(Large) > 64 * 1024),
    {Small, Large, Other}
  after
    rocksdb:close(Db),
    ?rm_rf(Dir)
  end.

add(Db, Term, Ids) ->
  {ok, B} = rocksdb:batch(),
  [ok = rocksdb:batch_merge(B, Term, {posting_add, key(I)}) || I <- Ids],
  ok = rocksdb:write_batch(Db, B, []),
  ok = rocksdb:release_batch(B).

key(I) ->
  iolist_to_binary(io_lib:format("doc-~8..0B", [I])).

%% Time the dirty CPU schedulers spent running code (NIFs included) while
%% Fun ran. Garbage collection, which also uses them, is counted apart.
dirty_cpu_nif_time(Fun) ->
  Old = erlang:system_flag(microstate_accounting, true),
  try
    erlang:system_flag(microstate_accounting, reset),
    Fun(),
    lists:sum([maps:get(emulator, Counters)
               || #{type := dirty_cpu_scheduler, counters := Counters}
                    <- erlang:statistics(microstate_accounting)])
  after
    erlang:system_flag(microstate_accounting, Old)
  end.
