%% Copyright (c) 2016-2020 Benoît Chesneau.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
-module(db).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

iolist_test() ->
  Key = <<"abc">>,
  ValAsIoList = ["1", [[[<<"2">>]]], ["3"]],
  ValAsBin = <<"123">>,
  {ok, Ref} = rocksdb:open("erocksdb.open.test", [{create_if_missing, true}]),
  true = rocksdb:is_empty(Ref),
  ok = rocksdb:put(Ref, Key, ValAsIoList, []),
  false = rocksdb:is_empty(Ref),
  {ok, ValAsBin} = rocksdb:get(Ref, Key, []),
  ?rm_rf("erocksdb.open.test"),
  ok.

open_test() -> [{open_test_Z(), l} || l <- lists:seq(1, 20)].
open_test_Z() ->
  ?rm_rf("erocksdb.open.test"),
  {ok, Ref} = rocksdb:open("erocksdb.open.test", [{create_if_missing, true}]),
  true = rocksdb:is_empty(Ref),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  false = rocksdb:is_empty(Ref),
  {ok, <<"123">>} = rocksdb:get(Ref, <<"abc">>, []),
  {ok, 1} = rocksdb:count(Ref),
  not_found = rocksdb:get(Ref, <<"def">>, []),
  ok = rocksdb:delete(Ref, <<"abc">>, []),
  not_found = rocksdb:get(Ref, <<"abc">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  {ok, <<"123">>} = rocksdb:get(Ref, <<"abc">>, []),
  ok = rocksdb:single_delete(Ref, <<"abc">>, []),
  not_found = rocksdb:get(Ref, <<"abc">>, []),
  true = rocksdb:is_empty(Ref),
  ?rm_rf("erocksdb.open.test").
open_with_optimize_filters_for_hits_test() ->
  ?rm_rf("erocksdb.optimize_filters_for_hits.test"),
  {ok, Ref} = rocksdb:open("erocksdb.optimize_filters_for_hits.test", [{create_if_missing, true}, {optimize_filters_for_hits, true}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.optimize_filters_for_hits.test", []),
   ?rm_rf("erocksdb.optimize_filters_for_hits.test"),
  ok.

open_new_table_reader_for_compaction_inputs_test() ->
  ?rm_rf("erocksdb.new_table_reader_for_compaction_inputs.test"),
  {ok, Ref} = rocksdb:open("erocksdb.new_table_reader_for_compaction_inputs.test",
                  [{create_if_missing, true}, {new_table_reader_for_compaction_inputs, true}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.new_table_reader_for_compaction_inputs.test", []),
  ok.


fold_test() -> [{fold_test_Z(), l} || l <- lists:seq(1, 20)].
fold_test_Z() ->
  ?rm_rf("erocksdb.fold.test"),
  {ok, Ref} = rocksdb:open("erocksdb.fold.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  ok = rocksdb:put(Ref, <<"hij">>, <<"789">>, []),
  [{<<"abc">>, <<"123">>},
  {<<"def">>, <<"456">>},
  {<<"hij">>, <<"789">>}] = lists:reverse(rocksdb:fold(Ref,
  fun({K, V}, Acc) ->
    [{K, V} | Acc]
  end,
  [], [])).

fold_keys_test() -> [{fold_keys_test_Z(), l} || l <- lists:seq(1, 20)].
fold_keys_test_Z() ->
  ?rm_rf("erocksdb.fold.keys.test"),
  {ok, Ref} = rocksdb:open("erocksdb.fold.keys.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  ok = rocksdb:put(Ref, <<"hij">>, <<"789">>, []),
  [<<"abc">>, <<"def">>, <<"hij">>] = lists:reverse(rocksdb:fold_keys(Ref,
  fun(K, Acc) -> [K | Acc] end,
  [], [])).

destroy_test() -> [{destroy_test_Z(), l} || l <- lists:seq(1, 20)].
destroy_test_Z() ->
  ?rm_rf("erocksdb.destroy.test"),
  {ok, Ref} = rocksdb:open("erocksdb.destroy.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  {ok, <<"456">>} = rocksdb:get(Ref, <<"def">>, []),
  rocksdb:close(Ref),
  ok = rocksdb:destroy("erocksdb.destroy.test", []),
  {error, {db_open, _}} = rocksdb:open("erocksdb.destroy.test", [{error_if_exists, true}]),
  ?rm_rf("erocksdb.destroy.test").


close_test() -> [{close_test_Z(), l} || l <- lists:seq(1, 20)].
close_test_Z() ->
  ?rm_rf("erocksdb.close.test"),
  {ok, Ref} = rocksdb:open("erocksdb.close.test", [{create_if_missing, true}], []),
  ?assertEqual(ok, rocksdb:close(Ref)),
  ?assertEqual({error, einval}, rocksdb:close(Ref)),
  ?rm_rf("erocksdb.close.test").

close_fold_test() -> [{close_fold_test_Z(), l} || l <- lists:seq(1, 20)].
close_fold_test_Z() ->
  ?rm_rf("erocksdb.close_fold.test"),
  {ok, Ref} = rocksdb:open("erocksdb.close_fold.test", [{create_if_missing, true}], []),
  ok = rocksdb:put(Ref, <<"k">>,<<"v">>,[]),
  ?assertException(throw, {iterator_closed, ok}, % ok is returned by close as the acc
                   rocksdb:fold(Ref, fun(_,_A) -> rocksdb:close(Ref) end, undefined, [])),
  ?rm_rf("erocksdb.close_fold.test").

fixed_prefix_extractor_test() ->
  ?rm_rf("erocksdb.fixed_prefix_extractor.test"),
  {ok, Ref} = rocksdb:open("erocksdb.fixed_prefix_extractor.test", [{create_if_missing, true}, {prefix_extractor,
                                                                                                     {fixed_prefix_transform, 1}}]),

  ok = rocksdb:put(Ref, <<"k1">>, <<"v1">>, []),
  ok = rocksdb:put(Ref, <<"k2">>, <<"v2">>, []),

  [<<"k1">>, <<"k2">>] = lists:reverse(rocksdb:fold_keys(Ref,
                                                         fun(K, Acc) -> [K | Acc] end,
                                                         [], [])),


  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.fixed_prefix_extractor.test", []),
  ?rm_rf("erocksdb.fixed_prefix_extractor.test"),
  ok.

capped_prefix_extractor_test() ->
  ?rm_rf("erocksdb.capped_prefix_extractor.test"),
  {ok, Ref} = rocksdb:open("erocksdb.capped_prefix_extractor.test", [{create_if_missing, true}, {prefix_extractor,
                                                                                                      {capped_prefix_transform, 1}}]),

  ok = rocksdb:put(Ref, <<"k1">>, <<"v1">>, []),
  ok = rocksdb:put(Ref, <<"k2">>, <<"v2">>, []),

  [<<"k1">>, <<"k2">>] = lists:reverse(rocksdb:fold_keys(Ref,
                                                         fun(K, Acc) -> [K | Acc] end,
                                                         [], [])),


  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.capped_prefix_extractor.test", []),
  ?rm_rf("erocksdb.capped_prefix_extractor.test"),
  ok.

aproximate_sizes_test() ->
  DbOptions = [{create_if_missing, true},
               {target_file_size_base, 2 * 1024 * 1024},
               {max_bytes_for_level_base, 10 * 1024 * 1024},
               {write_buffer_size, 100000000},
               {compaction_pri, compensated_size},
               {compression, none}],
  N = 128,
  rand:seed(exs64),
  with_db(
    "erocksdb_aproximate_sizes_test",
    DbOptions,
    fun(Ref) ->
      _ = [ok = rocksdb:put(Ref, key(I), random_string(1024), []) || I <- lists:seq(0, N-1)],
      R = {key(50), key(60)},
      [Size] = rocksdb:get_approximate_sizes(Ref, [R], include_both),
      ?assert(Size >= 0),
      ?assert(Size =< 204800),
      [0] = rocksdb:get_approximate_sizes(Ref, [R], include_files),
      R2 = {key(500), key(600)},
      [0] = rocksdb:get_approximate_sizes(Ref, [R2], include_both),
      _ = [ok = rocksdb:put(Ref, key(1000 + I), random_string(1024), []) || I <- lists:seq(0, N-1)],
      [0] = rocksdb:get_approximate_sizes(Ref, [R2], include_both),
      R3 = {key(100), key(1020)},
      [Size2] = rocksdb:get_approximate_sizes(Ref, [R3], include_both),
      ?assert(Size2 >= 0),
      ok
    end
  ).

approximate_memtable_stats_test() ->
  DbOptions = [{create_if_missing, true},
               {target_file_size_base, 2 * 1024 * 1024},
               {max_bytes_for_level_base, 10 * 1024 * 1024},
               {write_buffer_size, 100000000},
               {compaction_pri, compensated_size},
               {compression, none}],
  N = 128,
  rand:seed(exs64),
  with_db(
    "erocksdb_approximate_memtable_stats_test",
    DbOptions,
    fun(Ref) ->
      _ = [ok = rocksdb:put(Ref, key(I), random_string(1024), []) || I <- lists:seq(0, N-1)],
      {ok, {Count, Size}} = rocksdb:get_approximate_memtable_stats(Ref, key(50), key(60)),
      ?assert(Count >= 0),
      ?assert(Count =< N),
      ?assert(Size >= 0),
      ?assert(Size =< 204800),
      {ok, {0, 0}} = rocksdb:get_approximate_memtable_stats(Ref, key(500), key(600)),
      _ = [ok = rocksdb:put(Ref, key(1000 + I), random_string(1024), []) || I <- lists:seq(0, N-1)],
      {ok, {0, 0}} = rocksdb:get_approximate_memtable_stats(Ref, key(500), key(600)),
      ok
    end
  ).

key(I) ->
  list_to_binary(io_lib:format("key~6..0B", [I])).

random_string(Len) ->
  iolist_to_binary([ [31 +  rand:uniform(95)] || _ <- lists:seq(1, Len)]).

with_db(Path, DbOptions, Fun) ->
  _ = ?rm_rf("" ++ Path),
  {ok, Ref} = rocksdb:open(Path, DbOptions),
  try
    Fun(Ref)
  after
    ok = rocksdb:close(Ref),
    rocksdb:destroy(Path, []),
    ?rm_rf(Path)
  end.
