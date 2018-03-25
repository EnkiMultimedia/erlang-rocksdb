%% Copyright (c) 2016-2018 Benoît Chesneau.
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


open_test() -> [{open_test_Z(), l} || l <- lists:seq(1, 20)].
open_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.open.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.open.test", [{create_if_missing, true}]),
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
  true = rocksdb:is_empty(Ref).

open_with_snappy_test() ->
  os:cmd("rm -rf /tmp/erocksdb.snapppy.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.snapppy.test", [{create_if_missing, true}, {compression, snappy}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("/tmp/erocksdb.snapppy.test", []),
  ok.

open_with_lz4_test() ->
  os:cmd("rm -rf /tmp/erocksdb.lz4.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.lz4.test", [{create_if_missing, true}, {compression, lz4}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("/tmp/erocksdb.lz4.test", []),
  ok.

fold_test() -> [{fold_test_Z(), l} || l <- lists:seq(1, 20)].
fold_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.fold.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.fold.test", [{create_if_missing, true}]),
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
  os:cmd("rm -rf /tmp/erocksdb.fold.keys.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.fold.keys.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  ok = rocksdb:put(Ref, <<"hij">>, <<"789">>, []),
  [<<"abc">>, <<"def">>, <<"hij">>] = lists:reverse(rocksdb:fold_keys(Ref,
  fun(K, Acc) -> [K | Acc] end,
  [], [])).

destroy_test() -> [{destroy_test_Z(), l} || l <- lists:seq(1, 20)].
destroy_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.destroy.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.destroy.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  {ok, <<"456">>} = rocksdb:get(Ref, <<"def">>, []),
  rocksdb:close(Ref),
  ok = rocksdb:destroy("/tmp/erocksdb.destroy.test", []),
  {error, {db_open, _}} = rocksdb:open("/tmp/erocksdb.destroy.test", [{error_if_exists, true}]).

compression_test() -> [{compression_test_Z(), l} || l <- lists:seq(1, 20)].
compression_test_Z() ->
  CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
  os:cmd("rm -rf /tmp/erocksdb.compress.0 /tmp/erocksdb.compress.1"),
  {ok, Ref0} = rocksdb:open("/tmp/erocksdb.compress.0", [{create_if_missing, true}, {compression, none}]),
  [ok = rocksdb:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  {ok, Ref1} = rocksdb:open("/tmp/erocksdb.compress.1", [{create_if_missing, true}, {compression, snappy}]),
  [ok = rocksdb:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  %% Check both of the LOG files created to see if the compression option was correctly
  %% passed down
  MatchCompressOption =
  fun(File, Expected) ->
    {ok, Contents} = file:read_file(File),
    case re:run(Contents, "Options.compression: " ++ Expected) of
    {match, _} -> match;
    nomatch -> nomatch
    end
  end,
  Log0Option = MatchCompressOption("/tmp/erocksdb.compress.0/LOG", "0"),
  Log1Option = MatchCompressOption("/tmp/erocksdb.compress.1/LOG", "1"),
  ?assert(Log0Option =:= match andalso Log1Option =:= match).

close_test() -> [{close_test_Z(), l} || l <- lists:seq(1, 20)].
close_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.close.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.close.test", [{create_if_missing, true}], []),
  ?assertEqual(ok, rocksdb:close(Ref)),
  ?assertEqual({error, einval}, rocksdb:close(Ref)).

close_fold_test() -> [{close_fold_test_Z(), l} || l <- lists:seq(1, 20)].
close_fold_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.close_fold.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.close_fold.test", [{create_if_missing, true}], []),
  ok = rocksdb:put(Ref, <<"k">>,<<"v">>,[]),
  ?assertException(throw, {iterator_closed, ok}, % ok is returned by close as the acc
  rocksdb:fold(Ref, fun(_,_A) -> rocksdb:close(Ref) end, undefined, [])).

destroy_reopen(DbName, Options) ->
  _ = rocksdb:destroy(DbName, []),
  _ = os:cmd("rm -rf " ++ DbName),
  {ok, Db} = rocksdb:open(DbName, Options),
  Db.
