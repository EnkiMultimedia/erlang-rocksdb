%% Copyright (c) 2016-2026 Benoît Chesneau.
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
-module(multi_get).

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test basic multi_get with existing keys
basic_test() ->
  ?rm_rf("test_multi_get"),
  {ok, Db} = rocksdb:open("test_multi_get", [{create_if_missing, true}]),
  try
    %% Insert 5 keys
    ok = rocksdb:put(Db, <<"k1">>, <<"v1">>, []),
    ok = rocksdb:put(Db, <<"k2">>, <<"v2">>, []),
    ok = rocksdb:put(Db, <<"k3">>, <<"v3">>, []),
    ok = rocksdb:put(Db, <<"k4">>, <<"v4">>, []),
    ok = rocksdb:put(Db, <<"k5">>, <<"v5">>, []),

    %% Multi_get all 5 keys
    Keys = [<<"k1">>, <<"k2">>, <<"k3">>, <<"k4">>, <<"k5">>],
    Results = rocksdb:multi_get(Db, Keys, []),

    %% All should return {ok, Value}
    ?assertEqual([{ok, <<"v1">>},
                  {ok, <<"v2">>},
                  {ok, <<"v3">>},
                  {ok, <<"v4">>},
                  {ok, <<"v5">>}], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get").

%% Test multi_get with missing keys
not_found_test() ->
  ?rm_rf("test_multi_get_nf"),
  {ok, Db} = rocksdb:open("test_multi_get_nf", [{create_if_missing, true}]),
  try
    %% Insert 3 keys
    ok = rocksdb:put(Db, <<"k1">>, <<"v1">>, []),
    ok = rocksdb:put(Db, <<"k3">>, <<"v3">>, []),
    ok = rocksdb:put(Db, <<"k5">>, <<"v5">>, []),

    %% Multi_get 5 keys (3 exist, 2 don't)
    Keys = [<<"k1">>, <<"k2">>, <<"k3">>, <<"k4">>, <<"k5">>],
    Results = rocksdb:multi_get(Db, Keys, []),

    %% Check mixed results
    ?assertEqual([{ok, <<"v1">>},
                  not_found,
                  {ok, <<"v3">>},
                  not_found,
                  {ok, <<"v5">>}], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_nf").

%% Test multi_get with empty key list
empty_test() ->
  ?rm_rf("test_multi_get_empty"),
  {ok, Db} = rocksdb:open("test_multi_get_empty", [{create_if_missing, true}]),
  try
    %% Multi_get with empty list
    Results = rocksdb:multi_get(Db, [], []),
    ?assertEqual([], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_empty").

%% Test multi_get with column family
column_family_test() ->
  ?rm_rf("test_multi_get_cf"),
  {ok, Db, [_DefaultCf, Cf1]} = rocksdb:open_with_cf(
    "test_multi_get_cf",
    [{create_if_missing, true}, {create_missing_column_families, true}],
    [{"default", []}, {"cf1", []}]),
  try
    %% Insert keys in custom CF
    ok = rocksdb:put(Db, Cf1, <<"k1">>, <<"cf1_v1">>, []),
    ok = rocksdb:put(Db, Cf1, <<"k2">>, <<"cf1_v2">>, []),
    ok = rocksdb:put(Db, Cf1, <<"k3">>, <<"cf1_v3">>, []),

    %% Insert different values in default CF
    ok = rocksdb:put(Db, <<"k1">>, <<"default_v1">>, []),
    ok = rocksdb:put(Db, <<"k2">>, <<"default_v2">>, []),

    %% Multi_get from custom CF
    Keys = [<<"k1">>, <<"k2">>, <<"k3">>],
    Results = rocksdb:multi_get(Db, Cf1, Keys, []),

    %% Should get values from CF1, not default
    ?assertEqual([{ok, <<"cf1_v1">>},
                  {ok, <<"cf1_v2">>},
                  {ok, <<"cf1_v3">>}], Results),

    %% Multi_get from default CF (without CF handle)
    DefaultResults = rocksdb:multi_get(Db, Keys, []),
    ?assertEqual([{ok, <<"default_v1">>},
                  {ok, <<"default_v2">>},
                  not_found], DefaultResults)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_cf").

%% Test multi_get preserves key order
order_test() ->
  ?rm_rf("test_multi_get_order"),
  {ok, Db} = rocksdb:open("test_multi_get_order", [{create_if_missing, true}]),
  try
    %% Insert keys in one order
    ok = rocksdb:put(Db, <<"z">>, <<"last">>, []),
    ok = rocksdb:put(Db, <<"a">>, <<"first">>, []),
    ok = rocksdb:put(Db, <<"m">>, <<"middle">>, []),

    %% Request in a different order
    Keys = [<<"m">>, <<"z">>, <<"a">>],
    Results = rocksdb:multi_get(Db, Keys, []),

    %% Results should match request order, not insertion order
    ?assertEqual([{ok, <<"middle">>},
                  {ok, <<"last">>},
                  {ok, <<"first">>}], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_order").

%% Test multi_get with snapshot
snapshot_test() ->
  ?rm_rf("test_multi_get_snap"),
  {ok, Db} = rocksdb:open("test_multi_get_snap", [{create_if_missing, true}]),
  try
    %% Insert initial values
    ok = rocksdb:put(Db, <<"k1">>, <<"old1">>, []),
    ok = rocksdb:put(Db, <<"k2">>, <<"old2">>, []),

    %% Create snapshot
    {ok, Snapshot} = rocksdb:snapshot(Db),

    %% Modify values after snapshot
    ok = rocksdb:put(Db, <<"k1">>, <<"new1">>, []),
    ok = rocksdb:put(Db, <<"k2">>, <<"new2">>, []),
    ok = rocksdb:put(Db, <<"k3">>, <<"new3">>, []),

    %% Multi_get with snapshot should get old values
    Keys = [<<"k1">>, <<"k2">>, <<"k3">>],
    SnapshotResults = rocksdb:multi_get(Db, Keys, [{snapshot, Snapshot}]),
    ?assertEqual([{ok, <<"old1">>},
                  {ok, <<"old2">>},
                  not_found], SnapshotResults),

    %% Multi_get without snapshot should get new values
    CurrentResults = rocksdb:multi_get(Db, Keys, []),
    ?assertEqual([{ok, <<"new1">>},
                  {ok, <<"new2">>},
                  {ok, <<"new3">>}], CurrentResults),

    rocksdb:release_snapshot(Snapshot)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_snap").

%% Test multi_get with large batch
large_batch_test() ->
  ?rm_rf("test_multi_get_large"),
  {ok, Db} = rocksdb:open("test_multi_get_large", [{create_if_missing, true}]),
  try
    %% Insert 1000 keys
    NumKeys = 1000,
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        Value = <<"value", (integer_to_binary(I))/binary>>,
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, NumKeys)),

    %% Multi_get all 1000 keys
    Keys = [<<"key", (integer_to_binary(I))/binary>> || I <- lists:seq(1, NumKeys)],
    Results = rocksdb:multi_get(Db, Keys, []),

    %% Verify count
    ?assertEqual(NumKeys, length(Results)),

    %% Verify all are {ok, _}
    lists:foreach(
      fun({I, Result}) ->
        ExpectedValue = <<"value", (integer_to_binary(I))/binary>>,
        ?assertEqual({ok, ExpectedValue}, Result)
      end,
      lists:zip(lists:seq(1, NumKeys), Results))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_large").

%% Test multi_get with single key
single_key_test() ->
  ?rm_rf("test_multi_get_single"),
  {ok, Db} = rocksdb:open("test_multi_get_single", [{create_if_missing, true}]),
  try
    ok = rocksdb:put(Db, <<"only">>, <<"one">>, []),
    Results = rocksdb:multi_get(Db, [<<"only">>], []),
    ?assertEqual([{ok, <<"one">>}], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_single").

%% Test multi_get with duplicate keys
duplicate_keys_test() ->
  ?rm_rf("test_multi_get_dup"),
  {ok, Db} = rocksdb:open("test_multi_get_dup", [{create_if_missing, true}]),
  try
    ok = rocksdb:put(Db, <<"k1">>, <<"v1">>, []),
    %% Request same key multiple times
    Keys = [<<"k1">>, <<"k1">>, <<"k1">>],
    Results = rocksdb:multi_get(Db, Keys, []),
    ?assertEqual([{ok, <<"v1">>},
                  {ok, <<"v1">>},
                  {ok, <<"v1">>}], Results)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm("test_multi_get_dup").

destroy_and_rm(Dir) ->
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir).
