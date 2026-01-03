%% Copyright (c) 2016-2026 Benoit Chesneau
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

-module(entity).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(DB, "ltest").

destroy_and_rm(Dir, Options) ->
  rocksdb:destroy(Dir, Options),
  rocksdb_test_util:rm_rf(Dir).

%% Basic PutEntity/GetEntity test (similar to RocksDB's DBWideBasicTest.PutEntity)
put_entity_basic_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Write a wide-column entity with multiple attributes
    %% Note: <<>> is the default column name in RocksDB (kDefaultWideColumnName)
    FirstKey = <<"first">>,
    FirstColumns = [
      {<<>>, <<"hello">>},           %% default column
      {<<"attr_name1">>, <<"foo">>},
      {<<"attr_name2">>, <<"bar">>}
    ],
    ok = rocksdb:put_entity(Db, FirstKey, FirstColumns, []),

    %% Write second entity without default column
    SecondKey = <<"second">>,
    SecondColumns = [
      {<<"attr_one">>, <<"two">>},
      {<<"attr_three">>, <<"four">>}
    ],
    ok = rocksdb:put_entity(Db, SecondKey, SecondColumns, []),

    %% Verify first entity
    {ok, Result1} = rocksdb:get_entity(Db, FirstKey, []),
    ?assertEqual(<<"hello">>, proplists:get_value(<<>>, Result1)),
    ?assertEqual(<<"foo">>, proplists:get_value(<<"attr_name1">>, Result1)),
    ?assertEqual(<<"bar">>, proplists:get_value(<<"attr_name2">>, Result1)),

    %% Verify second entity
    {ok, Result2} = rocksdb:get_entity(Db, SecondKey, []),
    ?assertEqual(<<"two">>, proplists:get_value(<<"attr_one">>, Result2)),
    ?assertEqual(<<"four">>, proplists:get_value(<<"attr_three">>, Result2))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test with column family
put_entity_cf_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}, {"entities", []}],
  {ok, Db, [_DefaultH, EntitiesH]} = rocksdb:open(?DB,
    [{create_if_missing, true}, {create_missing_column_families, true}],
    ColumnFamilies),
  try
    %% Put entity in column family
    Key = <<"entity:1">>,
    Columns = [
      {<<"field1">>, <<"value1">>},
      {<<"field2">>, <<"value2">>},
      {<<"field3">>, <<"value3">>}
    ],
    ok = rocksdb:put_entity(Db, EntitiesH, Key, Columns, []),

    %% Get entity from column family
    {ok, Result} = rocksdb:get_entity(Db, EntitiesH, Key, []),
    ?assertEqual(<<"value1">>, proplists:get_value(<<"field1">>, Result)),
    ?assertEqual(<<"value2">>, proplists:get_value(<<"field2">>, Result)),
    ?assertEqual(<<"value3">>, proplists:get_value(<<"field3">>, Result))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test entity not found
entity_not_found_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    ?assertEqual(not_found, rocksdb:get_entity(Db, <<"nonexistent">>, []))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test empty columns list
empty_columns_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Put entity with empty columns list
    ok = rocksdb:put_entity(Db, <<"empty:1">>, [], []),

    %% Get should return empty list
    {ok, []} = rocksdb:get_entity(Db, <<"empty:1">>, [])
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test overwrite entity
overwrite_entity_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    Key = <<"overwrite_key">>,

    %% Write initial entity
    InitialColumns = [{<<"col1">>, <<"val1">>}, {<<"col2">>, <<"val2">>}],
    ok = rocksdb:put_entity(Db, Key, InitialColumns, []),

    %% Verify initial
    {ok, R1} = rocksdb:get_entity(Db, Key, []),
    ?assertEqual(<<"val1">>, proplists:get_value(<<"col1">>, R1)),
    ?assertEqual(<<"val2">>, proplists:get_value(<<"col2">>, R1)),

    %% Overwrite with different columns
    NewColumns = [{<<"new_col">>, <<"new_val">>}],
    ok = rocksdb:put_entity(Db, Key, NewColumns, []),

    %% Verify overwritten
    {ok, R2} = rocksdb:get_entity(Db, Key, []),
    ?assertEqual(<<"new_val">>, proplists:get_value(<<"new_col">>, R2)),
    %% Old columns should be gone
    ?assertEqual(undefined, proplists:get_value(<<"col1">>, R2)),
    ?assertEqual(undefined, proplists:get_value(<<"col2">>, R2))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test binary data in columns
binary_data_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    Key = <<"binary_key">>,
    %% Binary data with null bytes and special characters
    BinaryData = <<0, 1, 2, 3, 255, 254, 253>>,
    Columns = [{<<"binary_col">>, BinaryData}],
    ok = rocksdb:put_entity(Db, Key, Columns, []),

    {ok, Result} = rocksdb:get_entity(Db, Key, []),
    ?assertEqual(BinaryData, proplists:get_value(<<"binary_col">>, Result))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test large entity with many columns
large_entity_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    Key = <<"large_entity">>,
    %% Create 100 columns
    Columns = [{list_to_binary("col" ++ integer_to_list(I)),
                list_to_binary("val" ++ integer_to_list(I))}
               || I <- lists:seq(1, 100)],
    ok = rocksdb:put_entity(Db, Key, Columns, []),

    {ok, Result} = rocksdb:get_entity(Db, Key, []),
    ?assertEqual(100, length(Result)),

    %% Verify a few columns
    ?assertEqual(<<"val1">>, proplists:get_value(<<"col1">>, Result)),
    ?assertEqual(<<"val50">>, proplists:get_value(<<"col50">>, Result)),
    ?assertEqual(<<"val100">>, proplists:get_value(<<"col100">>, Result))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test multiple entities
multiple_entities_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Write multiple entities
    lists:foreach(fun(I) ->
      Key = list_to_binary("key" ++ integer_to_list(I)),
      Columns = [{<<"id">>, list_to_binary(integer_to_list(I))},
                 {<<"type">>, <<"entity">>}],
      ok = rocksdb:put_entity(Db, Key, Columns, [])
    end, lists:seq(1, 10)),

    %% Verify all entities
    lists:foreach(fun(I) ->
      Key = list_to_binary("key" ++ integer_to_list(I)),
      {ok, Result} = rocksdb:get_entity(Db, Key, []),
      ?assertEqual(list_to_binary(integer_to_list(I)),
                   proplists:get_value(<<"id">>, Result)),
      ?assertEqual(<<"entity">>, proplists:get_value(<<"type">>, Result))
    end, lists:seq(1, 10))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test delete entity
delete_entity_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    Key = <<"to_delete">>,
    Columns = [{<<"col1">>, <<"val1">>}, {<<"col2">>, <<"val2">>}],
    ok = rocksdb:put_entity(Db, Key, Columns, []),

    %% Verify it exists
    {ok, _} = rocksdb:get_entity(Db, Key, []),

    %% Delete entity
    ok = rocksdb:delete_entity(Db, Key, []),

    %% Verify it's gone
    ?assertEqual(not_found, rocksdb:get_entity(Db, Key, []))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test delete entity in column family
delete_entity_cf_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}, {"entities", []}],
  {ok, Db, [_DefaultH, EntitiesH]} = rocksdb:open(?DB,
    [{create_if_missing, true}, {create_missing_column_families, true}],
    ColumnFamilies),
  try
    Key = <<"cf_delete">>,
    Columns = [{<<"field">>, <<"value">>}],
    ok = rocksdb:put_entity(Db, EntitiesH, Key, Columns, []),

    %% Verify it exists
    {ok, _} = rocksdb:get_entity(Db, EntitiesH, Key, []),

    %% Delete
    ok = rocksdb:delete_entity(Db, EntitiesH, Key, []),

    %% Verify it's gone
    ?assertEqual(not_found, rocksdb:get_entity(Db, EntitiesH, Key, []))
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test iterator_columns with entities
iterator_columns_entity_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Write an entity
    Key = <<"entity_key">>,
    Columns = [
      {<<"col1">>, <<"val1">>},
      {<<"col2">>, <<"val2">>}
    ],
    ok = rocksdb:put_entity(Db, Key, Columns, []),

    %% Create iterator and get columns
    {ok, Itr} = rocksdb:iterator(Db, []),
    {ok, Key, _} = rocksdb:iterator_move(Itr, first),

    %% Get columns from iterator
    {ok, ItrCols} = rocksdb:iterator_columns(Itr),
    ?assertEqual(<<"val1">>, proplists:get_value(<<"col1">>, ItrCols)),
    ?assertEqual(<<"val2">>, proplists:get_value(<<"col2">>, ItrCols)),

    ok = rocksdb:iterator_close(Itr)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test iterator_columns with regular key-value
iterator_columns_kv_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Write a regular key-value
    Key = <<"regular_key">>,
    Value = <<"regular_value">>,
    ok = rocksdb:put(Db, Key, Value, []),

    %% Create iterator and get columns
    {ok, Itr} = rocksdb:iterator(Db, []),
    {ok, Key, Value} = rocksdb:iterator_move(Itr, first),

    %% Get columns - should return single column with empty name (default)
    {ok, Cols} = rocksdb:iterator_columns(Itr),
    ?assertEqual(1, length(Cols)),
    %% Default column name is empty binary
    ?assertEqual(Value, proplists:get_value(<<>>, Cols)),

    ok = rocksdb:iterator_close(Itr)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).

%% Test iterating over mixed entities and key-values
iterator_mixed_test() ->
  destroy_and_rm(?DB, []),
  {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
  try
    %% Write entity
    ok = rocksdb:put_entity(Db, <<"a_entity">>,
      [{<<"attr">>, <<"entity_value">>}], []),
    %% Write regular key-value
    ok = rocksdb:put(Db, <<"b_regular">>, <<"kv_value">>, []),

    %% Iterate and check columns for each
    {ok, Itr} = rocksdb:iterator(Db, []),

    %% First: entity
    {ok, <<"a_entity">>, _} = rocksdb:iterator_move(Itr, first),
    {ok, EntityCols} = rocksdb:iterator_columns(Itr),
    ?assertEqual(<<"entity_value">>, proplists:get_value(<<"attr">>, EntityCols)),

    %% Second: regular kv
    {ok, <<"b_regular">>, <<"kv_value">>} = rocksdb:iterator_move(Itr, next),
    {ok, KvCols} = rocksdb:iterator_columns(Itr),
    ?assertEqual(<<"kv_value">>, proplists:get_value(<<>>, KvCols)),

    ok = rocksdb:iterator_close(Itr)
  after
    rocksdb:close(Db)
  end,
  destroy_and_rm(?DB, []).
