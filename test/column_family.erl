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

-module(column_family).


-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

-define(DB, "test.db").

basic_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, Handles1} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  ?assertEqual(1, length(Handles1)),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families(?DB, [])),
  {ok, Handle} = rocksdb:create_column_family(Db, "test", []),
  ?assertEqual({ok, ["default", "test"]}, rocksdb:list_column_families(?DB, [])),
  ok = rocksdb:drop_column_family(Db, Handle),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families(?DB, [])),
  rocksdb:close(Db),
  destroy_and_rm(?DB, []),
  ok.

destroy_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, Handles1} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  ?assertEqual(1, length(Handles1)),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families(?DB, [])),
  {ok, Handle} = rocksdb:create_column_family(Db, "test", []),
  ?assertEqual({ok, ["default", "test"]}, rocksdb:list_column_families(?DB, [])),
  ok = rocksdb:destroy_column_family(Db, Handle),
  ?assertEqual({ok, ["default", "test"]}, rocksdb:list_column_families(?DB, [])),
  rocksdb:close(Db),
  destroy_and_rm(?DB, []),
  ok.

column_order_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, Handles1} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  ?assertEqual(1, length(Handles1)),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families(?DB, [])),
  {ok, _Handle} = rocksdb:create_column_family(Db, "test", []),
  ok = rocksdb:close(Db),
  ?assertEqual({ok, ["default", "test"]}, rocksdb:list_column_families(?DB, [])),
  ColumnFamilies2 = [{"default", []}, {"test", []}],
  {ok, Db2, Handles2} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies2),
  [_DefaultH, TestH] = Handles2,
  ok = rocksdb:drop_column_family(Db2, TestH),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families(?DB, [])),
  rocksdb:close(Db2),
  destroy_and_rm(?DB, []),
  ok.

try_remove_default_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, [DefaultH]} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  {error, _} = rocksdb:drop_column_family(Db, DefaultH),
  {ok, _Handle} = rocksdb:create_column_family(Db, "test", []),
  rocksdb:close(Db),
  ColumnFamilies2 = [{"default", []}, {"test", []}],
  {ok, Db2, [DefaultH2, _]} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies2),
  {error, _} = rocksdb:drop_column_family(Db2, DefaultH2),
  rocksdb:close(Db2),
  destroy_and_rm(?DB, []),
  ok.

basic_kvs_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, [DefaultH]} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  ok = rocksdb:put(Db, DefaultH, <<"a">>, <<"a1">>, []),
  {ok,  <<"a1">>} = rocksdb:get(Db, DefaultH, <<"a">>, []),
  ok = rocksdb:put(Db, DefaultH, <<"b">>, <<"b1">>, []),
  {ok, <<"b1">>} = rocksdb:get(Db, DefaultH, <<"b">>, []),
  ?assertEqual(2, count(Db,DefaultH)),

  ok = rocksdb:delete(Db, DefaultH, <<"b">>, []),
  not_found = rocksdb:get(Db, DefaultH, <<"b">>, []),
  ?assertEqual(1, count(Db, DefaultH)),

  {ok, TestH} = rocksdb:create_column_family(Db, "test", []),
  rocksdb:put(Db, TestH, <<"a">>, <<"a2">>, []),
  {ok,  <<"a1">>} = rocksdb:get(Db, DefaultH, <<"a">>, []),
  {ok,  <<"a2">>} = rocksdb:get(Db, TestH, <<"a">>, []),
  ?assertEqual(1, count(Db, TestH)),
  rocksdb:close(Db),
  destroy_and_rm(?DB, []),
  ok.

single_delete_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}],
  {ok, Db, [DefaultH]} = rocksdb:open(?DB, [{create_if_missing, true}], ColumnFamilies),
  ok = rocksdb:put(Db, DefaultH, <<"c">>, <<"c1">>, []),
  {ok, <<"c1">>} = rocksdb:get(Db, DefaultH, <<"c">>, []),
  ok = rocksdb:single_delete(Db, DefaultH, <<"c">>, []),
  not_found = rocksdb:get(Db, DefaultH, <<"c">>, []),
  rocksdb:close(Db),
  destroy_and_rm(?DB, []),
  ok.

count(DBH, CFH) ->
  {ok, C} = rocksdb:get_property(DBH, CFH, <<"rocksdb.estimate-num-keys">>),
  binary_to_integer(C).

%% Test coalescing iterator across multiple column families
coalescing_iterator_test() ->
  destroy_and_rm(?DB, []),
  ColumnFamilies = [{"default", []}, {"cf1", []}, {"cf2", []}],
  {ok, Db, [DefaultH, Cf1H, Cf2H]} = rocksdb:open(?DB, [{create_if_missing, true}, {create_missing_column_families, true}], ColumnFamilies),

  %% Write different keys to different column families
  ok = rocksdb:put(Db, DefaultH, <<"a">>, <<"default_a">>, []),
  ok = rocksdb:put(Db, Cf1H, <<"b">>, <<"cf1_b">>, []),
  ok = rocksdb:put(Db, Cf2H, <<"c">>, <<"cf2_c">>, []),
  %% Write same key to multiple CFs - coalescing should only return first value
  ok = rocksdb:put(Db, DefaultH, <<"d">>, <<"default_d">>, []),
  ok = rocksdb:put(Db, Cf1H, <<"d">>, <<"cf1_d">>, []),

  %% Create coalescing iterator over all three column families
  {ok, Itr} = rocksdb:coalescing_iterator(Db, [DefaultH, Cf1H, Cf2H], []),

  %% Iterate and collect all keys (should be sorted: a, b, c, d)
  {ok, <<"a">>, <<"default_a">>} = rocksdb:iterator_move(Itr, first),
  {ok, <<"b">>, <<"cf1_b">>} = rocksdb:iterator_move(Itr, next),
  {ok, <<"c">>, <<"cf2_c">>} = rocksdb:iterator_move(Itr, next),
  {ok, <<"d">>, _} = rocksdb:iterator_move(Itr, next),  % first value from either CF
  {error, invalid_iterator} = rocksdb:iterator_move(Itr, next),

  ok = rocksdb:iterator_close(Itr),
  rocksdb:close(Db),
  destroy_and_rm(?DB, []),
  ok.

destroy_and_rm(Dir, Options) ->
  rocksdb:destroy(Dir, Options),
  rocksdb_test_util:rm_rf(Dir).
