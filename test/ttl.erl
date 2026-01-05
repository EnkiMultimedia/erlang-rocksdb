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

-module(ttl).


-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  rocksdb_test_util:rm_rf("test.db"),
  {ok, Db} = rocksdb:open_with_ttl("test.db", [{create_if_missing, true}], 1, false),
  ?assertEqual({ok, ["default"]}, rocksdb:list_column_families("test.db", [])),
  ok = rocksdb:put(Db, <<"a">>, <<"a1">>, []),
  {ok,  <<"a1">>} = rocksdb:get(Db, <<"a">>, []),
  timer:sleep(4000),
  rocksdb:compact_range(Db, <<"0">>, <<"b">>, []),
  not_found = rocksdb:get(Db, <<"a">>, []),
  rocksdb:close(Db),
  rocksdb:destroy("test.db", []),
  rocksdb_test_util:rm_rf("test.db"),
  ok.

%% Test opening with column families and different TTLs
open_with_ttl_cf_test() ->
  rocksdb_test_util:rm_rf("test_cf.db"),
  %% First create the database with column families
  {ok, Db1} = rocksdb:open("test_cf.db", [{create_if_missing, true}]),
  {ok, _CF1} = rocksdb:create_column_family(Db1, "cf1", []),
  {ok, _CF2} = rocksdb:create_column_family(Db1, "cf2", []),
  rocksdb:close(Db1),

  %% Now open with TTL and column families
  {ok, Db, [DefaultCF, CF1Handle, CF2Handle]} = rocksdb:open_with_ttl_cf(
    "test_cf.db",
    [],
    [{"default", [], 3600}, {"cf1", [], 1}, {"cf2", [], 7200}],
    false
  ),

  %% Write to different column families
  ok = rocksdb:put(Db, DefaultCF, <<"key1">>, <<"value1">>, []),
  ok = rocksdb:put(Db, CF1Handle, <<"key2">>, <<"value2">>, []),
  ok = rocksdb:put(Db, CF2Handle, <<"key3">>, <<"value3">>, []),

  %% Verify reads
  {ok, <<"value1">>} = rocksdb:get(Db, DefaultCF, <<"key1">>, []),
  {ok, <<"value2">>} = rocksdb:get(Db, CF1Handle, <<"key2">>, []),
  {ok, <<"value3">>} = rocksdb:get(Db, CF2Handle, <<"key3">>, []),

  %% Wait for CF1 TTL to expire (1 second)
  timer:sleep(2000),
  rocksdb:compact_range(Db, CF1Handle, <<"0">>, <<"z">>, []),

  %% CF1 key should be expired, others should still exist
  not_found = rocksdb:get(Db, CF1Handle, <<"key2">>, []),
  {ok, <<"value1">>} = rocksdb:get(Db, DefaultCF, <<"key1">>, []),
  {ok, <<"value3">>} = rocksdb:get(Db, CF2Handle, <<"key3">>, []),

  rocksdb:close(Db),
  rocksdb:destroy("test_cf.db", []),
  rocksdb_test_util:rm_rf("test_cf.db"),
  ok.

%% Test get_ttl/2 function
get_ttl_test() ->
  rocksdb_test_util:rm_rf("test_get_ttl.db"),
  %% First create the database with a column family
  {ok, Db1} = rocksdb:open("test_get_ttl.db", [{create_if_missing, true}]),
  {ok, _CF1} = rocksdb:create_column_family(Db1, "cf1", []),
  rocksdb:close(Db1),

  {ok, Db, [DefaultCF, CF1Handle]} = rocksdb:open_with_ttl_cf(
    "test_get_ttl.db",
    [],
    [{"default", [], 3600}, {"cf1", [], 1800}],
    false
  ),

  %% Get TTL for column families
  {ok, 3600} = rocksdb:get_ttl(Db, DefaultCF),
  {ok, 1800} = rocksdb:get_ttl(Db, CF1Handle),

  rocksdb:close(Db),
  rocksdb:destroy("test_get_ttl.db", []),
  rocksdb_test_util:rm_rf("test_get_ttl.db"),
  ok.

%% Test set_ttl/2 and set_ttl/3 functions
set_ttl_test() ->
  rocksdb_test_util:rm_rf("test_set_ttl.db"),
  %% First create the database with a column family
  {ok, Db1} = rocksdb:open("test_set_ttl.db", [{create_if_missing, true}]),
  {ok, _CF1} = rocksdb:create_column_family(Db1, "cf1", []),
  rocksdb:close(Db1),

  {ok, Db, [DefaultCF, CF1Handle]} = rocksdb:open_with_ttl_cf(
    "test_set_ttl.db",
    [],
    [{"default", [], 3600}, {"cf1", [], 1800}],
    false
  ),

  %% Get initial TTL values
  {ok, 3600} = rocksdb:get_ttl(Db, DefaultCF),
  {ok, 1800} = rocksdb:get_ttl(Db, CF1Handle),

  %% Set new TTL for specific column family
  ok = rocksdb:set_ttl(Db, CF1Handle, 900),
  {ok, 900} = rocksdb:get_ttl(Db, CF1Handle),

  %% Set default TTL
  ok = rocksdb:set_ttl(Db, 7200),

  rocksdb:close(Db),
  rocksdb:destroy("test_set_ttl.db", []),
  rocksdb_test_util:rm_rf("test_set_ttl.db"),
  ok.

%% Test create_column_family_with_ttl/4 function
create_cf_with_ttl_test() ->
  rocksdb_test_util:rm_rf("test_create_cf_ttl.db"),
  {ok, Db} = rocksdb:open_with_ttl("test_create_cf_ttl.db", [{create_if_missing, true}], 3600, false),

  %% Create a new column family with a specific TTL
  {ok, NewCF} = rocksdb:create_column_family_with_ttl(Db, "new_cf", [], 1),

  %% Write to the new column family
  ok = rocksdb:put(Db, NewCF, <<"key">>, <<"value">>, []),
  {ok, <<"value">>} = rocksdb:get(Db, NewCF, <<"key">>, []),

  %% Get TTL for the new column family
  {ok, 1} = rocksdb:get_ttl(Db, NewCF),

  %% Wait for TTL to expire
  timer:sleep(2000),
  rocksdb:compact_range(Db, NewCF, <<"0">>, <<"z">>, []),

  %% Key should be expired
  not_found = rocksdb:get(Db, NewCF, <<"key">>, []),

  rocksdb:close(Db),
  rocksdb:destroy("test_create_cf_ttl.db", []),
  rocksdb_test_util:rm_rf("test_create_cf_ttl.db"),
  ok.

%% Test that TTL operations fail on non-TTL database
non_ttl_db_error_test() ->
  rocksdb_test_util:rm_rf("test_non_ttl.db"),
  {ok, Db} = rocksdb:open("test_non_ttl.db", [{create_if_missing, true}]),
  {ok, CF} = rocksdb:create_column_family(Db, "cf1", []),

  %% These should fail on non-TTL database
  {error, _} = rocksdb:get_ttl(Db, CF),
  {error, _} = rocksdb:set_ttl(Db, 3600),
  {error, _} = rocksdb:set_ttl(Db, CF, 3600),
  {error, _} = rocksdb:create_column_family_with_ttl(Db, "cf2", [], 3600),

  rocksdb:close(Db),
  rocksdb:destroy("test_non_ttl.db", []),
  rocksdb_test_util:rm_rf("test_non_ttl.db"),
  ok.

