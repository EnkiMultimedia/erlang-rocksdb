%% Copyright (c) 2016-2026 Benoit Chesneau.
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

-module(flush_wal).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test flush_wal with default options
flush_wal_default_test() ->
  ?rm_rf("erocksdb.flush_wal_default.test"),
  {ok, Ref} = rocksdb:open("erocksdb.flush_wal_default.test", [{create_if_missing, true}]),

  %% Write some data
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  ok = rocksdb:put(Ref, <<"key2">>, <<"value2">>, []),

  %% Flush WAL with default options
  ok = rocksdb:flush_wal(Ref, []),

  %% Verify data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value2">>} = rocksdb:get(Ref, <<"key2">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.flush_wal_default.test", []),
  ?rm_rf("erocksdb.flush_wal_default.test"),
  ok.

%% Test flush_wal with sync=true
flush_wal_sync_test() ->
  ?rm_rf("erocksdb.flush_wal_sync.test"),
  {ok, Ref} = rocksdb:open("erocksdb.flush_wal_sync.test", [{create_if_missing, true}]),

  %% Write some data
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  ok = rocksdb:put(Ref, <<"key2">>, <<"value2">>, []),

  %% Flush WAL with sync=true (also calls SyncWAL afterwards)
  ok = rocksdb:flush_wal(Ref, [{sync, true}]),

  %% Verify data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value2">>} = rocksdb:get(Ref, <<"key2">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.flush_wal_sync.test", []),
  ?rm_rf("erocksdb.flush_wal_sync.test"),
  ok.

%% Test flush_wal with rate_limiter_priority
flush_wal_rate_limiter_priority_test() ->
  ?rm_rf("erocksdb.flush_wal_priority.test"),
  {ok, Ref} = rocksdb:open("erocksdb.flush_wal_priority.test", [{create_if_missing, true}]),

  %% Write some data
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),

  %% Flush WAL with different rate limiter priorities
  ok = rocksdb:flush_wal(Ref, [{rate_limiter_priority, io_low}]),
  ok = rocksdb:put(Ref, <<"key2">>, <<"value2">>, []),
  ok = rocksdb:flush_wal(Ref, [{rate_limiter_priority, io_high}]),
  ok = rocksdb:put(Ref, <<"key3">>, <<"value3">>, []),
  ok = rocksdb:flush_wal(Ref, [{rate_limiter_priority, io_total}]),

  %% Verify data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value2">>} = rocksdb:get(Ref, <<"key2">>, []),
  {ok, <<"value3">>} = rocksdb:get(Ref, <<"key3">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.flush_wal_priority.test", []),
  ?rm_rf("erocksdb.flush_wal_priority.test"),
  ok.

%% Test flush_wal with combined options
flush_wal_combined_test() ->
  ?rm_rf("erocksdb.flush_wal_combined.test"),
  {ok, Ref} = rocksdb:open("erocksdb.flush_wal_combined.test", [{create_if_missing, true}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 100)],

  %% Flush WAL with combined options
  ok = rocksdb:flush_wal(Ref, [{sync, true}, {rate_limiter_priority, io_mid}]),

  %% Verify data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value100">>} = rocksdb:get(Ref, <<"key100">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.flush_wal_combined.test", []),
  ?rm_rf("erocksdb.flush_wal_combined.test"),
  ok.
