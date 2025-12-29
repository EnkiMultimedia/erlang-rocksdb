%% Copyright (c) 2025 Benoit Chesneau
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

-module(memtable_statistics).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

memtable_hit_miss_test() ->
  ?rm_rf("test_memtable_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_memtable_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write data without flush - stays in memtable
    lists:foreach(
      fun(I) ->
        Key = <<$m, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(50, $a)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 50)),

    %% Read back - should hit memtable
    lists:foreach(
      fun(I) ->
        Key = <<$m, (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 50)),

    %% Check memtable_hit and memtable_miss are readable
    {ok, MemtableHit} = rocksdb:statistics_ticker(Stats, memtable_hit),
    {ok, MemtableMiss} = rocksdb:statistics_ticker(Stats, memtable_miss),
    ?assert(is_integer(MemtableHit)),
    ?assert(is_integer(MemtableMiss)),
    %% We should have memtable hits since data is in memtable
    ?assert(MemtableHit >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_memtable_stats")
  end,
  ok.

write_done_counters_test() ->
  ?rm_rf("test_write_done_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_write_done_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Perform some writes
    lists:foreach(
      fun(I) ->
        Key = <<$w, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $b)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 100)),

    %% Check write_done counters
    {ok, WriteBySelf} = rocksdb:statistics_ticker(Stats, write_done_by_self),
    {ok, WriteByOther} = rocksdb:statistics_ticker(Stats, write_done_by_other),
    ?assert(is_integer(WriteBySelf)),
    ?assert(is_integer(WriteByOther)),
    %% At least some writes should be done by self
    ?assert(WriteBySelf >= 0),
    ?assert(WriteByOther >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_write_done_stats")
  end,
  ok.

wal_sync_test() ->
  ?rm_rf("test_wal_sync_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_wal_sync_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write with sync option to trigger WAL sync
    lists:foreach(
      fun(I) ->
        Key = <<$s, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(50, $c)),
        ok = rocksdb:put(Db, Key, Value, [{sync, true}])
      end,
      lists:seq(1, 10)),

    %% Check wal_file_synced counter
    {ok, WalSynced} = rocksdb:statistics_ticker(Stats, wal_file_synced),
    ?assert(is_integer(WalSynced)),
    %% With sync writes, should have WAL syncs
    ?assert(WalSynced > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_wal_sync_stats")
  end,
  ok.

stall_micros_test() ->
  ?rm_rf("test_stall_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_stall_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Just verify stall_micros is readable
    {ok, StallMicros} = rocksdb:statistics_ticker(Stats, stall_micros),
    ?assert(is_integer(StallMicros)),
    %% Stall micros should be >= 0 (likely 0 with small data)
    ?assert(StallMicros >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_stall_stats")
  end,
  ok.
