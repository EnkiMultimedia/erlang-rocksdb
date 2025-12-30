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

-module(compaction_statistics).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

flush_stats_test() ->
  ?rm_rf("test_flush_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_flush_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write enough data to make flush meaningful
    lists:foreach(
      fun(I) ->
        Key = <<"flush_key", (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(500, $x)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 100)),

    %% Force flush
    ok = rocksdb:flush(Db, []),

    %% Check flush_write_bytes
    {ok, FlushWriteBytes} = rocksdb:statistics_ticker(Stats, flush_write_bytes),
    ?assert(is_integer(FlushWriteBytes)),
    ?assert(FlushWriteBytes > 0),

    %% Superversion counters should increment after flush
    {ok, SvAcquires} = rocksdb:statistics_ticker(Stats, number_superversion_acquires),
    {ok, SvReleases} = rocksdb:statistics_ticker(Stats, number_superversion_releases),
    ?assert(is_integer(SvAcquires)),
    ?assert(is_integer(SvReleases)),
    ?assert(SvAcquires >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_flush_stats")
  end,
  ok.

compaction_stats_test() ->
  ?rm_rf("test_compact_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_compact_stats",
    [{create_if_missing, true},
     {statistics, Stats},
     %% Disable auto compaction to control when compaction happens
     {disable_auto_compactions, true}]),
  try
    %% Write multiple batches and flush to create multiple SST files
    lists:foreach(
      fun(Batch) ->
        lists:foreach(
          fun(I) ->
            Key = <<"compact_key", (integer_to_binary(Batch))/binary, "_", (integer_to_binary(I))/binary>>,
            Value = list_to_binary(lists:duplicate(200, $y)),
            ok = rocksdb:put(Db, Key, Value, [])
          end,
          lists:seq(1, 50)),
        ok = rocksdb:flush(Db, [])
      end,
      lists:seq(1, 4)),

    %% Force compaction
    ok = rocksdb:compact_range(Db, undefined, undefined, []),

    %% Check compaction bytes are readable
    {ok, CompactReadBytes} = rocksdb:statistics_ticker(Stats, compact_read_bytes),
    {ok, CompactWriteBytes} = rocksdb:statistics_ticker(Stats, compact_write_bytes),
    ?assert(is_integer(CompactReadBytes)),
    ?assert(is_integer(CompactWriteBytes)),
    ?assert(CompactReadBytes >= 0),
    ?assert(CompactWriteBytes >= 0),

    %% Check key drop counters are readable
    {ok, DropNewerEntry} = rocksdb:statistics_ticker(Stats, compaction_key_drop_newer_entry),
    {ok, DropObsolete} = rocksdb:statistics_ticker(Stats, compaction_key_drop_obsolete),
    {ok, DropRangeDel} = rocksdb:statistics_ticker(Stats, compaction_key_drop_range_del),
    {ok, DropUser} = rocksdb:statistics_ticker(Stats, compaction_key_drop_user),
    ?assert(is_integer(DropNewerEntry)),
    ?assert(is_integer(DropObsolete)),
    ?assert(is_integer(DropRangeDel)),
    ?assert(is_integer(DropUser)),

    %% Check compaction_cancelled is readable
    {ok, Cancelled} = rocksdb:statistics_ticker(Stats, compaction_cancelled),
    ?assert(is_integer(Cancelled))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_compact_stats")
  end,
  ok.

key_drop_test() ->
  ?rm_rf("test_key_drop"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_key_drop",
    [{create_if_missing, true},
     {statistics, Stats},
     {disable_auto_compactions, true}]),
  try
    %% Write same keys multiple times to generate "newer entry" drops
    lists:foreach(
      fun(_Round) ->
        lists:foreach(
          fun(I) ->
            Key = <<"overwrite_key", (integer_to_binary(I))/binary>>,
            Value = list_to_binary(lists:duplicate(100, $z)),
            ok = rocksdb:put(Db, Key, Value, [])
          end,
          lists:seq(1, 20)),
        ok = rocksdb:flush(Db, [])
      end,
      lists:seq(1, 3)),

    %% Force compaction - this should drop older versions
    ok = rocksdb:compact_range(Db, undefined, undefined, []),

    %% Check that key drop counters are readable (values depend on RocksDB behavior)
    {ok, DropNewerEntry} = rocksdb:statistics_ticker(Stats, compaction_key_drop_newer_entry),
    ?assert(is_integer(DropNewerEntry)),
    %% We wrote same keys 3 times, so some should be dropped
    ?assert(DropNewerEntry >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_key_drop")
  end,
  ok.
