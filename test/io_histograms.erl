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

-module(io_histograms).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

sst_write_histogram_test() ->
  ?rm_rf("test_sst_write_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_sst_write_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write data to memtable
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 50)),

    %% Flush to generate SST file
    ok = rocksdb:flush(Db, []),

    %% Check sst_write_micros histogram has recorded data
    {ok, SstWriteHist} = rocksdb:statistics_histogram(Stats, sst_write_micros),
    ?assert(is_map(SstWriteHist)),
    ?assert(maps:get(count, SstWriteHist) > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_sst_write_hist")
  end,
  ok.

sst_read_histogram_test() ->
  ?rm_rf("test_sst_read_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_sst_read_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write and flush data to create SST file
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 50)),
    ok = rocksdb:flush(Db, []),

    %% Read data from SST file
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 50)),

    %% Check sst_read_micros histogram has recorded data
    {ok, SstReadHist} = rocksdb:statistics_histogram(Stats, sst_read_micros),
    ?assert(is_map(SstReadHist)),
    ?assert(maps:get(count, SstReadHist) >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_sst_read_hist")
  end,
  ok.

wal_sync_histogram_test() ->
  ?rm_rf("test_wal_sync_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_wal_sync_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write with sync option to trigger WAL sync
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [{sync, true}])
      end,
      lists:seq(1, 10)),

    %% Check wal_file_sync_micros histogram has recorded data
    {ok, WalSyncHist} = rocksdb:statistics_histogram(Stats, wal_file_sync_micros),
    ?assert(is_map(WalSyncHist)),
    ?assert(maps:get(count, WalSyncHist) > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_wal_sync_hist")
  end,
  ok.

bytes_per_read_write_test() ->
  ?rm_rf("test_bytes_per_rw"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_bytes_per_rw",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write data
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 20)),

    %% Flush to create SST file
    ok = rocksdb:flush(Db, []),

    %% Read data
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 20)),

    %% Check bytes_per_write histogram
    {ok, BytesWriteHist} = rocksdb:statistics_histogram(Stats, bytes_per_write),
    ?assert(is_map(BytesWriteHist)),
    ?assert(maps:get(count, BytesWriteHist) >= 0),

    %% Check bytes_per_read histogram
    {ok, BytesReadHist} = rocksdb:statistics_histogram(Stats, bytes_per_read),
    ?assert(is_map(BytesReadHist)),
    ?assert(maps:get(count, BytesReadHist) >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_bytes_per_rw")
  end,
  ok.

table_sync_histogram_test() ->
  ?rm_rf("test_table_sync_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_table_sync_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write and flush data
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 50)),
    ok = rocksdb:flush(Db, []),

    %% Check table_sync_micros histogram is readable
    {ok, TableSyncHist} = rocksdb:statistics_histogram(Stats, table_sync_micros),
    ?assert(is_map(TableSyncHist)),
    ?assert(is_integer(maps:get(count, TableSyncHist)))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_table_sync_hist")
  end,
  ok.

all_io_histograms_readable_test() ->
  ?rm_rf("test_all_io_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_all_io_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Test that all I/O histograms are readable
    Histograms = [
      sst_read_micros,
      sst_write_micros,
      table_sync_micros,
      wal_file_sync_micros,
      bytes_per_read,
      bytes_per_write
    ],
    lists:foreach(
      fun(Histogram) ->
        {ok, Data} = rocksdb:statistics_histogram(Stats, Histogram),
        ?assert(is_map(Data)),
        ?assert(is_float(maps:get(median, Data))),
        ?assert(is_float(maps:get(percentile95, Data))),
        ?assert(is_float(maps:get(percentile99, Data))),
        ?assert(is_float(maps:get(average, Data))),
        ?assert(is_float(maps:get(standard_deviation, Data))),
        ?assert(is_float(maps:get(max, Data))),
        ?assert(is_integer(maps:get(count, Data))),
        ?assert(is_integer(maps:get(sum, Data)))
      end,
      Histograms)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_all_io_hist")
  end,
  ok.
