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

-module(db_operation_histograms).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

db_write_histogram_test() ->
  ?rm_rf("test_db_write_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_db_write_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Perform put operations to generate db_write histogram data
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 50)),

    %% Check db_write histogram has recorded data
    {ok, WriteHist} = rocksdb:statistics_histogram(Stats, db_write),
    ?assert(is_map(WriteHist)),
    ?assert(maps:get(count, WriteHist) > 0),
    ?assert(maps:get(sum, WriteHist) >= 0),
    ?assert(is_float(maps:get(median, WriteHist))),
    ?assert(is_float(maps:get(percentile95, WriteHist))),
    ?assert(is_float(maps:get(percentile99, WriteHist))),
    ?assert(is_float(maps:get(average, WriteHist)))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_db_write_hist")
  end,
  ok.

db_get_histogram_test() ->
  ?rm_rf("test_db_get_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_db_get_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write some data first
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 20)),

    %% Perform get operations to generate db_get histogram data
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 20)),

    %% Check db_get histogram has recorded data
    {ok, GetHist} = rocksdb:statistics_histogram(Stats, db_get),
    ?assert(is_map(GetHist)),
    ?assert(maps:get(count, GetHist) > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_db_get_hist")
  end,
  ok.

db_seek_histogram_test() ->
  ?rm_rf("test_db_seek_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_db_seek_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write some data first
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 20)),

    %% Use iterator with seeks to generate db_seek histogram data
    {ok, Iter} = rocksdb:iterator(Db, []),
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        {ok, _, _} = rocksdb:iterator_move(Iter, Key)
      end,
      lists:seq(1, 10)),
    ok = rocksdb:iterator_close(Iter),

    %% Check db_seek histogram has recorded data
    {ok, SeekHist} = rocksdb:statistics_histogram(Stats, db_seek),
    ?assert(is_map(SeekHist)),
    ?assert(maps:get(count, SeekHist) > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_db_seek_hist")
  end,
  ok.

flush_time_histogram_test() ->
  ?rm_rf("test_flush_time_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_flush_time_hist",
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

    %% Force a flush
    ok = rocksdb:flush(Db, []),

    %% Check flush_time histogram has recorded data
    {ok, FlushHist} = rocksdb:statistics_histogram(Stats, flush_time),
    ?assert(is_map(FlushHist)),
    ?assert(maps:get(count, FlushHist) > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_flush_time_hist")
  end,
  ok.

compaction_time_histogram_test() ->
  ?rm_rf("test_compaction_time_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_compaction_time_hist",
    [{create_if_missing, true},
     {statistics, Stats},
     {write_buffer_size, 1024}]),
  try
    %% Write enough data to trigger flush and create multiple SST files
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(200, $v)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 100)),

    %% Flush to ensure data is on disk
    ok = rocksdb:flush(Db, []),

    %% Force compaction
    ok = rocksdb:compact_range(Db, undefined, undefined, []),

    %% Check compaction_time histogram (may be 0 if compaction not needed but should be readable)
    {ok, CompactionHist} = rocksdb:statistics_histogram(Stats, compaction_time),
    ?assert(is_map(CompactionHist)),
    ?assert(is_integer(maps:get(count, CompactionHist))),
    ?assert(maps:get(count, CompactionHist) >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_compaction_time_hist")
  end,
  ok.

all_core_histograms_readable_test() ->
  ?rm_rf("test_all_core_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_all_core_hist",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Test that all core histograms are readable
    Histograms = [
      db_get,
      db_write,
      db_multiget,
      db_seek,
      compaction_time,
      flush_time
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
    ?rm_rf("test_all_core_hist")
  end,
  ok.
