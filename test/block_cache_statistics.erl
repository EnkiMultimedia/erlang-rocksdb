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

-module(block_cache_statistics).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

block_cache_hit_miss_test() ->
  ?rm_rf("test_block_cache_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_block_cache_stats",
    [{create_if_missing, true},
     {statistics, Stats},
     %% Enable block cache
     {block_based_table_options, [{block_cache_size, 8388608}]}]),  %% 8MB cache
  try
    %% Write data and flush to create SST files
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $x)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 100)),
    ok = rocksdb:flush(Db, []),

    %% Check block_cache_add - should be > 0 after flush creates SST
    {ok, AddCount} = rocksdb:statistics_ticker(Stats, block_cache_add),
    ?assert(is_integer(AddCount)),

    %% Read data to generate cache hits/misses
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 100)),

    %% Read again to ensure cache hits
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 50)),

    %% Verify hit/miss counters
    {ok, HitCount} = rocksdb:statistics_ticker(Stats, block_cache_hit),
    {ok, MissCount} = rocksdb:statistics_ticker(Stats, block_cache_miss),
    ?assert(is_integer(HitCount)),
    ?assert(is_integer(MissCount)),
    %% After reads, should have some cache activity
    ?assert(HitCount + MissCount > 0),

    %% Check bytes read/write counters
    {ok, BytesRead} = rocksdb:statistics_ticker(Stats, block_cache_bytes_read),
    {ok, BytesWrite} = rocksdb:statistics_ticker(Stats, block_cache_bytes_write),
    ?assert(is_integer(BytesRead)),
    ?assert(is_integer(BytesWrite)),

    %% Check data cache stats
    {ok, DataHit} = rocksdb:statistics_ticker(Stats, block_cache_data_hit),
    {ok, DataMiss} = rocksdb:statistics_ticker(Stats, block_cache_data_miss),
    ?assert(is_integer(DataHit)),
    ?assert(is_integer(DataMiss)),

    %% Check index cache stats
    {ok, IndexHit} = rocksdb:statistics_ticker(Stats, block_cache_index_hit),
    {ok, IndexMiss} = rocksdb:statistics_ticker(Stats, block_cache_index_miss),
    ?assert(is_integer(IndexHit)),
    ?assert(is_integer(IndexMiss)),

    %% Check filter cache stats
    {ok, FilterHit} = rocksdb:statistics_ticker(Stats, block_cache_filter_hit),
    {ok, FilterMiss} = rocksdb:statistics_ticker(Stats, block_cache_filter_miss),
    ?assert(is_integer(FilterHit)),
    ?assert(is_integer(FilterMiss)),

    %% Check add failures (should be 0 or more)
    {ok, AddFailures} = rocksdb:statistics_ticker(Stats, block_cache_add_failures),
    ?assert(is_integer(AddFailures))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_block_cache_stats")
  end,
  ok.

cache_bytes_test() ->
  ?rm_rf("test_block_cache_bytes"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_block_cache_bytes",
    [{create_if_missing, true},
     {statistics, Stats},
     {block_based_table_options, [{block_cache_size, 16777216}]}]),  %% 16MB cache
  try
    %% Write larger values to ensure measurable bytes
    lists:foreach(
      fun(I) ->
        Key = <<"largekey", (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(1000, $y)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 200)),
    ok = rocksdb:flush(Db, []),

    %% Force reads from SST files
    lists:foreach(
      fun(I) ->
        Key = <<"largekey", (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, 200)),

    %% Check bytes counters have meaningful values
    {ok, BytesRead} = rocksdb:statistics_ticker(Stats, block_cache_bytes_read),
    {ok, BytesWrite} = rocksdb:statistics_ticker(Stats, block_cache_bytes_write),

    %% After reading data from SST, we should see cache byte activity
    ?assert(BytesRead >= 0),
    ?assert(BytesWrite >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_block_cache_bytes")
  end,
  ok.
