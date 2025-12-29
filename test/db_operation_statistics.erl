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

-module(db_operation_statistics).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

keys_written_read_test() ->
  ?rm_rf("test_db_op_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_db_op_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write N keys
    N = 50,
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        Value = <<"value", (integer_to_binary(I))/binary>>,
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, N)),

    %% Check number_keys_written
    {ok, KeysWritten} = rocksdb:statistics_ticker(Stats, number_keys_written),
    ?assert(is_integer(KeysWritten)),
    ?assert(KeysWritten >= N),

    %% Check bytes_written
    {ok, BytesWritten} = rocksdb:statistics_ticker(Stats, bytes_written),
    ?assert(is_integer(BytesWritten)),
    ?assert(BytesWritten > 0),

    %% Read M keys
    M = 30,
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        {ok, _} = rocksdb:get(Db, Key, [])
      end,
      lists:seq(1, M)),

    %% Check number_keys_read
    {ok, KeysRead} = rocksdb:statistics_ticker(Stats, number_keys_read),
    ?assert(is_integer(KeysRead)),
    ?assert(KeysRead >= M),

    %% Check bytes_read
    {ok, BytesRead} = rocksdb:statistics_ticker(Stats, bytes_read),
    ?assert(is_integer(BytesRead)),
    ?assert(BytesRead >= 0),

    %% Check number_keys_updated (update some keys)
    lists:foreach(
      fun(I) ->
        Key = <<"key", (integer_to_binary(I))/binary>>,
        Value = <<"updated", (integer_to_binary(I))/binary>>,
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 10)),

    {ok, KeysUpdated} = rocksdb:statistics_ticker(Stats, number_keys_updated),
    ?assert(is_integer(KeysUpdated))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_db_op_stats")
  end,
  ok.

iterator_stats_test() ->
  ?rm_rf("test_iter_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(
    "test_iter_stats",
    [{create_if_missing, true},
     {statistics, Stats}]),
  try
    %% Write some data
    lists:foreach(
      fun(I) ->
        Key = <<"iter_key", (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(100, $x)),
        ok = rocksdb:put(Db, Key, Value, [])
      end,
      lists:seq(1, 100)),

    %% Use iterator with seek, next, prev
    {ok, Iter} = rocksdb:iterator(Db, []),
    try
      %% Seek to first
      {ok, _, _} = rocksdb:iterator_move(Iter, first),

      %% Move forward several times
      lists:foreach(
        fun(_) ->
          case rocksdb:iterator_move(Iter, next) of
            {ok, _, _} -> ok;
            {error, invalid_iterator} -> ok
          end
        end,
        lists:seq(1, 50)),

      %% Move backward several times
      lists:foreach(
        fun(_) ->
          case rocksdb:iterator_move(Iter, prev) of
            {ok, _, _} -> ok;
            {error, invalid_iterator} -> ok
          end
        end,
        lists:seq(1, 20)),

      %% Seek to specific key
      _ = rocksdb:iterator_move(Iter, {seek, <<"iter_key50">>})
    after
      ok = rocksdb:iterator_close(Iter)
    end,

    %% Check iterator counters
    {ok, SeekCount} = rocksdb:statistics_ticker(Stats, number_db_seek),
    {ok, NextCount} = rocksdb:statistics_ticker(Stats, number_db_next),
    {ok, PrevCount} = rocksdb:statistics_ticker(Stats, number_db_prev),
    ?assert(is_integer(SeekCount)),
    ?assert(is_integer(NextCount)),
    ?assert(is_integer(PrevCount)),
    ?assert(SeekCount > 0),  %% At least first and seek operations
    ?assert(NextCount > 0),
    ?assert(PrevCount > 0),

    %% Check "found" counters
    {ok, SeekFound} = rocksdb:statistics_ticker(Stats, number_db_seek_found),
    {ok, NextFound} = rocksdb:statistics_ticker(Stats, number_db_next_found),
    {ok, PrevFound} = rocksdb:statistics_ticker(Stats, number_db_prev_found),
    ?assert(is_integer(SeekFound)),
    ?assert(is_integer(NextFound)),
    ?assert(is_integer(PrevFound)),

    %% Check iter_bytes_read
    {ok, IterBytesRead} = rocksdb:statistics_ticker(Stats, iter_bytes_read),
    ?assert(is_integer(IterBytesRead)),
    ?assert(IterBytesRead > 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_iter_stats")
  end,
  ok.
