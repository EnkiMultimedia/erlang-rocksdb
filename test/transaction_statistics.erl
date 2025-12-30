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

-module(transaction_statistics).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

txn_mutex_overhead_test() ->
  ?rm_rf("test_txn_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_txn_stats",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Create and commit transactions to generate statistics
    lists:foreach(
      fun(I) ->
        {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
        Key = <<$t, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(50, $a)),
        ok = rocksdb:pessimistic_transaction_put(Txn, Key, Value),
        ok = rocksdb:pessimistic_transaction_commit(Txn),
        ok = rocksdb:release_pessimistic_transaction(Txn)
      end,
      lists:seq(1, 20)),

    %% Check txn_prepare_mutex_overhead is readable
    {ok, PrepareMutex} = rocksdb:statistics_ticker(Stats, txn_prepare_mutex_overhead),
    ?assert(is_integer(PrepareMutex)),
    ?assert(PrepareMutex >= 0),

    %% Check txn_old_commit_map_mutex_overhead is readable
    {ok, CommitMapMutex} = rocksdb:statistics_ticker(Stats, txn_old_commit_map_mutex_overhead),
    ?assert(is_integer(CommitMapMutex)),
    ?assert(CommitMapMutex >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_txn_stats")
  end,
  ok.

txn_duplicate_key_overhead_test() ->
  ?rm_rf("test_txn_dup_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_txn_dup_stats",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Write duplicate keys in the same transaction
    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
    lists:foreach(
      fun(I) ->
        Key = <<"dup_key">>,
        Value = list_to_binary([I | lists:duplicate(49, $b)]),
        ok = rocksdb:pessimistic_transaction_put(Txn, Key, Value)
      end,
      lists:seq(1, 10)),
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    %% Check txn_duplicate_key_overhead is readable
    {ok, DupKeyOverhead} = rocksdb:statistics_ticker(Stats, txn_duplicate_key_overhead),
    ?assert(is_integer(DupKeyOverhead)),
    ?assert(DupKeyOverhead >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_txn_dup_stats")
  end,
  ok.

txn_snapshot_mutex_overhead_test() ->
  ?rm_rf("test_txn_snapshot_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_txn_snapshot_stats",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Create transactions with snapshots
    lists:foreach(
      fun(I) ->
        {ok, Txn} = rocksdb:pessimistic_transaction(Db, [{set_snapshot, true}]),
        Key = <<$s, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(50, $c)),
        ok = rocksdb:pessimistic_transaction_put(Txn, Key, Value),
        ok = rocksdb:pessimistic_transaction_commit(Txn),
        ok = rocksdb:release_pessimistic_transaction(Txn)
      end,
      lists:seq(1, 10)),

    %% Check txn_snapshot_mutex_overhead is readable
    {ok, SnapshotMutex} = rocksdb:statistics_ticker(Stats, txn_snapshot_mutex_overhead),
    ?assert(is_integer(SnapshotMutex)),
    ?assert(SnapshotMutex >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_txn_snapshot_stats")
  end,
  ok.

txn_get_try_again_test() ->
  ?rm_rf("test_txn_try_again_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_txn_try_again_stats",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Insert some initial data
    {ok, Txn1} = rocksdb:pessimistic_transaction(Db, []),
    ok = rocksdb:pessimistic_transaction_put(Txn1, <<"key1">>, <<"value1">>),
    ok = rocksdb:pessimistic_transaction_commit(Txn1),
    ok = rocksdb:release_pessimistic_transaction(Txn1),

    %% Perform reads that might trigger try_again
    {ok, Txn2} = rocksdb:pessimistic_transaction(Db, [{set_snapshot, true}]),
    _ = rocksdb:pessimistic_transaction_get(Txn2, <<"key1">>, []),
    ok = rocksdb:pessimistic_transaction_commit(Txn2),
    ok = rocksdb:release_pessimistic_transaction(Txn2),

    %% Check txn_get_try_again is readable
    {ok, TryAgain} = rocksdb:statistics_ticker(Stats, txn_get_try_again),
    ?assert(is_integer(TryAgain)),
    ?assert(TryAgain >= 0)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_txn_try_again_stats")
  end,
  ok.

all_transaction_tickers_readable_test() ->
  ?rm_rf("test_all_txn_tickers"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_all_txn_tickers",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Test that all transaction tickers are readable
    Tickers = [
      txn_prepare_mutex_overhead,
      txn_old_commit_map_mutex_overhead,
      txn_duplicate_key_overhead,
      txn_snapshot_mutex_overhead,
      txn_get_try_again
    ],
    lists:foreach(
      fun(Ticker) ->
        {ok, Value} = rocksdb:statistics_ticker(Stats, Ticker),
        ?assert(is_integer(Value)),
        ?assert(Value >= 0)
      end,
      Tickers)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_all_txn_tickers")
  end,
  ok.

num_op_per_transaction_histogram_test() ->
  ?rm_rf("test_txn_ops_hist"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "test_txn_ops_hist",
    [{create_if_missing, true},
     {statistics, Stats}],
    [{"default", []}]),
  try
    %% Create a transaction with multiple operations
    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
    lists:foreach(
      fun(I) ->
        Key = <<$k, (integer_to_binary(I))/binary>>,
        Value = list_to_binary(lists:duplicate(50, $v)),
        ok = rocksdb:pessimistic_transaction_put(Txn, Key, Value)
      end,
      lists:seq(1, 10)),
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    %% Check num_op_per_transaction histogram is readable
    {ok, OpHist} = rocksdb:statistics_histogram(Stats, num_op_per_transaction),
    ?assert(is_map(OpHist)),
    ?assert(is_float(maps:get(median, OpHist))),
    ?assert(is_float(maps:get(percentile95, OpHist))),
    ?assert(is_float(maps:get(percentile99, OpHist))),
    ?assert(is_float(maps:get(average, OpHist))),
    ?assert(is_integer(maps:get(count, OpHist))),
    ?assert(is_integer(maps:get(sum, OpHist)))
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ?rm_rf("test_txn_ops_hist")
  end,
  ok.

