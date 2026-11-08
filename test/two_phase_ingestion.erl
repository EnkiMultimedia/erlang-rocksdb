%%% -*- erlang -*-
%%%
%%% Copyright (c) 2026 Benoit Chesneau
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%% http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

%% Tests for prepare_ingestion / commit_ingestion / abort_ingestion
%% (DB::PrepareFileIngestion, RocksDB 11.5).
-module(two_phase_ingestion).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

prepare_then_commit_test() ->
  with_db("prepare_commit", fun(Db, Sst) ->
    Sst1 = write_sst(Sst, "1", [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]),
    {ok, Ingestion} = rocksdb:prepare_ingestion(Db, [Sst1], []),
    %% nothing is visible until the commit
    not_found = rocksdb:get(Db, <<"a">>, []),
    ok = rocksdb:commit_ingestion(Db, Ingestion),
    {ok, <<"1">>} = rocksdb:get(Db, <<"a">>, []),
    {ok, <<"2">>} = rocksdb:get(Db, <<"b">>, []),
    %% a handle is spent once committed
    ?assertError(badarg, rocksdb:commit_ingestion(Db, Ingestion)),
    ?assertError(badarg, rocksdb:abort_ingestion(Ingestion))
  end).

abort_test() ->
  with_db("abort", fun(Db, Sst) ->
    Sst1 = write_sst(Sst, "1", [{<<"a">>, <<"1">>}]),
    {ok, Ingestion} = rocksdb:prepare_ingestion(Db, [Sst1], []),
    %% prepare stages a copy of the file in the database directory
    1 = staged_files(Sst),
    ok = rocksdb:abort_ingestion(Ingestion),
    0 = staged_files(Sst),
    not_found = rocksdb:get(Db, <<"a">>, []),
    ?assertError(badarg, rocksdb:commit_ingestion(Db, Ingestion)),
    ?assertError(badarg, rocksdb:abort_ingestion(Ingestion)),
    %% the external file is untouched and can still be ingested
    ok = rocksdb:ingest_external_file(Db, [Sst1], []),
    {ok, <<"1">>} = rocksdb:get(Db, <<"a">>, [])
  end).

commit_several_column_families_test() ->
  Dir = test_dir("multi_cf"),
  {ok, Db, [Default, Cf]} =
    rocksdb:open(filename:join(Dir, "db"),
                 [{create_if_missing, true}, {create_missing_column_families, true}],
                 [{"default", []}, {"other", []}]),
  Sst1 = write_sst(Dir, "1", [{<<"k">>, <<"default">>}]),
  Sst2 = write_sst(Dir, "2", [{<<"k">>, <<"other">>}]),
  {ok, I1} = rocksdb:prepare_ingestion(Db, Default, [Sst1], []),
  {ok, I2} = rocksdb:prepare_ingestion(Db, Cf, [Sst2], []),
  ok = rocksdb:commit_ingestion(Db, [I1, I2]),
  {ok, <<"default">>} = rocksdb:get(Db, Default, <<"k">>, []),
  {ok, <<"other">>} = rocksdb:get(Db, Cf, <<"k">>, []),
  ok = rocksdb:close(Db),
  ?rm_rf(Dir).

later_handle_wins_test() ->
  with_db("later_wins", fun(Db, Sst) ->
    Sst1 = write_sst(Sst, "1", [{<<"k">>, <<"first">>}]),
    Sst2 = write_sst(Sst, "2", [{<<"k">>, <<"second">>}]),
    {ok, I1} = rocksdb:prepare_ingestion(Db, [Sst1], []),
    {ok, I2} = rocksdb:prepare_ingestion(Db, [Sst2], []),
    ok = rocksdb:commit_ingestion(Db, [I1, I2]),
    {ok, <<"second">>} = rocksdb:get(Db, <<"k">>, [])
  end).

invalid_batch_keeps_handles_test() ->
  with_db("invalid_batch", fun(Db, Sst) ->
    Sst1 = write_sst(Sst, "1", [{<<"a">>, <<"1">>}]),
    {ok, Ingestion} = rocksdb:prepare_ingestion(Db, [Sst1], []),
    %% listed twice: rejected before reaching RocksDB, the handle stays usable
    ?assertError(badarg, rocksdb:commit_ingestion(Db, [Ingestion, Ingestion])),
    ?assertError(badarg, rocksdb:commit_ingestion(Db, [])),
    ok = rocksdb:commit_ingestion(Db, [Ingestion]),
    {ok, <<"1">>} = rocksdb:get(Db, <<"a">>, [])
  end).

prepare_error_test() ->
  with_db("prepare_error", fun(Db, Sst) ->
    Missing = filename:join(Sst, "missing.sst"),
    ?assertMatch({error, _}, rocksdb:prepare_ingestion(Db, [Missing], [])),
    ?assertError(badarg, rocksdb:prepare_ingestion(Db, [], []))
  end).

foreign_database_test() ->
  Dir = test_dir("foreign"),
  {ok, Db1} = rocksdb:open(filename:join(Dir, "db1"), [{create_if_missing, true}]),
  {ok, Db2, [_, Cf2]} =
    rocksdb:open(filename:join(Dir, "db2"),
                 [{create_if_missing, true}, {create_missing_column_families, true}],
                 [{"default", []}, {"other", []}]),
  Sst1 = write_sst(Dir, "1", [{<<"a">>, <<"1">>}]),
  ?assertError(badarg, rocksdb:prepare_ingestion(Db1, Cf2, [Sst1], [])),
  {ok, Ingestion} = rocksdb:prepare_ingestion(Db2, [Sst1], []),
  %% a handle can only be committed into the database that prepared it
  ?assertError(badarg, rocksdb:commit_ingestion(Db1, Ingestion)),
  ok = rocksdb:commit_ingestion(Db2, Ingestion),
  ok = rocksdb:close(Db1),
  ok = rocksdb:close(Db2),
  ?rm_rf(Dir).

close_rolls_back_test() ->
  Dir = test_dir("close"),
  DbPath = filename:join(Dir, "db"),
  {ok, Db} = rocksdb:open(DbPath, [{create_if_missing, true}]),
  Sst1 = write_sst(Dir, "1", [{<<"a">>, <<"1">>}]),
  {ok, Ingestion} = rocksdb:prepare_ingestion(Db, [Sst1], []),
  1 = staged_files(Dir),
  ok = rocksdb:close(Db),
  0 = staged_files(Dir),
  ?assertError(badarg, rocksdb:abort_ingestion(Ingestion)),
  {ok, Db2} = rocksdb:open(DbPath, []),
  not_found = rocksdb:get(Db2, <<"a">>, []),
  ok = rocksdb:close(Db2),
  ?rm_rf(Dir).

garbage_collected_handle_rolls_back_test() ->
  with_db("gc", fun(Db, Sst) ->
    Sst1 = write_sst(Sst, "1", [{<<"a">>, <<"1">>}]),
    {Pid, Ref} = spawn_monitor(fun() ->
      {ok, Ingestion} = rocksdb:prepare_ingestion(Db, [Sst1], []),
      1 = staged_files(Sst),
      %% keep the handle alive until the check above is done
      _ = erlang:phash2(Ingestion)
    end),
    receive {'DOWN', Ref, process, Pid, Reason} -> ?assertEqual(normal, Reason) end,
    erlang:garbage_collect(),
    0 = staged_files(Sst),
    not_found = rocksdb:get(Db, <<"a">>, []),
    ok = rocksdb:ingest_external_file(Db, [Sst1], []),
    {ok, <<"1">>} = rocksdb:get(Db, <<"a">>, [])
  end).

%% ingest_external_file_{prepare,run}_micros (RocksDB 11.5): one sample per
%% successful ingestion, from both the one-shot and the two-phase path
ingestion_histograms_test() ->
  Dir = test_dir("histograms"),
  {ok, Stats} = rocksdb:new_statistics(),
  {ok, Db} = rocksdb:open(filename:join(Dir, "db"),
                          [{create_if_missing, true}, {statistics, Stats}]),
  {ok, #{count := 0}} = rocksdb:statistics_histogram(Stats, ingest_external_file_prepare_micros),
  {ok, #{count := 0}} = rocksdb:statistics_histogram(Stats, ingest_external_file_run_micros),

  ok = rocksdb:ingest_external_file(Db, [write_sst(Dir, "1", [{<<"a">>, <<"1">>}])], []),
  {ok, #{count := 1}} = rocksdb:statistics_histogram(Stats, ingest_external_file_prepare_micros),
  {ok, #{count := 1}} = rocksdb:statistics_histogram(Stats, ingest_external_file_run_micros),

  {ok, I} = rocksdb:prepare_ingestion(Db, [write_sst(Dir, "2", [{<<"b">>, <<"2">>}])], []),
  {ok, #{count := 2}} = rocksdb:statistics_histogram(Stats, ingest_external_file_prepare_micros),
  {ok, #{count := 1}} = rocksdb:statistics_histogram(Stats, ingest_external_file_run_micros),
  ok = rocksdb:commit_ingestion(Db, I),
  {ok, #{count := 2}} = rocksdb:statistics_histogram(Stats, ingest_external_file_run_micros),

  ok = rocksdb:close(Db),
  ok = rocksdb:release_statistics(Stats),
  ?rm_rf(Dir).

compaction_queue_properties_test() ->
  with_db("properties", fun(Db, _Dir) ->
    lists:foreach(
      fun(Name) ->
        {ok, Value} = rocksdb:get_property(Db, Name),
        ?assert(is_integer(binary_to_integer(Value)))
      end,
      [<<"rocksdb.num-unscheduled-compactions">>,
       <<"rocksdb.num-running-compactions">>,
       <<"rocksdb.num-running-remote-compactions">>])
  end).

%% helpers

with_db(Name, Fun) ->
  Dir = test_dir(Name),
  {ok, Db} = rocksdb:open(filename:join(Dir, "db"), [{create_if_missing, true}]),
  try
    Fun(Db, Dir)
  after
    rocksdb:close(Db),
    ?rm_rf(Dir)
  end.

test_dir(Name) ->
  Dir = "erocksdb.two_phase_ingestion." ++ Name ++ ".test",
  ?rm_rf(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "x")),
  Dir.

%% SST files inside the database directory (nothing is flushed in these tests,
%% so they are the files staged by prepare_ingestion)
staged_files(Dir) ->
  length(filelib:wildcard(filename:join([Dir, "db", "*.sst"]))).

write_sst(Dir, Name, KVs) ->
  Path = filename:join(Dir, Name ++ ".sst"),
  {ok, Writer} = rocksdb:sst_file_writer_open([], Path),
  [ok = rocksdb:sst_file_writer_put(Writer, K, V) || {K, V} <- KVs],
  ok = rocksdb:sst_file_writer_finish(Writer),
  ok = rocksdb:release_sst_file_writer(Writer),
  Path.
