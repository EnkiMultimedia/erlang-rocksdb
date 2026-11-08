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

%% Tests for options and APIs added in RocksDB 11.0/11.1.
-module(rocksdb_11_features).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% AbortAllCompactions / ResumeAllCompactions (RocksDB 11.0)
abort_resume_compactions_test() ->
  Dir = "erocksdb.abort_resume.test",
  ?rm_rf(Dir),
  {ok, Ref} = rocksdb:open(Dir, [{create_if_missing, true}]),
  [ok = rocksdb:put(Ref, <<I:32>>, <<I:32>>, []) || I <- lists:seq(1, 200)],

  ok = rocksdb:abort_all_compactions(Ref),
  %% paired resume (must be called as many times as abort)
  ok = rocksdb:resume_all_compactions(Ref),

  %% data still readable after abort/resume
  {ok, <<1:32>>} = rocksdb:get(Ref, <<1:32>>, []),
  {ok, <<200:32>>} = rocksdb:get(Ref, <<200:32>>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

%% AbortCompactions / ResumeCompactions (RocksDB 11.9)
abort_resume_cf_compactions_test() ->
  Dir = "erocksdb.abort_resume_cf.test",
  ?rm_rf(Dir),
  {ok, Ref, [Default, Cf]} =
    rocksdb:open(Dir, [{create_if_missing, true}, {create_missing_column_families, true}],
                 [{"default", []}, {"other", []}]),
  [begin
     ok = rocksdb:put(Ref, Default, <<I:32>>, <<I:32>>, []),
     ok = rocksdb:put(Ref, Cf, <<I:32>>, <<I:32>>, [])
   end || I <- lists:seq(1, 200)],

  ok = rocksdb:abort_compactions(Ref, Cf),
  %% only the aborted column family rejects compactions
  ?assertMatch({error, _}, rocksdb:compact_range(Ref, Cf, undefined, undefined, [])),
  ok = rocksdb:compact_range(Ref, Default, undefined, undefined, []),

  %% calls are counted: two aborts need two resumes
  ok = rocksdb:abort_compactions(Ref, Cf),
  ok = rocksdb:resume_compactions(Ref, Cf),
  ?assertMatch({error, _}, rocksdb:compact_range(Ref, Cf, undefined, undefined, [])),
  ok = rocksdb:resume_compactions(Ref, Cf),
  ok = rocksdb:compact_range(Ref, Cf, undefined, undefined, []),
  {ok, <<200:32>>} = rocksdb:get(Ref, Cf, <<200:32>>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

abort_compactions_foreign_cf_test() ->
  Dir1 = "erocksdb.abort_cf_foreign1.test",
  Dir2 = "erocksdb.abort_cf_foreign2.test",
  ?rm_rf(Dir1), ?rm_rf(Dir2),
  {ok, Ref1} = rocksdb:open(Dir1, [{create_if_missing, true}]),
  {ok, Ref2, [_, Cf2]} =
    rocksdb:open(Dir2, [{create_if_missing, true}, {create_missing_column_families, true}],
                 [{"default", []}, {"other", []}]),
  %% a column family of another database is rejected, not passed to RocksDB
  ?assertError(badarg, rocksdb:abort_compactions(Ref1, Cf2)),
  ?assertError(badarg, rocksdb:resume_compactions(Ref1, Cf2)),
  ok = rocksdb:close(Ref1),
  ok = rocksdb:close(Ref2),
  rocksdb:destroy(Dir1, []), rocksdb:destroy(Dir2, []),
  ?rm_rf(Dir1), ?rm_rf(Dir2),
  ok.

%% Options added in RocksDB 11.2 - 11.13. RocksDB writes the options it
%% applied to an OPTIONS-* file, so check the values landed there.
options_11_13_test() ->
  Dir = "erocksdb.opts_11_13.test",
  ?rm_rf(Dir),
  Opts = [{create_if_missing, true},
          {max_compaction_trigger_wakeup_seconds, 3600},
          {optimize_manifest_for_recovery, true},
          {reuse_manifest_on_open, true},
          {periodic_compaction_seconds, 86400},
          {read_triggered_compaction_threshold, 0.01},
          {min_tombstones_for_range_conversion, 16},
          {blob_file_writable_file_max_buffer_size, 1048576},
          {block_based_table_options, [{format_version, 8},
                                       {optimize_key_common_prefix, enabled}]}],
  {ok, Ref} = rocksdb:open(Dir, Opts),
  [ok = rocksdb:put(Ref, <<"prefix:", I:32>>, <<I:32>>, []) || I <- lists:seq(1, 500)],
  [ok = rocksdb:delete(Ref, <<"prefix:", I:32>>, []) || I <- lists:seq(1, 100)],
  ok = rocksdb:flush(Ref, []),
  ok = rocksdb:close(Ref),

  %% reopen exercises the manifest options and reads format_version 8 files
  {ok, Ref2} = rocksdb:open(Dir, Opts),
  not_found = rocksdb:get(Ref2, <<"prefix:", 1:32>>, []),
  {ok, <<500:32>>} = rocksdb:get(Ref2, <<"prefix:", 500:32>>, []),
  ok = rocksdb:close(Ref2),

  Applied = latest_options_file(Dir),
  lists:foreach(
    fun(Expected) -> ?assertNotEqual(nomatch, binary:match(Applied, Expected)) end,
    [<<"max_compaction_trigger_wakeup_seconds=3600">>,
     <<"optimize_manifest_for_recovery=true">>,
     <<"reuse_manifest_on_open=true">>,
     <<"periodic_compaction_seconds=86400">>,
     <<"read_triggered_compaction_threshold=0.01">>,
     <<"min_tombstones_for_range_conversion=16">>,
     <<"blob_file_writable_file_max_buffer_size=1048576">>,
     <<"format_version=8">>,
     <<"optimize_key_common_prefix=kEnabled">>]),

  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

use_direct_io_for_compaction_reads_test() ->
  Dir = "erocksdb.direct_io_compaction_reads.test",
  ?rm_rf(Dir),
  {ok, Ref} = rocksdb:open(Dir, [{create_if_missing, true},
                                 {use_direct_io_for_compaction_reads, true}]),
  [ok = rocksdb:put(Ref, <<I:32>>, <<I:32>>, []) || I <- lists:seq(1, 100)],
  ok = rocksdb:flush(Ref, []),
  ok = rocksdb:compact_range(Ref, undefined, undefined, []),
  {ok, <<100:32>>} = rocksdb:get(Ref, <<100:32>>, []),
  ok = rocksdb:close(Ref),
  ?assertNotEqual(nomatch, binary:match(latest_options_file(Dir),
                                        <<"use_direct_io_for_compaction_reads=true">>)),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

latest_options_file(Dir) ->
  [Latest | _] = lists:reverse(lists:sort(filelib:wildcard(filename:join(Dir, "OPTIONS-*")))),
  {ok, Bin} = file:read_file(Latest),
  Bin.

%% DB options added in RocksDB 11.1
db_options_test() ->
  Dir = "erocksdb.db_opts_11.test",
  ?rm_rf(Dir),
  %% open_files_async requires skip_stats_update_on_db_open = true
  Opts = [{create_if_missing, true},
          {open_files_async, true},
          {skip_stats_update_on_db_open, true},
          {enforce_write_buffer_manager_during_recovery, true},
          {verify_manifest_content_on_close, true}],
  {ok, Ref} = rocksdb:open(Dir, Opts),
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  ok = rocksdb:close(Ref),

  %% reopen exercises the close-time manifest verification + async open path
  {ok, Ref2} = rocksdb:open(Dir, Opts),
  {ok, <<"value1">>} = rocksdb:get(Ref2, <<"key1">>, []),
  ok = rocksdb:close(Ref2),

  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

%% CF option memtable_batch_lookup_optimization (RocksDB 11.1)
memtable_batch_lookup_optimization_test() ->
  Dir = "erocksdb.memtable_batch.test",
  ?rm_rf(Dir),
  {ok, Ref} = rocksdb:open(Dir, [{create_if_missing, true},
                                 {memtable_batch_lookup_optimization, true}]),
  ok = rocksdb:put(Ref, <<"a">>, <<"1">>, []),
  ok = rocksdb:put(Ref, <<"b">>, <<"2">>, []),
  ok = rocksdb:put(Ref, <<"c">>, <<"3">>, []),
  [{ok, <<"1">>}, {ok, <<"2">>}, {ok, <<"3">>}] =
    rocksdb:multi_get(Ref, [<<"a">>, <<"b">>, <<"c">>], []),
  ok = rocksdb:close(Ref),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

%% Block-based table options added in RocksDB 11.0/11.1
table_options_test() ->
  Dir = "erocksdb.table_opts_11.test",
  ?rm_rf(Dir),
  BBT = [{index_block_search_type, auto},
         {uniform_cv_threshold, 0.2},
         {prepopulate_block_cache, flush_and_compaction}],
  {ok, Ref} = rocksdb:open(Dir, [{create_if_missing, true},
                                 {block_based_table_options, BBT}]),
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 100)],
  %% flush so blocks go through the table builder (prepopulate path)
  ok = rocksdb:flush(Ref, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value100">>} = rocksdb:get(Ref, <<"key100">>, []),
  ok = rocksdb:close(Ref),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.

%% index_block_search_type=interpolation
interpolation_search_test() ->
  Dir = "erocksdb.interpolation.test",
  ?rm_rf(Dir),
  BBT = [{index_block_search_type, interpolation}],
  {ok, Ref} = rocksdb:open(Dir, [{create_if_missing, true},
                                 {block_based_table_options, BBT}]),
  ok = rocksdb:put(Ref, <<"k">>, <<"v">>, []),
  ok = rocksdb:flush(Ref, []),
  {ok, <<"v">>} = rocksdb:get(Ref, <<"k">>, []),
  ok = rocksdb:close(Ref),
  rocksdb:destroy(Dir, []),
  ?rm_rf(Dir),
  ok.
