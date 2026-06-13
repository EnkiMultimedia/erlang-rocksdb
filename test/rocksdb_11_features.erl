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
