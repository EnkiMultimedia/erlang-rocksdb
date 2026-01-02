%%% -*- erlang -*-
%%%
%%% Copyright (c) 2018-2025 Benoit Chesneau
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
-module(ingest_external_file).

-include_lib("eunit/include/eunit.hrl").

-define(SST_FILE, "/tmp/rocksdb_ingest_test.sst").
-define(TEST_DB, "rocksdb_ingest_test.db").

basic_ingest_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),
    os:cmd("rm -rf " ++ ?TEST_DB),

    %% Create SST file with some data
    {ok, Writer} = rocksdb:sst_file_writer_open([], ?SST_FILE),
    ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"b">>, <<"value_b">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"c">>, <<"value_c">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Verify SST file was created
    ?assert(filelib:is_file(?SST_FILE)),

    %% Open database
    {ok, Db} = rocksdb:open(?TEST_DB, [{create_if_missing, true}]),

    %% Ingest the SST file
    ok = rocksdb:ingest_external_file(Db, [?SST_FILE], []),

    %% Verify data was ingested
    {ok, <<"value_a">>} = rocksdb:get(Db, <<"a">>, []),
    {ok, <<"value_b">>} = rocksdb:get(Db, <<"b">>, []),
    {ok, <<"value_c">>} = rocksdb:get(Db, <<"c">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(?TEST_DB, []),
    file:delete(?SST_FILE),
    ok.

ingest_with_move_test() ->
    %% Cleanup any previous test files
    SstFile = "/tmp/rocksdb_ingest_move_test.sst",
    TestDb = "rocksdb_ingest_move_test.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key1">>, <<"val1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key2">>, <<"val2">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [{create_if_missing, true}]),

    %% Ingest with move_files option
    ok = rocksdb:ingest_external_file(Db, [SstFile], [{move_files, true}]),

    %% Verify data was ingested
    {ok, <<"val1">>} = rocksdb:get(Db, <<"key1">>, []),
    {ok, <<"val2">>} = rocksdb:get(Db, <<"key2">>, []),

    %% The file should be moved (no longer exists at original location)
    %% Note: This depends on filesystem support; if move fails, it will copy

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

ingest_multiple_files_test() ->
    %% Cleanup
    SstFile1 = "/tmp/rocksdb_ingest_multi1.sst",
    SstFile2 = "/tmp/rocksdb_ingest_multi2.sst",
    TestDb = "rocksdb_ingest_multi_test.db",
    file:delete(SstFile1),
    file:delete(SstFile2),
    os:cmd("rm -rf " ++ TestDb),

    %% Create first SST file with keys a-c
    {ok, Writer1} = rocksdb:sst_file_writer_open([], SstFile1),
    ok = rocksdb:sst_file_writer_put(Writer1, <<"a">>, <<"1">>),
    ok = rocksdb:sst_file_writer_put(Writer1, <<"b">>, <<"2">>),
    ok = rocksdb:sst_file_writer_put(Writer1, <<"c">>, <<"3">>),
    ok = rocksdb:sst_file_writer_finish(Writer1),
    ok = rocksdb:release_sst_file_writer(Writer1),

    %% Create second SST file with keys x-z (non-overlapping)
    {ok, Writer2} = rocksdb:sst_file_writer_open([], SstFile2),
    ok = rocksdb:sst_file_writer_put(Writer2, <<"x">>, <<"24">>),
    ok = rocksdb:sst_file_writer_put(Writer2, <<"y">>, <<"25">>),
    ok = rocksdb:sst_file_writer_put(Writer2, <<"z">>, <<"26">>),
    ok = rocksdb:sst_file_writer_finish(Writer2),
    ok = rocksdb:release_sst_file_writer(Writer2),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [{create_if_missing, true}]),

    %% Ingest both files at once
    ok = rocksdb:ingest_external_file(Db, [SstFile1, SstFile2], []),

    %% Verify all data was ingested
    {ok, <<"1">>} = rocksdb:get(Db, <<"a">>, []),
    {ok, <<"2">>} = rocksdb:get(Db, <<"b">>, []),
    {ok, <<"3">>} = rocksdb:get(Db, <<"c">>, []),
    {ok, <<"24">>} = rocksdb:get(Db, <<"x">>, []),
    {ok, <<"25">>} = rocksdb:get(Db, <<"y">>, []),
    {ok, <<"26">>} = rocksdb:get(Db, <<"z">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile1),
    file:delete(SstFile2),
    ok.

ingest_with_column_family_test() ->
    %% Cleanup
    SstFile = "/tmp/rocksdb_ingest_cf.sst",
    TestDb = "rocksdb_ingest_cf_test.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"cf_key1">>, <<"cf_val1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"cf_key2">>, <<"cf_val2">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database with column families
    {ok, Db, [_DefaultCf, TestCf]} = rocksdb:open_with_cf(
        TestDb,
        [{create_if_missing, true}, {create_missing_column_families, true}],
        [{"default", []}, {"test_cf", []}]
    ),

    %% Ingest into specific column family
    ok = rocksdb:ingest_external_file(Db, TestCf, [SstFile], []),

    %% Verify data is in the column family
    {ok, <<"cf_val1">>} = rocksdb:get(Db, TestCf, <<"cf_key1">>, []),
    {ok, <<"cf_val2">>} = rocksdb:get(Db, TestCf, <<"cf_key2">>, []),

    %% Verify data is NOT in default column family
    not_found = rocksdb:get(Db, <<"cf_key1">>, []),
    not_found = rocksdb:get(Db, <<"cf_key2">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

ingest_with_options_test() ->
    %% Cleanup
    SstFile = "/tmp/rocksdb_ingest_opts.sst",
    TestDb = "rocksdb_ingest_opts_test.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"opt_key">>, <<"opt_val">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [{create_if_missing, true}]),

    %% Ingest with various options
    IngestOpts = [
        {move_files, false},
        {snapshot_consistency, true},
        {allow_global_seqno, true},
        {allow_blocking_flush, true},
        {verify_checksums_before_ingest, true},
        {verify_file_checksum, true},
        {fill_cache, true}
    ],
    ok = rocksdb:ingest_external_file(Db, [SstFile], IngestOpts),

    %% Verify data
    {ok, <<"opt_val">>} = rocksdb:get(Db, <<"opt_key">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

ingest_binary_path_test() ->
    %% Test with binary path
    SstFile = <<"/tmp/rocksdb_ingest_binary_path.sst">>,
    TestDb = "rocksdb_ingest_binary_path_test.db",
    file:delete(binary_to_list(SstFile)),
    os:cmd("rm -rf " ++ TestDb),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"bin_key">>, <<"bin_val">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [{create_if_missing, true}]),

    %% Ingest using binary path
    ok = rocksdb:ingest_external_file(Db, [SstFile], []),

    %% Verify data
    {ok, <<"bin_val">>} = rocksdb:get(Db, <<"bin_key">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(binary_to_list(SstFile)),
    ok.
