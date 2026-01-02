%%% -*- erlang -*-
%%
%% Copyright (c) 2018-2025 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
-module(sst_file_writer).

-include_lib("eunit/include/eunit.hrl").

-define(SST_FILE, "/tmp/rocksdb_sst_writer_test.sst").
-define(TEST_DB, "rocksdb_sst_writer_test.db").

basic_write_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file writer
    Options = [{create_if_missing, true}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, ?SST_FILE),

    %% Add key-value pairs (must be in sorted order)
    ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"b">>, <<"value_b">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"c">>, <<"value_c">>),

    %% Finish writing and get file info
    {ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

    %% File size should be non-zero after finishing
    FileSize = maps:get(file_size, FileInfo),
    ?assert(FileSize > 0),

    %% Verify file was created
    ?assert(filelib:is_file(?SST_FILE)),

    %% Release the writer
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

finish_with_file_info_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file writer
    Options = [],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, ?SST_FILE),

    %% Add some key-value pairs
    ok = rocksdb:sst_file_writer_put(Writer, <<"key1">>, <<"value1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key2">>, <<"value2">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key3">>, <<"value3">>),

    %% Finish with file info
    {ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

    %% Verify file info structure
    ?assert(is_map(FileInfo)),
    ?assertEqual(list_to_binary(?SST_FILE), maps:get(file_path, FileInfo)),
    ?assertEqual(<<"key1">>, maps:get(smallest_key, FileInfo)),
    ?assertEqual(<<"key3">>, maps:get(largest_key, FileInfo)),
    ?assertEqual(3, maps:get(num_entries, FileInfo)),
    ?assert(maps:get(file_size, FileInfo) > 0),

    %% Release the writer
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

delete_operation_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file writer
    {ok, Writer} = rocksdb:sst_file_writer_open([], ?SST_FILE),

    %% Add key-value pairs with delete tombstones
    ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),
    ok = rocksdb:sst_file_writer_delete(Writer, <<"b">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"c">>, <<"value_c">>),

    %% Finish writing
    {ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

    %% Should have 3 entries (2 puts + 1 delete)
    ?assertEqual(3, maps:get(num_entries, FileInfo)),

    %% Release the writer
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

delete_range_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file writer
    {ok, Writer} = rocksdb:sst_file_writer_open([], ?SST_FILE),

    %% Add key-value pairs and range delete
    ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"z">>, <<"value_z">>),
    %% Range delete can be added in any order
    ok = rocksdb:sst_file_writer_delete_range(Writer, <<"b">>, <<"y">>),

    %% Finish writing
    {ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

    %% Should have range delete entries
    ?assertEqual(1, maps:get(num_range_del_entries, FileInfo)),

    %% Release the writer
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

unsorted_keys_error_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file writer
    {ok, Writer} = rocksdb:sst_file_writer_open([], ?SST_FILE),

    %% Add key in correct order
    ok = rocksdb:sst_file_writer_put(Writer, <<"b">>, <<"value_b">>),

    %% Adding key before previous should fail
    {error, _Reason} = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),

    %% Release the writer (cleanup partial file)
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

binary_path_test() ->
    %% Test with binary path
    SstFile = <<"/tmp/rocksdb_sst_binary_path_test.sst">>,
    file:delete(binary_to_list(SstFile)),

    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key">>, <<"value">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Verify file was created
    ?assert(filelib:is_file(binary_to_list(SstFile))),

    %% Cleanup
    file:delete(binary_to_list(SstFile)),
    ok.

merge_operation_test() ->
    %% Test merge operations in SST file
    SstFile = "/tmp/rocksdb_sst_merge_test.sst",
    file:delete(SstFile),

    %% Create SST file with merge operator
    %% counter_merge_operator uses ASCII string format like <<"100">>
    Options = [{merge_operator, counter_merge_operator}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, SstFile),

    %% Add merge operations (keys must be in sorted order)
    ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:a">>, <<"10">>),
    ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:b">>, <<"20">>),
    ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:c">>, <<"30">>),

    %% Finish writing
    {ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

    %% Should have 3 entries
    ?assertEqual(3, maps:get(num_entries, FileInfo)),

    ok = rocksdb:release_sst_file_writer(Writer),

    %% Cleanup
    file:delete(SstFile),
    ok.

counter_merge_ingest_test() ->
    %% Test counter merge operation with ingestion
    %% counter_merge_operator uses ASCII string format: <<"100">> means 100
    SstFile = "/tmp/rocksdb_counter_merge_ingest.sst",
    TestDb = "rocksdb_counter_merge_ingest.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    %% Create SST file with counter merge operations
    Options = [{merge_operator, counter_merge_operator}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, SstFile),

    ok = rocksdb:sst_file_writer_merge(Writer, <<"views:page1">>, <<"100">>),
    ok = rocksdb:sst_file_writer_merge(Writer, <<"views:page2">>, <<"200">>),

    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database with counter merge operator
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, counter_merge_operator}
    ]),

    %% Ingest the SST file
    ok = rocksdb:ingest_external_file(Db, [SstFile], []),

    %% Read counter values (result is ASCII string)
    {ok, <<"100">>} = rocksdb:get(Db, <<"views:page1">>, []),
    {ok, <<"200">>} = rocksdb:get(Db, <<"views:page2">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

counter_merge_accumulate_test() ->
    %% Test that counter merges accumulate across ingestions
    SstFile1 = "/tmp/rocksdb_counter_accum1.sst",
    SstFile2 = "/tmp/rocksdb_counter_accum2.sst",
    TestDb = "rocksdb_counter_accum.db",
    file:delete(SstFile1),
    file:delete(SstFile2),
    os:cmd("rm -rf " ++ TestDb),

    Options = [{merge_operator, counter_merge_operator}],

    %% Create first SST file
    {ok, Writer1} = rocksdb:sst_file_writer_open(Options, SstFile1),
    ok = rocksdb:sst_file_writer_merge(Writer1, <<"counter">>, <<"100">>),
    ok = rocksdb:sst_file_writer_finish(Writer1),
    ok = rocksdb:release_sst_file_writer(Writer1),

    %% Create second SST file
    {ok, Writer2} = rocksdb:sst_file_writer_open(Options, SstFile2),
    ok = rocksdb:sst_file_writer_merge(Writer2, <<"counter">>, <<"50">>),
    ok = rocksdb:sst_file_writer_finish(Writer2),
    ok = rocksdb:release_sst_file_writer(Writer2),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, counter_merge_operator}
    ]),

    %% Ingest first file
    ok = rocksdb:ingest_external_file(Db, [SstFile1], []),
    {ok, <<"100">>} = rocksdb:get(Db, <<"counter">>, []),

    %% Ingest second file - should accumulate
    ok = rocksdb:ingest_external_file(Db, [SstFile2], []),
    {ok, <<"150">>} = rocksdb:get(Db, <<"counter">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile1),
    file:delete(SstFile2),
    ok.

mixed_put_merge_test() ->
    %% Test mixing put and merge operations in SST file
    SstFile = "/tmp/rocksdb_mixed_put_merge.sst",
    TestDb = "rocksdb_mixed_put_merge.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    Options = [{merge_operator, counter_merge_operator}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, SstFile),

    %% Mix puts and merges (must be in sorted order)
    ok = rocksdb:sst_file_writer_put(Writer, <<"a_regular">>, <<"value">>),
    ok = rocksdb:sst_file_writer_merge(Writer, <<"b_counter">>, <<"42">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"c_regular">>, <<"another">>),

    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database and ingest
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, counter_merge_operator}
    ]),
    ok = rocksdb:ingest_external_file(Db, [SstFile], []),

    %% Verify both types of values
    {ok, <<"value">>} = rocksdb:get(Db, <<"a_regular">>, []),
    {ok, <<"42">>} = rocksdb:get(Db, <<"b_counter">>, []),
    {ok, <<"another">>} = rocksdb:get(Db, <<"c_regular">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

erlang_merge_list_test() ->
    %% Test erlang merge operator with list operations
    SstFile = "/tmp/rocksdb_erlang_merge_list.sst",
    TestDb = "rocksdb_erlang_merge_list.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    Options = [{merge_operator, erlang_merge_operator}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, SstFile),

    %% Add list append merge operations
    MergeValue = term_to_binary({list_append, [item1, item2]}),
    ok = rocksdb:sst_file_writer_merge(Writer, <<"mylist">>, MergeValue),

    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database with erlang merge operator
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, erlang_merge_operator}
    ]),

    %% Put initial list
    ok = rocksdb:put(Db, <<"mylist">>, term_to_binary([existing]), []),

    %% Ingest merge operations
    ok = rocksdb:ingest_external_file(Db, [SstFile], []),

    %% Read and verify merged list
    {ok, Bin} = rocksdb:get(Db, <<"mylist">>, []),
    List = binary_to_term(Bin),
    ?assertEqual([existing, item1, item2], List),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

bitset_merge_test() ->
    %% Test bitset merge operator with SST files
    %% bitset_merge_operator uses <<"+N">> to set bit at position N
    %% and <<"-N">> to clear bit at position N
    SstFile = "/tmp/rocksdb_bitset_merge.sst",
    TestDb = "rocksdb_bitset_merge.db",
    file:delete(SstFile),
    os:cmd("rm -rf " ++ TestDb),

    %% bitset_merge_operator requires a size parameter (in bits)
    Options = [{merge_operator, {bitset_merge_operator, 64}}],
    {ok, Writer} = rocksdb:sst_file_writer_open(Options, SstFile),

    %% Set bit at position 2 (format: <<"+N">> where N is ASCII digits)
    ok = rocksdb:sst_file_writer_merge(Writer, <<"flags:user1">>, <<"+2">>),

    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open database with bitset merge operator
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, {bitset_merge_operator, 64}}
    ]),

    %% Put initial value (all zeros, 8 bytes = 64 bits)
    ok = rocksdb:put(Db, <<"flags:user1">>, <<0:64/unsigned>>, []),

    %% Ingest the SST file with bitset operation
    ok = rocksdb:ingest_external_file(Db, [SstFile], []),

    %% Read and verify bit was set
    %% Bit 2 set means value = 32 (0b00100000 in first byte)
    {ok, <<32, _/binary>>} = rocksdb:get(Db, <<"flags:user1">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile),
    ok.

bitset_merge_multiple_bits_test() ->
    %% Test setting multiple bits with bitset merge operator
    SstFile1 = "/tmp/rocksdb_bitset_multi1.sst",
    SstFile2 = "/tmp/rocksdb_bitset_multi2.sst",
    TestDb = "rocksdb_bitset_multi.db",
    file:delete(SstFile1),
    file:delete(SstFile2),
    os:cmd("rm -rf " ++ TestDb),

    Options = [{merge_operator, {bitset_merge_operator, 64}}],

    %% Create first SST file - set bit at position 2
    {ok, Writer1} = rocksdb:sst_file_writer_open(Options, SstFile1),
    ok = rocksdb:sst_file_writer_merge(Writer1, <<"bits">>, <<"+2">>),
    ok = rocksdb:sst_file_writer_finish(Writer1),
    ok = rocksdb:release_sst_file_writer(Writer1),

    %% Create second SST file - set bit at position 11
    {ok, Writer2} = rocksdb:sst_file_writer_open(Options, SstFile2),
    ok = rocksdb:sst_file_writer_merge(Writer2, <<"bits">>, <<"+11">>),
    ok = rocksdb:sst_file_writer_finish(Writer2),
    ok = rocksdb:release_sst_file_writer(Writer2),

    %% Open database
    {ok, Db} = rocksdb:open(TestDb, [
        {create_if_missing, true},
        {merge_operator, {bitset_merge_operator, 64}}
    ]),

    %% Put initial value (all zeros, 8 bytes = 64 bits)
    ok = rocksdb:put(Db, <<"bits">>, <<0:64/unsigned>>, []),

    %% Ingest first file - sets bit 2 (value becomes 32 = 0b00100000)
    ok = rocksdb:ingest_external_file(Db, [SstFile1], []),
    {ok, <<32, _/binary>>} = rocksdb:get(Db, <<"bits">>, []),

    %% Ingest second file - sets bit 11 (second byte gets 16 = 0b00010000)
    %% Result: first byte = 32, second byte = 16
    ok = rocksdb:ingest_external_file(Db, [SstFile2], []),
    {ok, <<32, 16, _/binary>>} = rocksdb:get(Db, <<"bits">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    rocksdb:destroy(TestDb, []),
    file:delete(SstFile1),
    file:delete(SstFile2),
    ok.
