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
-module(sst_file_reader).

-include_lib("eunit/include/eunit.hrl").

-define(SST_FILE, "/tmp/rocksdb_reader_test.sst").

basic_read_test() ->
    %% Cleanup any previous test files
    file:delete(?SST_FILE),

    %% Create SST file with some data
    {ok, Writer} = rocksdb:sst_file_writer_open([], ?SST_FILE),
    ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value_a">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"b">>, <<"value_b">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"c">>, <<"value_c">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Verify SST file was created
    ?assert(filelib:is_file(?SST_FILE)),

    %% Open the SST file for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], ?SST_FILE),

    %% Create iterator and read all entries
    {ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),

    %% Move to first entry
    {ok, <<"a">>, <<"value_a">>} = rocksdb:sst_file_reader_iterator_move(Itr, first),
    {ok, <<"b">>, <<"value_b">>} = rocksdb:sst_file_reader_iterator_move(Itr, next),
    {ok, <<"c">>, <<"value_c">>} = rocksdb:sst_file_reader_iterator_move(Itr, next),
    {error, invalid_iterator} = rocksdb:sst_file_reader_iterator_move(Itr, next),

    %% Close iterator and reader
    ok = rocksdb:sst_file_reader_iterator_close(Itr),
    ok = rocksdb:release_sst_file_reader(Reader),

    %% Cleanup
    file:delete(?SST_FILE),
    ok.

iterator_seek_test() ->
    %% Cleanup
    SstFile = "/tmp/rocksdb_reader_seek_test.sst",
    file:delete(SstFile),

    %% Create SST file with data
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key1">>, <<"val1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key2">>, <<"val2">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key3">>, <<"val3">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key4">>, <<"val4">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"key5">>, <<"val5">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),
    {ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),

    %% Test seek
    {ok, <<"key3">>, <<"val3">>} = rocksdb:sst_file_reader_iterator_move(Itr, {seek, <<"key3">>}),

    %% Test seek to non-existent key (should go to next)
    {ok, <<"key4">>, <<"val4">>} = rocksdb:sst_file_reader_iterator_move(Itr, {seek, <<"key35">>}),

    %% Test seek_for_prev
    {ok, <<"key3">>, <<"val3">>} = rocksdb:sst_file_reader_iterator_move(Itr, {seek_for_prev, <<"key35">>}),

    %% Test last
    {ok, <<"key5">>, <<"val5">>} = rocksdb:sst_file_reader_iterator_move(Itr, last),

    %% Test prev
    {ok, <<"key4">>, <<"val4">>} = rocksdb:sst_file_reader_iterator_move(Itr, prev),

    %% Cleanup
    ok = rocksdb:sst_file_reader_iterator_close(Itr),
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(SstFile),
    ok.

table_properties_test() ->
    %% Cleanup
    SstFile = "/tmp/rocksdb_reader_props_test.sst",
    file:delete(SstFile),

    %% Create SST file with data
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"prop_key1">>, <<"prop_val1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"prop_key2">>, <<"prop_val2">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"prop_key3">>, <<"prop_val3">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),

    %% Get table properties
    {ok, Props} = rocksdb:sst_file_reader_get_table_properties(Reader),

    %% Verify some expected properties
    ?assertEqual(3, maps:get(num_entries, Props)),
    ?assert(maps:get(data_size, Props) > 0),
    ?assert(is_binary(maps:get(compression_name, Props))),
    ?assert(maps:get(creation_time, Props) > 0 orelse maps:get(creation_time, Props) =:= 0),

    %% Cleanup
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(SstFile),
    ok.

verify_checksum_test() ->
    %% Cleanup
    SstFile = "/tmp/rocksdb_reader_checksum_test.sst",
    file:delete(SstFile),

    %% Create SST file with data
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"chk_key1">>, <<"chk_val1">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"chk_key2">>, <<"chk_val2">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),

    %% Verify checksums (should pass for valid file)
    ok = rocksdb:sst_file_reader_verify_checksum(Reader),

    %% Also test with options
    ok = rocksdb:sst_file_reader_verify_checksum(Reader, []),

    %% Cleanup
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(SstFile),
    ok.

binary_path_test() ->
    %% Test with binary path
    SstFile = <<"/tmp/rocksdb_reader_binary_path.sst">>,
    file:delete(binary_to_list(SstFile)),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"bin_key">>, <<"bin_val">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open using binary path
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),

    %% Read data
    {ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),
    {ok, <<"bin_key">>, <<"bin_val">>} = rocksdb:sst_file_reader_iterator_move(Itr, first),

    %% Cleanup
    ok = rocksdb:sst_file_reader_iterator_close(Itr),
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(binary_to_list(SstFile)),
    ok.

nonexistent_file_test() ->
    %% Try to open a non-existent file
    {error, _Reason} = rocksdb:sst_file_reader_open([], "/tmp/nonexistent_sst_file.sst"),
    ok.

multiple_iterators_test() ->
    %% Test that we can create multiple iterators from the same reader
    SstFile = "/tmp/rocksdb_reader_multi_itr.sst",
    file:delete(SstFile),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"multi_a">>, <<"val_a">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"multi_b">>, <<"val_b">>),
    ok = rocksdb:sst_file_writer_put(Writer, <<"multi_c">>, <<"val_c">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),

    %% Create two iterators
    {ok, Itr1} = rocksdb:sst_file_reader_iterator(Reader, []),
    {ok, Itr2} = rocksdb:sst_file_reader_iterator(Reader, []),

    %% Position them differently
    {ok, <<"multi_a">>, <<"val_a">>} = rocksdb:sst_file_reader_iterator_move(Itr1, first),
    {ok, <<"multi_c">>, <<"val_c">>} = rocksdb:sst_file_reader_iterator_move(Itr2, last),

    %% Move them independently
    {ok, <<"multi_b">>, <<"val_b">>} = rocksdb:sst_file_reader_iterator_move(Itr1, next),
    {ok, <<"multi_b">>, <<"val_b">>} = rocksdb:sst_file_reader_iterator_move(Itr2, prev),

    %% Cleanup
    ok = rocksdb:sst_file_reader_iterator_close(Itr1),
    ok = rocksdb:sst_file_reader_iterator_close(Itr2),
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(SstFile),
    ok.

iterator_with_verify_checksums_test() ->
    %% Test iterator with verify_checksums option
    SstFile = "/tmp/rocksdb_reader_verify_itr.sst",
    file:delete(SstFile),

    %% Create SST file
    {ok, Writer} = rocksdb:sst_file_writer_open([], SstFile),
    ok = rocksdb:sst_file_writer_put(Writer, <<"verify_key">>, <<"verify_val">>),
    ok = rocksdb:sst_file_writer_finish(Writer),
    ok = rocksdb:release_sst_file_writer(Writer),

    %% Open for reading
    {ok, Reader} = rocksdb:sst_file_reader_open([], SstFile),

    %% Create iterator with verify_checksums enabled
    {ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, [{verify_checksums, true}]),
    {ok, <<"verify_key">>, <<"verify_val">>} = rocksdb:sst_file_reader_iterator_move(Itr, first),

    %% Cleanup
    ok = rocksdb:sst_file_reader_iterator_close(Itr),
    ok = rocksdb:release_sst_file_reader(Reader),
    file:delete(SstFile),
    ok.
