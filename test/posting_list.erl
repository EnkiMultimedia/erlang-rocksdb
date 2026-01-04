%% Copyright (c) 2016-2026 Benoit Chesneau
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

-module(posting_list).

-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Basic Tests
%% ===================================================================

posting_list_add_test() ->
    DbPath = "posting_list_add.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Add keys to posting list
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    %% Get and verify
    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    Keys = rocksdb:posting_list_keys(Bin),
    ?assert(lists:member(<<"doc1">>, Keys)),
    ?assert(lists:member(<<"doc2">>, Keys)),
    ?assert(lists:member(<<"doc3">>, Keys)),
    ?assertEqual(3, rocksdb:posting_list_count(Bin)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_delete_test() ->
    DbPath = "posting_list_delete.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Add keys
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    %% Delete doc2
    ok = rocksdb:merge(Db, <<"term">>, {posting_delete, <<"doc2">>}, []),

    %% Get and verify
    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    Keys = rocksdb:posting_list_keys(Bin),
    ?assert(lists:member(<<"doc1">>, Keys)),
    ?assertNot(lists:member(<<"doc2">>, Keys)),  % Deleted
    ?assert(lists:member(<<"doc3">>, Keys)),
    ?assertEqual(2, rocksdb:posting_list_count(Bin)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_compaction_test() ->
    %% Test that merge operator cleans up tombstones during merge
    %% Tombstones are removed during reads (FullMergeV2) and compaction (PartialMergeMulti)
    DbPath = "posting_list_tombstones.test",
    rocksdb_test_util:rm_rf(DbPath),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create first SST file with base value
    ok = rocksdb:put(Db, <<"term">>, <<>>, []),
    lists:foreach(fun(N) ->
        PadKey = iolist_to_binary(["padding_a", integer_to_list(N)]),
        PadValue = binary:copy(<<"x">>, 1000),
        ok = rocksdb:put(Db, PadKey, PadValue, [])
    end, lists:seq(1, 50)),
    ok = rocksdb:flush(Db, []),

    %% Create second SST file with merge operands
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_delete, <<"doc1">>}, []),
    lists:foreach(fun(N) ->
        PadKey = iolist_to_binary(["padding_b", integer_to_list(N)]),
        PadValue = binary:copy(<<"y">>, 1000),
        ok = rocksdb:put(Db, PadKey, PadValue, [])
    end, lists:seq(1, 50)),
    ok = rocksdb:flush(Db, []),

    %% During read, the merge operator cleans up tombstones (FullMergeV2)
    %% Only doc2 should be present (doc1 was added then deleted)
    {ok, Bin1} = rocksdb:get(Db, <<"term">>, []),
    Entries1 = rocksdb:posting_list_decode(Bin1),
    ?assertEqual([{<<"doc2">>, false}], Entries1),

    %% Force compaction - PartialMergeMulti consolidates operands
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% After compaction, result should still be only doc2
    {ok, Bin2} = rocksdb:get(Db, <<"term">>, []),
    Entries2 = rocksdb:posting_list_decode(Bin2),
    ?assertEqual([{<<"doc2">>, false}], Entries2),

    ok = rocksdb:close(Db),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Helper Function Tests
%% ===================================================================

posting_list_decode_test() ->
    %% Build a posting list binary manually
    Bin = <<3:32/big, 0, "foo", 3:32/big, 1, "bar", 3:32/big, 0, "baz">>,
    Entries = rocksdb:posting_list_decode(Bin),
    ?assertEqual([{<<"foo">>, false}, {<<"bar">>, true}, {<<"baz">>, false}], Entries).

posting_list_fold_test() ->
    Bin = <<3:32/big, 0, "foo", 3:32/big, 1, "bar", 3:32/big, 0, "baz">>,
    Count = rocksdb:posting_list_fold(
        fun(_Key, _IsTombstone, Acc) -> Acc + 1 end,
        0,
        Bin
    ),
    ?assertEqual(3, Count).

posting_list_keys_test() ->
    %% foo (normal), bar (tombstone), baz (normal)
    Bin = <<3:32/big, 0, "foo", 3:32/big, 1, "bar", 3:32/big, 0, "baz">>,
    Keys = rocksdb:posting_list_keys(Bin),
    ?assert(lists:member(<<"foo">>, Keys)),
    ?assertNot(lists:member(<<"bar">>, Keys)),  % Tombstoned
    ?assert(lists:member(<<"baz">>, Keys)),
    ?assertEqual(2, length(Keys)).

posting_list_contains_test() ->
    %% foo (normal), bar (normal), bar (tombstone) - bar is tombstoned
    Bin = <<3:32/big, 0, "foo", 3:32/big, 0, "bar", 3:32/big, 1, "bar">>,
    ?assertEqual(true, rocksdb:posting_list_contains(Bin, <<"foo">>)),
    ?assertEqual(false, rocksdb:posting_list_contains(Bin, <<"bar">>)),  % Tombstoned
    ?assertEqual(false, rocksdb:posting_list_contains(Bin, <<"baz">>)).  % Not found

posting_list_find_test() ->
    %% foo (normal), bar (normal), bar (tombstone) - bar is tombstoned
    Bin = <<3:32/big, 0, "foo", 3:32/big, 0, "bar", 3:32/big, 1, "bar">>,
    ?assertEqual({ok, false}, rocksdb:posting_list_find(Bin, <<"foo">>)),
    ?assertEqual({ok, true}, rocksdb:posting_list_find(Bin, <<"bar">>)),  % Tombstoned
    ?assertEqual(not_found, rocksdb:posting_list_find(Bin, <<"baz">>)).

posting_list_count_test() ->
    Bin = <<3:32/big, 0, "foo", 3:32/big, 1, "bar", 3:32/big, 0, "baz">>,
    ?assertEqual(2, rocksdb:posting_list_count(Bin)).  % bar is tombstoned

posting_list_to_map_test() ->
    Bin = <<3:32/big, 0, "foo", 3:32/big, 1, "bar", 3:32/big, 0, "baz">>,
    Map = rocksdb:posting_list_to_map(Bin),
    ?assertEqual(active, maps:get(<<"foo">>, Map)),
    ?assertEqual(tombstone, maps:get(<<"bar">>, Map)),
    ?assertEqual(active, maps:get(<<"baz">>, Map)).

posting_list_empty_test() ->
    Bin = <<>>,
    ?assertEqual([], rocksdb:posting_list_decode(Bin)),
    ?assertEqual([], rocksdb:posting_list_keys(Bin)),
    ?assertEqual(0, rocksdb:posting_list_count(Bin)),
    ?assertEqual(#{}, rocksdb:posting_list_to_map(Bin)).

%% ===================================================================
%% Duplicate Keys Tests
%% ===================================================================

posting_list_duplicate_keys_test() ->
    %% Add same key multiple times, last occurrence wins
    %% doc1 (add), doc1 (delete), doc1 (add) -> doc1 is active
    Bin = <<4:32/big, 0, "doc1", 4:32/big, 1, "doc1", 4:32/big, 0, "doc1">>,
    ?assertEqual(true, rocksdb:posting_list_contains(Bin, <<"doc1">>)),
    ?assertEqual({ok, false}, rocksdb:posting_list_find(Bin, <<"doc1">>)),
    ?assertEqual(1, rocksdb:posting_list_count(Bin)).

posting_list_duplicate_keys_tombstoned_test() ->
    %% doc1 (add), doc1 (add), doc1 (delete) -> doc1 is tombstoned
    Bin = <<4:32/big, 0, "doc1", 4:32/big, 0, "doc1", 4:32/big, 1, "doc1">>,
    ?assertEqual(false, rocksdb:posting_list_contains(Bin, <<"doc1">>)),
    ?assertEqual({ok, true}, rocksdb:posting_list_find(Bin, <<"doc1">>)),
    ?assertEqual(0, rocksdb:posting_list_count(Bin)).
