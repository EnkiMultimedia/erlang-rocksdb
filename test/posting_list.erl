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

posting_list_batch_merge_test() ->
    DbPath = "posting_list_batch_merge.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create a batch with posting list operations
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_merge(Batch, <<"term1">>, {posting_add, <<"doc1">>}),
    ok = rocksdb:batch_merge(Batch, <<"term1">>, {posting_add, <<"doc2">>}),
    ok = rocksdb:batch_merge(Batch, <<"term1">>, {posting_add, <<"doc3">>}),
    ok = rocksdb:batch_merge(Batch, <<"term2">>, {posting_add, <<"doc1">>}),
    ok = rocksdb:batch_merge(Batch, <<"term2">>, {posting_add, <<"doc2">>}),
    %% Delete doc2 from term1 in the same batch
    ok = rocksdb:batch_merge(Batch, <<"term1">>, {posting_delete, <<"doc2">>}),

    %% Write the batch
    ok = rocksdb:write_batch(Db, Batch, []),
    ok = rocksdb:release_batch(Batch),

    %% Verify term1: should have doc1 and doc3 (doc2 was deleted)
    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    Keys1 = rocksdb:posting_list_keys(Bin1),
    ?assert(lists:member(<<"doc1">>, Keys1)),
    ?assertNot(lists:member(<<"doc2">>, Keys1)),
    ?assert(lists:member(<<"doc3">>, Keys1)),
    ?assertEqual(2, rocksdb:posting_list_count(Bin1)),

    %% Verify term2: should have doc1 and doc2
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),
    Keys2 = rocksdb:posting_list_keys(Bin2),
    ?assert(lists:member(<<"doc1">>, Keys2)),
    ?assert(lists:member(<<"doc2">>, Keys2)),
    ?assertEqual(2, rocksdb:posting_list_count(Bin2)),

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
    Keys1 = rocksdb:posting_list_keys(Bin1),
    ?assertEqual([<<"doc2">>], Keys1),

    %% Force compaction - PartialMergeMulti consolidates operands
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% After compaction, result should still be only doc2
    {ok, Bin2} = rocksdb:get(Db, <<"term">>, []),
    Keys2 = rocksdb:posting_list_keys(Bin2),
    ?assertEqual([<<"doc2">>], Keys2),

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

%% ===================================================================
%% V2 Format Tests
%% ===================================================================

posting_list_v2_format_test() ->
    DbPath = "posting_list_v2_format.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Add keys to posting list
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),

    %% Verify V2 format
    ?assertEqual(2, rocksdb:posting_list_version(Bin)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_sorted_order_test() ->
    DbPath = "posting_list_sorted.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Add keys in random order
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"zebra">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"apple">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"mango">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    Keys = rocksdb:posting_list_keys(Bin),

    %% Keys should be returned in lexicographic order
    ?assertEqual([<<"apple">>, <<"mango">>, <<"zebra">>], Keys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_version_test() ->
    %% V1 format (legacy)
    V1Bin = <<4:32/big, 0, "doc1", 4:32/big, 0, "doc2">>,
    ?assertEqual(1, rocksdb:posting_list_version(V1Bin)),

    %% V2 format starts with version byte 0x02
    V2Bin = <<2, 0, 0, 0, 0, 0, 0, 0, 0>>,  % Empty V2
    ?assertEqual(2, rocksdb:posting_list_version(V2Bin)).

%% ===================================================================
%% Backward Compatibility Tests
%% ===================================================================

posting_list_v1_compat_test() ->
    %% Create V1 format binary manually
    V1Bin = <<4:32/big, 0, "doc1", 4:32/big, 0, "doc2">>,

    %% Should work with all existing functions
    ?assertEqual(1, rocksdb:posting_list_version(V1Bin)),
    Keys = rocksdb:posting_list_keys(V1Bin),
    ?assert(lists:member(<<"doc1">>, Keys)),
    ?assert(lists:member(<<"doc2">>, Keys)),
    ?assertEqual(2, rocksdb:posting_list_count(V1Bin)),
    ?assertEqual(true, rocksdb:posting_list_contains(V1Bin, <<"doc1">>)).

posting_list_v1_to_v2_migration_test() ->
    DbPath = "posting_list_migration.test",
    rocksdb_test_util:rm_rf(DbPath),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Put V1 format directly
    V1Bin = <<4:32/big, 0, "doc1", 4:32/big, 0, "doc2">>,
    ok = rocksdb:put(Db, <<"term">>, V1Bin, []),

    %% Add new key via merge (triggers conversion to V2)
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    %% Verify conversion happened
    {ok, ResultBin} = rocksdb:get(Db, <<"term">>, []),
    ?assertEqual(2, rocksdb:posting_list_version(ResultBin)),

    %% Verify all keys present and sorted
    Keys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Keys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Set Operation Tests
%% ===================================================================

posting_list_intersection_test() ->
    DbPath = "posting_list_intersection.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create two posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc4">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test intersection
    ResultBin = rocksdb:posting_list_intersection(Bin1, Bin2),
    ResultKeys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc2">>, <<"doc3">>], ResultKeys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_union_test() ->
    DbPath = "posting_list_union.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create two posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test union
    ResultBin = rocksdb:posting_list_union(Bin1, Bin2),
    ResultKeys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], ResultKeys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_difference_test() ->
    DbPath = "posting_list_difference.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create two posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test difference (Bin1 - Bin2)
    ResultBin = rocksdb:posting_list_difference(Bin1, Bin2),
    ResultKeys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc1">>, <<"doc3">>], ResultKeys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_intersection_count_test() ->
    DbPath = "posting_list_int_count.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create two posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc4">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test intersection count
    ?assertEqual(2, rocksdb:posting_list_intersection_count(Bin1, Bin2)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_intersect_all_test() ->
    DbPath = "posting_list_int_all.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create three posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc4">>}, []),

    ok = rocksdb:merge(Db, <<"term3">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term3">>, {posting_add, <<"doc4">>}, []),
    ok = rocksdb:merge(Db, <<"term3">>, {posting_add, <<"doc5">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),
    {ok, Bin3} = rocksdb:get(Db, <<"term3">>, []),

    %% Test intersect_all
    ResultBin = rocksdb:posting_list_intersect_all([Bin1, Bin2, Bin3]),
    ResultKeys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc3">>], ResultKeys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_empty_intersection_test() ->
    DbPath = "posting_list_empty_int.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create two disjoint posting lists
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test empty intersection
    ResultBin = rocksdb:posting_list_intersection(Bin1, Bin2),
    ?assertEqual([], rocksdb:posting_list_keys(ResultBin)),
    ?assertEqual(0, rocksdb:posting_list_count(ResultBin)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_self_intersection_test() ->
    DbPath = "posting_list_self_int.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),

    %% A AND A = A
    ResultBin = rocksdb:posting_list_intersection(Bin, Bin),
    ResultKeys = rocksdb:posting_list_keys(ResultBin),
    ?assertEqual([<<"doc1">>, <<"doc2">>], ResultKeys),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Bitmap Tests
%% ===================================================================

posting_list_bitmap_contains_test() ->
    DbPath = "posting_list_bitmap.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),

    %% Test bitmap contains
    ?assertEqual(true, rocksdb:posting_list_bitmap_contains(Bin, <<"doc1">>)),
    ?assertEqual(true, rocksdb:posting_list_bitmap_contains(Bin, <<"doc2">>)),
    ?assertEqual(false, rocksdb:posting_list_bitmap_contains(Bin, <<"doc3">>)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Scale Tests
%% ===================================================================

posting_list_scale_10k_test_() ->
    {timeout, 60, fun() ->
        DbPath = "posting_list_scale.test",
        rocksdb_test_util:rm_rf(DbPath),
        {ok, Db} = rocksdb:open(DbPath, [
            {create_if_missing, true},
            {merge_operator, posting_list_merge_operator}
        ]),

        %% Add 10K keys
        N = 10000,
        lists:foreach(fun(I) ->
            DocId = iolist_to_binary(["doc", integer_to_list(I)]),
            ok = rocksdb:merge(Db, <<"term">>, {posting_add, DocId}, [])
        end, lists:seq(1, N)),

        {ok, Bin} = rocksdb:get(Db, <<"term">>, []),

        ?assertEqual(N, rocksdb:posting_list_count(Bin)),
        ?assertEqual(2, rocksdb:posting_list_version(Bin)),

        %% Test fast contains
        ?assertEqual(true, rocksdb:posting_list_contains(Bin, <<"doc5000">>)),
        ?assertEqual(false, rocksdb:posting_list_contains(Bin, <<"doc99999">>)),

        ok = rocksdb:close(Db),
        rocksdb:destroy(DbPath, []),
        rocksdb_test_util:rm_rf(DbPath)
    end}.

posting_list_intersection_scale_test_() ->
    {timeout, 60, fun() ->
        DbPath = "posting_list_int_scale.test",
        rocksdb_test_util:rm_rf(DbPath),
        {ok, Db} = rocksdb:open(DbPath, [
            {create_if_missing, true},
            {merge_operator, posting_list_merge_operator}
        ]),

        %% Create two 5K posting lists with 50% overlap
        lists:foreach(fun(I) ->
            DocId = iolist_to_binary(["doc", integer_to_list(I)]),
            ok = rocksdb:merge(Db, <<"term1">>, {posting_add, DocId}, [])
        end, lists:seq(1, 5000)),

        lists:foreach(fun(I) ->
            DocId = iolist_to_binary(["doc", integer_to_list(I)]),
            ok = rocksdb:merge(Db, <<"term2">>, {posting_add, DocId}, [])
        end, lists:seq(2501, 7500)),

        {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
        {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

        %% Intersection should have 2500 elements (2501-5000)
        ?assertEqual(2500, rocksdb:posting_list_intersection_count(Bin1, Bin2)),

        ResultBin = rocksdb:posting_list_intersection(Bin1, Bin2),
        ?assertEqual(2500, rocksdb:posting_list_count(ResultBin)),

        ok = rocksdb:close(Db),
        rocksdb:destroy(DbPath, []),
        rocksdb_test_util:rm_rf(DbPath)
    end}.

%% ===================================================================
%% Compaction Tests (V2)
%% ===================================================================

posting_list_v2_compaction_test() ->
    DbPath = "posting_list_v2_compact.test",
    rocksdb_test_util:rm_rf(DbPath),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create SST file with posting list
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    lists:foreach(fun(N) ->
        PadKey = iolist_to_binary(["padding", integer_to_list(N)]),
        PadValue = binary:copy(<<"x">>, 1000),
        ok = rocksdb:put(Db, PadKey, PadValue, [])
    end, lists:seq(1, 50)),
    ok = rocksdb:flush(Db, []),

    %% Read before compaction - should be V2
    {ok, Bin1} = rocksdb:get(Db, <<"term">>, []),
    ?assertEqual(2, rocksdb:posting_list_version(Bin1)),

    %% Force compaction
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Read after compaction - should still be V2
    {ok, Bin2} = rocksdb:get(Db, <<"term">>, []),
    ?assertEqual(2, rocksdb:posting_list_version(Bin2)),
    ?assertEqual([<<"doc1">>, <<"doc2">>], rocksdb:posting_list_keys(Bin2)),

    ok = rocksdb:close(Db),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Edge Cases
%% ===================================================================

posting_list_single_key_test() ->
    DbPath = "posting_list_single.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    ?assertEqual([<<"doc1">>], rocksdb:posting_list_keys(Bin)),
    ?assertEqual(1, rocksdb:posting_list_count(Bin)),
    ?assertEqual(2, rocksdb:posting_list_version(Bin)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

posting_list_large_key_test() ->
    DbPath = "posting_list_large_key.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Add a 10KB key
    LargeKey = binary:copy(<<"x">>, 10000),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, LargeKey}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"small">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    Keys = rocksdb:posting_list_keys(Bin),
    ?assertEqual(2, length(Keys)),
    ?assert(lists:member(LargeKey, Keys)),
    ?assert(lists:member(<<"small">>, Keys)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

%% ===================================================================
%% Postings Resource API Tests
%% ===================================================================

postings_open_test() ->
    DbPath = "postings_open.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),

    %% Open as resource
    {ok, Postings} = rocksdb:postings_open(Bin),
    ?assert(is_reference(Postings)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_contains_test() ->
    DbPath = "postings_contains.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    {ok, Postings} = rocksdb:postings_open(Bin),

    %% Test exact contains
    ?assertEqual(true, rocksdb:postings_contains(Postings, <<"doc1">>)),
    ?assertEqual(true, rocksdb:postings_contains(Postings, <<"doc2">>)),
    ?assertEqual(true, rocksdb:postings_contains(Postings, <<"doc3">>)),
    ?assertEqual(false, rocksdb:postings_contains(Postings, <<"doc4">>)),
    ?assertEqual(false, rocksdb:postings_contains(Postings, <<"unknown">>)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_bitmap_contains_test() ->
    DbPath = "postings_bitmap.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    {ok, Postings} = rocksdb:postings_open(Bin),

    %% Test bitmap contains (O(1) hash lookup)
    ?assertEqual(true, rocksdb:postings_bitmap_contains(Postings, <<"doc1">>)),
    ?assertEqual(true, rocksdb:postings_bitmap_contains(Postings, <<"doc2">>)),
    ?assertEqual(true, rocksdb:postings_bitmap_contains(Postings, <<"doc3">>)),
    %% Note: bitmap may have rare false positives, but should not have false negatives
    %% for keys that exist

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_count_keys_test() ->
    DbPath = "postings_count_keys.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    {ok, Postings} = rocksdb:postings_open(Bin),

    %% Test count
    ?assertEqual(3, rocksdb:postings_count(Postings)),

    %% Test keys (should be sorted)
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Postings)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_to_binary_test() ->
    DbPath = "postings_to_bin.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    {ok, Postings} = rocksdb:postings_open(Bin),

    %% Convert back to binary
    Bin2 = rocksdb:postings_to_binary(Postings),
    ?assert(is_binary(Bin2)),
    ?assertEqual(2, rocksdb:posting_list_version(Bin2)),
    ?assertEqual([<<"doc1">>, <<"doc2">>], rocksdb:posting_list_keys(Bin2)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_intersection_test() ->
    DbPath = "postings_intersection.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc4">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    %% Test with binaries
    {ok, Result1} = rocksdb:postings_intersection(Bin1, Bin2),
    ?assertEqual([<<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Result1)),

    %% Test with resources
    {ok, P1} = rocksdb:postings_open(Bin1),
    {ok, P2} = rocksdb:postings_open(Bin2),
    {ok, Result2} = rocksdb:postings_intersection(P1, P2),
    ?assertEqual([<<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Result2)),

    %% Test mixed (binary + resource)
    {ok, Result3} = rocksdb:postings_intersection(Bin1, P2),
    ?assertEqual([<<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Result3)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_union_test() ->
    DbPath = "postings_union.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    {ok, P1} = rocksdb:postings_open(Bin1),
    {ok, P2} = rocksdb:postings_open(Bin2),

    {ok, Result} = rocksdb:postings_union(P1, P2),
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Result)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_difference_test() ->
    DbPath = "postings_diff.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    {ok, P1} = rocksdb:postings_open(Bin1),
    {ok, P2} = rocksdb:postings_open(Bin2),

    {ok, Result} = rocksdb:postings_difference(P1, P2),
    ?assertEqual([<<"doc1">>, <<"doc3">>], rocksdb:postings_keys(Result)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_intersection_count_test() ->
    DbPath = "postings_int_count.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc4">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),

    {ok, P1} = rocksdb:postings_open(Bin1),
    {ok, P2} = rocksdb:postings_open(Bin2),

    ?assertEqual(2, rocksdb:postings_intersection_count(P1, P2)),
    ?assertEqual(2, rocksdb:postings_intersection_count(Bin1, Bin2)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_intersect_all_test() ->
    DbPath = "postings_int_all.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term1">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"term2">>, {posting_add, <<"doc3">>}, []),

    ok = rocksdb:merge(Db, <<"term3">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"term3">>, {posting_add, <<"doc4">>}, []),

    {ok, Bin1} = rocksdb:get(Db, <<"term1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"term2">>, []),
    {ok, Bin3} = rocksdb:get(Db, <<"term3">>, []),

    {ok, P1} = rocksdb:postings_open(Bin1),
    {ok, P2} = rocksdb:postings_open(Bin2),
    {ok, P3} = rocksdb:postings_open(Bin3),

    %% Test with resources
    {ok, Result1} = rocksdb:postings_intersect_all([P1, P2, P3]),
    ?assertEqual([<<"doc3">>], rocksdb:postings_keys(Result1)),

    %% Test with binaries
    {ok, Result2} = rocksdb:postings_intersect_all([Bin1, Bin2, Bin3]),
    ?assertEqual([<<"doc3">>], rocksdb:postings_keys(Result2)),

    %% Test empty list
    {ok, Empty} = rocksdb:postings_intersect_all([]),
    ?assertEqual([], rocksdb:postings_keys(Empty)),

    %% Test single element
    {ok, Single} = rocksdb:postings_intersect_all([P1]),
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], rocksdb:postings_keys(Single)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).

postings_batch_lookup_test() ->
    DbPath = "postings_batch.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Create posting list with many keys
    Keys = [iolist_to_binary(["doc", integer_to_list(N)]) || N <- lists:seq(1, 100)],
    lists:foreach(fun(Key) ->
        ok = rocksdb:merge(Db, <<"term">>, {posting_add, Key}, [])
    end, Keys),

    {ok, Bin} = rocksdb:get(Db, <<"term">>, []),
    {ok, Postings} = rocksdb:postings_open(Bin),

    %% Batch lookup - all should be found
    lists:foreach(fun(Key) ->
        ?assertEqual(true, rocksdb:postings_contains(Postings, Key))
    end, Keys),

    %% Verify count
    ?assertEqual(100, rocksdb:postings_count(Postings)),

    ok = rocksdb:close(Db),
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).
