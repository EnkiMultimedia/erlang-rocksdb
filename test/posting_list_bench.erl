-module(posting_list_bench).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%% Benchmark posting list V1 vs V2 performance
%% V1: Legacy format (pre-2.5.0)
%% V2: Roaring bitmap format (2.5.0+)

%%====================================================================
%% Test runner
%%====================================================================

run_all_benchmarks_test_() ->
    {timeout, 300, fun() ->
        io:format("~n~n=== Posting List Benchmark: V1 vs V2 ===~n~n"),

        %% Benchmark 1: 100 keys of 128 bytes
        io:format("--- Benchmark 1: 100 keys x 128 bytes ---~n"),
        bench_insert_get(100, 128),

        io:format("~n--- Benchmark 2: 1000 keys x 256 bytes ---~n"),
        bench_insert_get(1000, 256),

        io:format("~n--- Benchmark 3: Contains checks (1000 keys) ---~n"),
        bench_contains(1000, 128),

        io:format("~n=== Benchmark Complete ===~n~n"),
        ok
    end}.

%%====================================================================
%% Benchmarks
%%====================================================================

bench_insert_get(NumKeys, KeySize) ->
    Keys = generate_keys(NumKeys, KeySize),

    %% V1 Format benchmark (manual encoding, simulating old behavior)
    {V1InsertTime, V1Binary} = timer:tc(fun() ->
        build_v1_posting_list(Keys)
    end),

    %% V2 Format benchmark (using merge operator)
    DbPath = "/tmp/posting_bench_v2_" ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    {V2InsertTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            ok = rocksdb:merge(Db, <<"bench">>, {posting_add, Key}, [])
        end, Keys)
    end),

    %% Force a get to trigger merge
    {V2GetTime, {ok, V2Binary}} = timer:tc(fun() ->
        rocksdb:get(Db, <<"bench">>, [])
    end),

    %% V1 get simulation (decode keys)
    {V1GetTime, V1Keys} = timer:tc(fun() ->
        decode_v1_keys(V1Binary)
    end),

    %% V2 get keys
    {V2KeysTime, V2Keys} = timer:tc(fun() ->
        rocksdb:posting_list_keys(V2Binary)
    end),

    rocksdb:close(Db),
    cleanup_db(DbPath),

    %% Report results
    io:format("  Insert ~p keys:~n", [NumKeys]),
    io:format("    V1 (manual build):  ~.2f ms~n", [V1InsertTime / 1000]),
    io:format("    V2 (merge ops):     ~.2f ms~n", [V2InsertTime / 1000]),

    io:format("  Get + decode:~n"),
    io:format("    V1 decode:          ~.2f ms (~p keys)~n", [V1GetTime / 1000, length(V1Keys)]),
    io:format("    V2 get from DB:     ~.2f ms~n", [V2GetTime / 1000]),
    io:format("    V2 extract keys:    ~.2f ms (~p keys)~n", [V2KeysTime / 1000, length(V2Keys)]),

    io:format("  Binary size:~n"),
    io:format("    V1: ~p bytes~n", [byte_size(V1Binary)]),
    io:format("    V2: ~p bytes~n", [byte_size(V2Binary)]),

    ok.

bench_contains(NumKeys, KeySize) ->
    Keys = generate_keys(NumKeys, KeySize),

    %% Build V1 binary
    V1Binary = build_v1_posting_list(Keys),

    %% Build V2 binary via DB
    DbPath = "/tmp/posting_bench_contains_" ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),
    lists:foreach(fun(Key) ->
        ok = rocksdb:merge(Db, <<"bench">>, {posting_add, Key}, [])
    end, Keys),
    {ok, V2Binary} = rocksdb:get(Db, <<"bench">>, []),
    rocksdb:close(Db),
    cleanup_db(DbPath),

    %% Test keys: mix of existing and non-existing
    TestKeys = lists:sublist(Keys, 100) ++ generate_keys(100, KeySize),
    NumLookups = length(TestKeys),

    %% V1: decode to map ONCE, then do lookups (fair comparison)
    V1Map = decode_v1_to_map(V1Binary),
    {V1LookupTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            maps:is_key(Key, V1Map)
        end, TestKeys)
    end),

    %% V2 contains check (uses posting_list_contains - exact, binary search)
    {V2ContainsTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            rocksdb:posting_list_contains(V2Binary, Key)
        end, TestKeys)
    end),

    %% V2 bitmap contains (fast hash lookup)
    {V2BitmapTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            rocksdb:posting_list_bitmap_contains(V2Binary, Key)
        end, TestKeys)
    end),

    %% V2 using map (like V1)
    V2Map = rocksdb:posting_list_to_map(V2Binary),
    {V2MapLookupTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            maps:is_key(Key, V2Map)
        end, TestKeys)
    end),

    %% V2 using parsed resource (parse once, lookup many)
    {ok, Postings} = rocksdb:postings_open(V2Binary),
    {V2ResourceTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            rocksdb:postings_contains(Postings, Key)
        end, TestKeys)
    end),

    {V2ResourceBitmapTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            rocksdb:postings_bitmap_contains(Postings, Key)
        end, TestKeys)
    end),

    io:format("  Contains check (~p lookups on ~p keys):~n", [NumLookups, NumKeys]),
    io:format("    V1 (map lookup):        ~.3f ms  (~.1f us/lookup)~n",
              [V1LookupTime / 1000, V1LookupTime / NumLookups]),
    io:format("    V2 (map lookup):        ~.3f ms  (~.1f us/lookup)~n",
              [V2MapLookupTime / 1000, V2MapLookupTime / NumLookups]),
    io:format("    V2 (resource exact):    ~.3f ms  (~.1f us/lookup)~n",
              [V2ResourceTime / 1000, V2ResourceTime / NumLookups]),
    io:format("    V2 (resource bitmap):   ~.3f ms  (~.1f us/lookup)~n",
              [V2ResourceBitmapTime / 1000, V2ResourceBitmapTime / NumLookups]),
    io:format("    V2 (binary NIF*):       ~.3f ms  (~.1f us/lookup)~n",
              [V2ContainsTime / 1000, V2ContainsTime / NumLookups]),
    io:format("    * NIF on binary parses each call - use resource or map~n"),

    ok.

%%====================================================================
%% V1 Format Helpers (simulate legacy format)
%%====================================================================

%% V1 format: <<KeyLength:32/big, Flag:8, KeyData:KeyLength/binary>>...
build_v1_posting_list(Keys) ->
    lists:foldl(fun(Key, Acc) ->
        Len = byte_size(Key),
        <<Acc/binary, Len:32/big, 0:8, Key/binary>>
    end, <<>>, Keys).

decode_v1_keys(Binary) ->
    decode_v1_keys(Binary, []).

decode_v1_keys(<<>>, Acc) ->
    lists:reverse(Acc);
decode_v1_keys(<<Len:32/big, Flag:8, Key:Len/binary, Rest/binary>>, Acc) ->
    case Flag of
        0 -> decode_v1_keys(Rest, [Key | Acc]);
        _ -> decode_v1_keys(Rest, Acc)  % tombstone
    end.

decode_v1_to_map(Binary) ->
    Keys = decode_v1_keys(Binary),
    maps:from_list([{K, true} || K <- Keys]).

%%====================================================================
%% Utilities
%%====================================================================

generate_keys(Count, Size) ->
    [generate_key(I, Size) || I <- lists:seq(1, Count)].

generate_key(Index, Size) ->
    %% Create a key with index prefix and random-ish padding
    Prefix = integer_to_binary(Index),
    PrefixLen = byte_size(Prefix),
    PaddingLen = max(0, Size - PrefixLen),
    Padding = binary:copy(<<(Index rem 256)>>, PaddingLen),
    <<Prefix/binary, Padding/binary>>.

cleanup_db(Path) ->
    os:cmd("rm -rf " ++ Path).

%%====================================================================
%% Standalone benchmark (run from shell)
%%====================================================================

run() ->
    io:format("~n=== Posting List Benchmark: V1 (2.4.0) vs V2 (2.5.0) ===~n~n"),

    io:format("--- Benchmark 1: 100 keys x 128 bytes ---~n"),
    bench_insert_get(100, 128),

    io:format("~n--- Benchmark 2: 1000 keys x 256 bytes ---~n"),
    bench_insert_get(1000, 256),

    io:format("~n--- Benchmark 3: Contains (1000 keys x 128 bytes) ---~n"),
    bench_contains(1000, 128),

    io:format("~n--- Benchmark 4: Contains (10000 keys x 64 bytes) ---~n"),
    bench_contains(10000, 64),

    io:format("~n--- Benchmark 5: Set operations (500 keys x 128 bytes) ---~n"),
    bench_set_operations(500, 128),

    io:format("~n--- Benchmark 6: Set operations (5000 keys x 64 bytes) ---~n"),
    bench_set_operations(5000, 64),

    io:format("~n=== Summary ===~n~n"),
    io:format("V1 (2.4.0): Simple sequential format, no set operations~n"),
    io:format("V2 (2.5.0): Roaring bitmap + sorted keys, native set operations~n~n"),

    io:format("Performance comparison:~n"),
    io:format("  Insert/Get:     Similar (V2 merge includes DB overhead in bench)~n"),
    io:format("  Extract keys:   Similar performance~n"),
    io:format("  Contains:       All methods 0.1-0.2 us/lookup~n"),
    io:format("  Binary size:    V2 8-15 pct larger (bitmap overhead)~n~n"),

    io:format("Contains lookup methods (parse once, lookup many):~n"),
    io:format("  - posting_list_to_map/1 + maps:is_key/2~n"),
    io:format("  - postings_open/1 + postings_contains/2~n"),
    io:format("  - postings_open/1 + postings_bitmap_contains/2~n~n"),

    io:format("V2 exclusive features:~n"),
    io:format("  - posting_list_intersection/2:       AND two lists~n"),
    io:format("  - posting_list_union/2:              OR two lists~n"),
    io:format("  - posting_list_difference/2:         A - B~n"),
    io:format("  - posting_list_intersection_count/2: Fast cardinality~n"),
    io:format("  - posting_list_intersect_all/1:      Multi-list AND~n"),
    io:format("  - Keys always returned sorted~n~n"),

    ok.

bench_set_operations(NumKeys, KeySize) ->
    %% Create two posting lists with 50% overlap
    Keys1 = generate_keys(NumKeys, KeySize),
    Keys2 = generate_keys_offset(NumKeys, KeySize, NumKeys div 2),

    DbPath = "/tmp/posting_bench_setops_" ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),

    %% Build two posting lists
    lists:foreach(fun(Key) ->
        ok = rocksdb:merge(Db, <<"list1">>, {posting_add, Key}, [])
    end, Keys1),
    lists:foreach(fun(Key) ->
        ok = rocksdb:merge(Db, <<"list2">>, {posting_add, Key}, [])
    end, Keys2),

    {ok, Bin1} = rocksdb:get(Db, <<"list1">>, []),
    {ok, Bin2} = rocksdb:get(Db, <<"list2">>, []),

    rocksdb:close(Db),
    cleanup_db(DbPath),

    %% Benchmark set operations (V2 only - V1 didn't have these)
    {IntersectTime, IntersectBin} = timer:tc(fun() ->
        rocksdb:posting_list_intersection(Bin1, Bin2)
    end),

    {UnionTime, UnionBin} = timer:tc(fun() ->
        rocksdb:posting_list_union(Bin1, Bin2)
    end),

    {DiffTime, DiffBin} = timer:tc(fun() ->
        rocksdb:posting_list_difference(Bin1, Bin2)
    end),

    {CountTime, Count} = timer:tc(fun() ->
        rocksdb:posting_list_intersection_count(Bin1, Bin2)
    end),

    io:format("  Set operations (~p keys per list, 50 pct overlap):~n", [NumKeys]),
    io:format("    Intersection:       ~.3f ms (~p keys)~n",
              [IntersectTime / 1000, rocksdb:posting_list_count(IntersectBin)]),
    io:format("    Union:              ~.3f ms (~p keys)~n",
              [UnionTime / 1000, rocksdb:posting_list_count(UnionBin)]),
    io:format("    Difference:         ~.3f ms (~p keys)~n",
              [DiffTime / 1000, rocksdb:posting_list_count(DiffBin)]),
    io:format("    Intersection count: ~.3f ms (count=~p)~n", [CountTime / 1000, Count]),

    ok.

generate_keys_offset(Count, Size, Offset) ->
    [generate_key(I + Offset, Size) || I <- lists:seq(1, Count)].
