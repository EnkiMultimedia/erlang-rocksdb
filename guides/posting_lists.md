# Posting Lists

Posting lists are used for inverted indexes, search engines, and document tagging systems. erlang-rocksdb provides a specialized merge operator for efficient posting list management with built-in support for set operations and roaring bitmap indexes.

## Overview

A posting list stores a set of keys (e.g., document IDs) associated with a term. The merge operator allows efficient append operations and handles key deletion using tombstones that are automatically cleaned up during merge operations (reads and compaction).

**Key Features:**
- Sorted keys (lexicographic order)
- Roaring bitmap for fast existence checks and set operations
- Automatic V1 to V2 format migration
- Efficient intersection, union, and difference operations

## Binary Formats

### V2 Format (Current)

V2 is the default format as of version 2.5.0. It stores keys sorted and includes a roaring64 bitmap for fast operations.

| Field | Size | Description |
|-------|------|-------------|
| Version | 1 byte | 0x02 for V2 |
| BitmapSize | 4 bytes (big-endian) | Size of serialized roaring bitmap |
| BitmapData | BitmapSize bytes | Serialized roaring64 bitmap |
| KeyCount | 4 bytes (big-endian) | Number of keys |
| Keys | Variable | Sorted entries: `<Len:32/big><Key:Len>...` |

### V1 Format (Legacy)

V1 format is still readable but will be automatically upgraded to V2 on the next merge operation.

| Field | Size | Description |
|-------|------|-------------|
| KeyLength | 4 bytes (big-endian) | Length of the key data |
| Flag | 1 byte | 0 = normal, non-zero = tombstone |
| KeyData | KeyLength bytes | The key binary |

## Setup

Open a database with the posting list merge operator:

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {merge_operator, posting_list_merge_operator}
]).
```

## Adding Keys

Use merge with `{posting_add, Key}`:

```erlang
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_add, <<"doc1">>}, []),
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_add, <<"doc2">>}, []),
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_add, <<"doc3">>}, []).
```

## Deleting Keys

Use merge with `{posting_delete, Key}` to add a tombstone:

```erlang
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_delete, <<"doc2">>}, []).
```

The key is logically deleted and removed during the merge operation.

## Reading Posting Lists

```erlang
{ok, Binary} = rocksdb:get(Db, <<"term:erlang">>, []).
```

## Helper Functions

### Get Active Keys (Sorted)

Returns deduplicated keys in lexicographic order:

```erlang
Keys = rocksdb:posting_list_keys(Binary),
%% [<<"doc1">>, <<"doc3">>]  % sorted
```

### Check if Key is Active

```erlang
true = rocksdb:posting_list_contains(Binary, <<"doc1">>),
false = rocksdb:posting_list_contains(Binary, <<"doc2">>).  % deleted
```

### Find Key

Returns `{ok, IsTombstone}` or `not_found`:

```erlang
{ok, false} = rocksdb:posting_list_find(Binary, <<"doc1">>),
not_found = rocksdb:posting_list_find(Binary, <<"unknown">>).
```

### Count Active Keys

```erlang
Count = rocksdb:posting_list_count(Binary).
```

### Get Format Version

```erlang
2 = rocksdb:posting_list_version(Binary).  % V2 format
```

### Convert to Map

Get the full state as a map:

```erlang
Map = rocksdb:posting_list_to_map(Binary),
%% #{<<"doc1">> => active, <<"doc3">> => active}
```

### Decode to List (V1 only)

For V1 format binaries:

```erlang
Entries = rocksdb:posting_list_decode(Binary),
%% [{<<"doc1">>, false}, {<<"doc2">>, true}]
```

### Fold Over Entries

```erlang
Count = rocksdb:posting_list_fold(
    fun(_Key, _IsTombstone, Acc) -> Acc + 1 end,
    0,
    Binary
).
```

## Set Operations

V2 posting lists support efficient set operations using roaring bitmaps:

### Intersection

Find keys present in both posting lists:

```erlang
{ok, Bin1} = rocksdb:get(Db, <<"term:erlang">>, []),
{ok, Bin2} = rocksdb:get(Db, <<"term:otp">>, []),
ResultBin = rocksdb:posting_list_intersection(Bin1, Bin2),
CommonKeys = rocksdb:posting_list_keys(ResultBin).
```

### Union

Combine all keys from both posting lists:

```erlang
ResultBin = rocksdb:posting_list_union(Bin1, Bin2),
AllKeys = rocksdb:posting_list_keys(ResultBin).
```

### Difference

Find keys in the first list but not in the second:

```erlang
ResultBin = rocksdb:posting_list_difference(Bin1, Bin2),
UniqueKeys = rocksdb:posting_list_keys(ResultBin).
```

### Fast Intersection Count

Get cardinality without materializing the result (uses roaring bitmap):

```erlang
Count = rocksdb:posting_list_intersection_count(Bin1, Bin2).
```

### Multi-List Intersection

Intersect multiple posting lists efficiently (processes smallest first):

```erlang
ResultBin = rocksdb:posting_list_intersect_all([Bin1, Bin2, Bin3]).
```

### Bitmap Contains (Fast Lookup)

Fast hash-based lookup using the embedded bitmap:

```erlang
true = rocksdb:posting_list_bitmap_contains(Binary, <<"doc1">>).
```

Note: May have rare false positives due to hash collisions. Use `posting_list_contains/2` for exact checks.

## Postings Resource API

For repeated lookups on the same posting list, use the resource-based API. Parse once, lookup many times with O(1) performance.

### Open/Parse Posting List

```erlang
{ok, Binary} = rocksdb:get(Db, <<"term:erlang">>, []),
{ok, Postings} = rocksdb:postings_open(Binary).
```

### Fast Contains Lookup

```erlang
%% Exact match (O(log n) using sorted set)
true = rocksdb:postings_contains(Postings, <<"doc1">>),

%% Hash-based lookup (O(1), rare false positives)
true = rocksdb:postings_bitmap_contains(Postings, <<"doc1">>).
```

### Count and Keys

```erlang
Count = rocksdb:postings_count(Postings),
Keys = rocksdb:postings_keys(Postings).  %% sorted
```

### Set Operations on Resources

Set operations accept both binaries and resources, returning a resource:

```erlang
%% Intersection (AND)
{ok, Result} = rocksdb:postings_intersection(Postings1, Postings2),

%% Union (OR)
{ok, Result} = rocksdb:postings_union(Postings1, Postings2),

%% Difference (A - B)
{ok, Result} = rocksdb:postings_difference(Postings1, Postings2),

%% Fast intersection count
Count = rocksdb:postings_intersection_count(Postings1, Postings2),

%% Multi-way intersection
{ok, Result} = rocksdb:postings_intersect_all([P1, P2, P3]).
```

### Convert Back to Binary

```erlang
Binary = rocksdb:postings_to_binary(Postings).
```

### Performance Comparison

| Method | Lookup Time |
|--------|-------------|
| `postings_contains/2` | ~0.1-0.2 us |
| `postings_bitmap_contains/2` | ~0.1 us |
| `posting_list_to_map/1` + `maps:is_key/2` | ~0.1 us |
| `posting_list_contains/2` (binary) | ~1-10 us |

Use the resource API for batch lookups (e.g., checking many document IDs against a term's posting list).

## Tombstone Cleanup

Tombstones are automatically cleaned up by the merge operator:

- **During reads**: When reading a key, the merge operator combines all entries and removes tombstoned keys from the result.
- **During compaction**: The merge operator consolidates entries, keeping only active (non-tombstoned) keys.

This means you don't need a separate compaction filter for tombstone removal - it's built into the merge operator.

## Format Migration

V1 data is automatically migrated to V2 format on the next merge operation. No manual migration is needed. You can check the format version with:

```erlang
Version = rocksdb:posting_list_version(Binary).
%% 1 = V1 (legacy), 2 = V2 (current)
```

## Use Cases

- **Inverted Index**: Map terms to document IDs for full-text search
- **Tagging System**: Map tags to item IDs
- **Graph Adjacency**: Store outgoing edges for each node
- **Set Membership**: Efficient set operations via roaring bitmaps
- **Query Intersection**: Fast AND queries across multiple terms

## Performance Tips

1. **Batch Writes**: Use write_batch to add multiple keys atomically
2. **Periodic Compaction**: Run compaction to reclaim space and optimize layout
3. **Large Lists**: For very large posting lists (100K+), consider sharding by key prefix
4. **Use Set Operations**: For multi-term queries, use `postings_intersection/2` or `postings_intersect_all/1` instead of manual iteration
5. **Intersection Count**: Use `postings_intersection_count/2` when you only need cardinality
6. **Use Resources for Batch Lookups**: For multiple contains checks, use `postings_open/1` once then `postings_contains/2` for each lookup (~0.1 us vs ~1-10 us per lookup)
7. **NIF Functions**: All helper functions are implemented as NIFs for efficiency

## Example: Inverted Index with Query

```erlang
%% Index a document
index_document(Db, DocId, Terms) ->
    lists:foreach(fun(Term) ->
        ok = rocksdb:merge(Db, <<"term:", Term/binary>>, {posting_add, DocId}, [])
    end, Terms).

%% Remove document from index
remove_document(Db, DocId, Terms) ->
    lists:foreach(fun(Term) ->
        ok = rocksdb:merge(Db, <<"term:", Term/binary>>, {posting_delete, DocId}, [])
    end, Terms).

%% Search for documents containing a term
search(Db, Term) ->
    case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
        {ok, Binary} -> rocksdb:posting_list_keys(Binary);
        not_found -> []
    end.

%% Search for documents containing ALL terms (AND query)
search_all(Db, Terms) ->
    Postings = lists:filtermap(fun(Term) ->
        case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
            {ok, Binary} ->
                {ok, P} = rocksdb:postings_open(Binary),
                {true, P};
            not_found -> false
        end
    end, Terms),
    case Postings of
        [] -> [];
        _ ->
            {ok, Result} = rocksdb:postings_intersect_all(Postings),
            rocksdb:postings_keys(Result)
    end.

%% Search for documents containing ANY term (OR query)
search_any(Db, Terms) ->
    Postings = lists:filtermap(fun(Term) ->
        case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
            {ok, Binary} ->
                {ok, P} = rocksdb:postings_open(Binary),
                {true, P};
            not_found -> false
        end
    end, Terms),
    case Postings of
        [] -> [];
        [Single] -> rocksdb:postings_keys(Single);
        [First | Rest] ->
            {ok, Result} = lists:foldl(fun(P, {ok, Acc}) ->
                rocksdb:postings_union(Acc, P)
            end, {ok, First}, Rest),
            rocksdb:postings_keys(Result)
    end.

%% Check if document contains term (single lookup)
has_term(Db, Term, DocId) ->
    case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
        {ok, Binary} -> rocksdb:posting_list_contains(Binary, DocId);
        not_found -> false
    end.

%% Check if document contains term (batch lookups - use resource)
has_term_batch(Postings, DocId) ->
    rocksdb:postings_contains(Postings, DocId).

%% Count documents matching AND query
count_matches(Db, Terms) ->
    Postings = lists:filtermap(fun(Term) ->
        case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
            {ok, Binary} ->
                {ok, P} = rocksdb:postings_open(Binary),
                {true, P};
            not_found -> false
        end
    end, Terms),
    case Postings of
        [] -> 0;
        [Single] -> rocksdb:postings_count(Single);
        [First, Second | Rest] ->
            %% Use fast intersection count
            lists:foldl(fun(P, Count) ->
                min(Count, rocksdb:postings_intersection_count(First, P))
            end, rocksdb:postings_intersection_count(First, Second), Rest)
    end.

%% Filter documents by multiple terms (batch contains check)
filter_docs(Db, Term, DocIds) ->
    case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
        {ok, Binary} ->
            {ok, Postings} = rocksdb:postings_open(Binary),
            [DocId || DocId <- DocIds,
                      rocksdb:postings_contains(Postings, DocId)];
        not_found -> []
    end.
```
