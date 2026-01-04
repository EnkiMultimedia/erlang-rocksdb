# Posting Lists

Posting lists are used for inverted indexes, search engines, and document tagging systems. erlang-rocksdb provides a specialized merge operator for efficient posting list management.

## Overview

A posting list stores a set of keys (e.g., document IDs) associated with a term. The merge operator allows efficient append operations and handles key deletion using tombstones that are automatically cleaned up during merge operations (reads and compaction).

## Binary Format

Each posting list value is a concatenation of entries:

| Field | Size | Description |
|-------|------|-------------|
| KeyLength | 4 bytes (big-endian) | Length of the key data |
| Flag | 1 byte | 0 = normal, non-zero = tombstone |
| KeyData | KeyLength bytes | The key binary |

Example entry for key `<<"doc1">>`:
- Normal: `<<4:32/big, 0, "doc1">>`
- Tombstone: `<<4:32/big, 1, "doc1">>`

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

The key is logically deleted but remains in the binary until compaction.

## Reading Posting Lists

```erlang
{ok, Binary} = rocksdb:get(Db, <<"term:erlang">>, []).
```

The binary contains all entries including tombstones. Use helper functions to process it.

## Helper Functions

### Decode to List

```erlang
Entries = rocksdb:posting_list_decode(Binary),
%% [{<<"doc1">>, false}, {<<"doc2">>, false}, {<<"doc3">>, false}, {<<"doc2">>, true}]
```

### Get Active Keys

Returns deduplicated keys with tombstones filtered out (NIF for efficiency):

```erlang
Keys = rocksdb:posting_list_keys(Binary),
%% [<<"doc1">>, <<"doc3">>]
```

### Check if Key is Active

```erlang
true = rocksdb:posting_list_contains(Binary, <<"doc1">>),
false = rocksdb:posting_list_contains(Binary, <<"doc2">>).  % tombstoned
```

### Find Key

Returns `{ok, IsTombstone}` or `not_found`:

```erlang
{ok, false} = rocksdb:posting_list_find(Binary, <<"doc1">>),
{ok, true} = rocksdb:posting_list_find(Binary, <<"doc2">>),   % tombstoned
not_found = rocksdb:posting_list_find(Binary, <<"unknown">>).
```

### Count Active Keys

```erlang
Count = rocksdb:posting_list_count(Binary).
```

### Convert to Map

Get the full state as a map:

```erlang
Map = rocksdb:posting_list_to_map(Binary),
%% #{<<"doc1">> => active, <<"doc2">> => tombstone, <<"doc3">> => active}
```

### Fold Over Entries

```erlang
Count = rocksdb:posting_list_fold(
    fun(_Key, _IsTombstone, Acc) -> Acc + 1 end,
    0,
    Binary
).
```

## Tombstone Cleanup

Tombstones are automatically cleaned up by the merge operator:

- **During reads**: When reading a key, the merge operator combines all entries and removes tombstoned keys from the result.
- **During compaction**: The merge operator consolidates entries, keeping only active (non-tombstoned) keys.

This means you don't need a separate compaction filter for tombstone removal - it's built into the merge operator.

## Use Cases

- **Inverted Index**: Map terms to document IDs for full-text search
- **Tagging System**: Map tags to item IDs
- **Graph Adjacency**: Store outgoing edges for each node
- **Set Membership**: Efficient set union via merge operations

## Performance Tips

1. **Batch Writes**: Use write_batch to add multiple keys atomically
2. **Periodic Compaction**: Run compaction to reclaim space from tombstones
3. **Large Lists**: For very large posting lists, consider sharding by key prefix
4. **NIF Functions**: Use `posting_list_keys/1`, `posting_list_contains/2`, `posting_list_find/2`, `posting_list_count/1`, and `posting_list_to_map/1` for efficient access - they are implemented as NIFs and process the binary in C++

## Example: Inverted Index

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

%% Check if document contains term
has_term(Db, Term, DocId) ->
    case rocksdb:get(Db, <<"term:", Term/binary>>, []) of
        {ok, Binary} -> rocksdb:posting_list_contains(Binary, DocId);
        not_found -> false
    end.
```
