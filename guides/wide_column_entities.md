# Wide-Column Entities

Starting with RocksDB 8.8 and Erlang RocksDB 2.0.0, you can store structured data using the Wide-Column Entity API. This allows storing multiple named columns per key, providing a more flexible schema than simple key-value pairs.

## Basic Usage

### Storing an Entity

Use `put_entity/4` to store an entity with multiple columns:

```erlang
{ok, Db} = rocksdb:open("/tmp/mydb", [{create_if_missing, true}]),

%% Store a user entity with multiple attributes
Key = <<"user:123">>,
Columns = [
    {<<"name">>, <<"Alice">>},
    {<<"email">>, <<"alice@example.com">>},
    {<<"role">>, <<"admin">>}
],
ok = rocksdb:put_entity(Db, Key, Columns, []).
```

### Retrieving an Entity

Use `get_entity/3` to retrieve all columns as a proplist:

```erlang
{ok, Result} = rocksdb:get_entity(Db, <<"user:123">>, []),
Name = proplists:get_value(<<"name">>, Result),    %% <<"Alice">>
Email = proplists:get_value(<<"email">>, Result),  %% <<"alice@example.com">>
```

### Deleting an Entity

Entities are deleted using the normal delete operation. All columns are removed when the key is deleted:

```erlang
ok = rocksdb:delete_entity(Db, <<"user:123">>, []),
%% or equivalently:
ok = rocksdb:delete(Db, <<"user:123">>, []).
```

## Column Families

Entities work with column families just like regular key-values:

```erlang
{ok, Db, [DefaultCf, UsersCf]} = rocksdb:open("/tmp/mydb",
    [{create_if_missing, true}, {create_missing_column_families, true}],
    [{"default", []}, {"users", []}]),

%% Store entity in users column family
ok = rocksdb:put_entity(Db, UsersCf, <<"user:456">>,
    [{<<"name">>, <<"Bob">>}], []),

%% Retrieve from column family
{ok, Result} = rocksdb:get_entity(Db, UsersCf, <<"user:456">>, []).
```

## Iterating Over Entities

When iterating, use `iterator_columns/1` to access the columns of the current entry:

```erlang
{ok, Itr} = rocksdb:iterator(Db, []),
{ok, Key, _Value} = rocksdb:iterator_move(Itr, first),

%% Get all columns for the current entry
{ok, Columns} = rocksdb:iterator_columns(Itr),
lists:foreach(fun({Name, Value}) ->
    io:format("~s: ~s~n", [Name, Value])
end, Columns),

rocksdb:iterator_close(Itr).
```

### Mixed Data

`iterator_columns/1` works for both entities and regular key-values:

- **Entities**: Returns all stored columns
- **Regular key-values**: Returns `[{<<>>, Value}]` - a single column with the default (empty) name

```erlang
%% Store mixed data
ok = rocksdb:put(Db, <<"plain_key">>, <<"plain_value">>, []),
ok = rocksdb:put_entity(Db, <<"entity_key">>,
    [{<<"attr">>, <<"value">>}], []),

{ok, Itr} = rocksdb:iterator(Db, []),

%% First entry: entity
{ok, <<"entity_key">>, _} = rocksdb:iterator_move(Itr, first),
{ok, [{<<"attr">>, <<"value">>}]} = rocksdb:iterator_columns(Itr),

%% Second entry: plain key-value
{ok, <<"plain_key">>, <<"plain_value">>} = rocksdb:iterator_move(Itr, next),
{ok, [{<<>>, <<"plain_value">>}]} = rocksdb:iterator_columns(Itr),

rocksdb:iterator_close(Itr).
```

## The Default Column

In RocksDB's wide-column model, the default column has an empty name (`<<>>`). When you use `put/4` to store a regular key-value, it's internally treated as an entity with a single default column:

```erlang
%% These are equivalent from RocksDB's perspective:
rocksdb:put(Db, <<"key">>, <<"value">>, []),
rocksdb:put_entity(Db, <<"key">>, [{<<>>, <<"value">>}], []).
```

When using `get/3` on an entity, RocksDB returns the value of the default column (or empty if none exists).

## Multi-Column Family Iteration

Use `coalescing_iterator/3` to iterate across multiple column families efficiently:

```erlang
{ok, Db, [Cf1, Cf2, Cf3]} = rocksdb:open("/tmp/mydb",
    [{create_if_missing, true}, {create_missing_column_families, true}],
    [{"default", []}, {"cf1", []}, {"cf2", []}]),

%% Store data in different column families
ok = rocksdb:put_entity(Db, Cf1, <<"a">>, [{<<"src">>, <<"cf1">>}], []),
ok = rocksdb:put_entity(Db, Cf2, <<"b">>, [{<<"src">>, <<"cf2">>}], []),

%% Iterate across all column families in sorted key order
{ok, Itr} = rocksdb:coalescing_iterator(Db, [Cf1, Cf2], []),
{ok, <<"a">>, _} = rocksdb:iterator_move(Itr, first),
{ok, <<"b">>, _} = rocksdb:iterator_move(Itr, next),
rocksdb:iterator_close(Itr).
```

## Use Cases

Wide-column entities are useful when:

- You have structured data with multiple attributes per key
- You want to avoid serialization/deserialization overhead
- Different keys may have different sets of columns
- You need to update individual columns without rewriting the entire value

## API Reference

| Function | Description |
|----------|-------------|
| `put_entity/4` | Store entity in default column family |
| `put_entity/5` | Store entity in specified column family |
| `get_entity/3` | Get entity from default column family |
| `get_entity/4` | Get entity from specified column family |
| `delete_entity/3` | Delete entity from default column family |
| `delete_entity/4` | Delete entity from specified column family |
| `iterator_columns/1` | Get columns from current iterator position |
| `coalescing_iterator/3` | Create iterator across multiple column families |
