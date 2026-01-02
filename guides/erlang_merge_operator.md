Starting in version 0.21.0, Erlang Rocksdb supports a Merge Operator for Erlang data types. 

RocksDB offers the possibility of doing appends to existing key values efficiently through the use of a [merge operator](https://github.com/facebook/rocksdb/wiki/Merge-Operator). This operator is a user-provided callback that knows how to merge the old value ("the message") and the new value ("the delta") into a single value ("the merged value").

The Erlang Merge Operator allows you to combine two values of the same Erlang data type into a single value. As long as your data is stored as Erlang binary term (encoded using the `term_to_binary` function), , it should be possible to apply a single predefined merge operator in order to take advantage of the RocksDB merge operation. 

For example appending an item to a list (like `++`):

```erlang
%% store a list encoded using `term_to_binary`
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b] = binary_to_term(Bin0),
%% append two items
ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_append, [c, d]}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d] = binary_to_term(Bin1),
```

## Setup a merge operator

To set the merge operator, add it to the columns options (or the db options if only one column) by setting the `merge_operator` option:

```erlang
{ok, Db} = rocksdb:open("mydb",
                        [{create_if_missing, true},
                         {merge_operator, erlang_merge_operator}]).
```

## Operations: 

### on integer

The Erlang merge operator support the `int_add` operation to add an integer value:

```erlang
 ok = rocksdb:put(Db, <<"i">>, term_to_binary(0), []),
{ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
0 = binary_to_term(IBin0),

ok = rocksdb:merge(Db, <<"i">>, term_to_binary({int_add, 1}), []),
{ok, IBin1} = rocksdb:get(Db, <<"i">>, []),
1 = binary_to_term(IBin1),
```

### on lists

The Erlang merge operator support the following operations on lists:

* `list_append` like `++` or `lists:append/2`: Returns a new list List3, which is made from the elements of List1 followed by the elements of List2.

```erlang
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b] = binary_to_term(Bin0),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_append, [c, d]}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d] = binary_to_term(Bin1),
```

* `list_substract`: like `lists:subtract/2` or `--`, Returns a new list List3 that is a copy of List1, subjected to the following procedure: for each element in List2, its first occurrence in List1 is deleted.

```erlang
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, a, b, c]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d, e, a, b, c] = binary_to_term(Bin0),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_substract, [c, a]}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[b, d, e, a, b, c] = binary_to_term(Bin1),

```

* `list_set`: to set an element in the list at a position:

```erlang
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d, e] = binary_to_term(Bin0),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_set, 2, 'c1'}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[a, b, 'c1', d, e] = binary_to_term(Bin1),
```

* `list_delete` : to delete an 1 or more elements a at position in the list:

```erlang
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, f, g]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d, e, f, g] = binary_to_term(Bin0),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 2}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[a, b, d, e, f, g] = binary_to_term(Bin1),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 2, 4}), []),
{ok, Bin2} = rocksdb:get(Db, <<"list">>, []),
[a, b, g] = binary_to_term(Bin2),
```

* `list_insert`: to insert N elements at a position in a list:

```erlang
ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, f, g]), []),
{ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
[a, b, c, d, e, f, g] = binary_to_term(Bin0),

ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_insert, 2, [h, i]}), []),
{ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
[a, b, h, i, c, d, e, f, g] = binary_to_term(Bin1),
```

### on binary

The Erlang merge operator support the following operations on binaries:

* `binary_append`: to append a binary:

```erlang
ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_append, <<"abc">>}), []),
{ok, Bin1} = rocksdb:get(Db, <<"encbin">>, []),
<<"testabc">> = binary_to_term(Bin1),

ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_append, <<"de">>}), []),
{ok, Bin2} = rocksdb:get(Db, <<"encbin">>, []),
<<"testabcde">> = binary_to_term(Bin2),
```

* `binary_replace`: Constructs a new binary by replacing the part in the range with the content of replacement. The range is given in bits:

```erlang
 ok = rocksdb:put(Db, <<"encbin">>, term_to_binary(<<"The quick brown fox jumps over the lazy dog.">>), []),
{ok, Bin} = rocksdb:get(Db, <<"encbin">>, []),
<<"The quick brown fox jumps over the lazy dog.">> = binary_to_term(Bin),

ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 10, 5, <<"red">>}), []),
ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 0, 3, <<"A">>}), []),
{ok, Bin1} = rocksdb:get(Db, <<"encbin">>, []),
<<"A quick red fox jumps over the lazy dog.">> = binary_to_term(Bin1),

ok = rocksdb:put(Db, <<"bitmap">>, term_to_binary(<<1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1>>), []),
ok = rocksdb:merge(Db, <<"bitmap">>, term_to_binary({binary_replace, 2, 1, <<0>>}), []),
{ok, Bin2} = rocksdb:get(Db, <<"bitmap">>, []),
  <<1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1>> = binary_to_term(Bin2),
```

* `binary_erase`: Constructs a new binary by deleting the part in the range. The range is given in bits:

```erlang
ok = rocksdb:put(Db, <<"eraseterm">>, term_to_binary(<<"abcdefghij">>), []),
ok = rocksdb:merge(Db, <<"eraseterm">>, term_to_binary({binary_erase, 2, 4}), []),
{ok, Bin} = rocksdb:get(Db, <<"eraseterm">>, []),
<<"abghij">> = binary_to_term(Bin),
```

* `binary_insert`: Constructs a new binary by inserting a part at a position given in bits:

```erlang
ok = rocksdb:put(Db, <<"insertterm">>, term_to_binary(<<"abcdefghij">>), []),
ok = rocksdb:merge(Db, <<"insertterm">>, term_to_binary({binary_insert, 2, <<"1234">>}), []),
{ok, Bin} = rocksdb:get(Db, <<"insertterm">>, []),
<<"ab1234cdefghij">> = binary_to_term(Bin),
```