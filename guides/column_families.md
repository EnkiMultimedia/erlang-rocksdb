Each key-value pair in RocksDB is associated with exactly one Column Family. If there is no Column Family specified, key-value pair is associated with Column Family "default".

Column Families provide a way to logically partition the database. Some interesting properties:

* Atomic writes across Column Families are supported. This means you can atomically execute Write({cf1, key1, value1}, {cf2, key2, value2}).
* Consistent view of the database across Column Families.
* Ability to configure different Column Families independently.
* On-the-fly adding new Column Families and dropping them. Both operations are reasonably fast.

## basic key/value operations

```erlang
ColumnFamilies = [{"default", []}],
{ok, Db, [DefaultH]} = rocksdb:open("test.db", [{create_if_missing, true}], ColumnFamilies),
ok = rocksdb:put(Db, DefaultH, <<"a">>, <<"a1">>, []),
{ok,  <<"a1">>} = rocksdb:get(Db, DefaultH, <<"a">>, []),
ok = rocksdb:put(Db, DefaultH, <<"b">>, <<"b1">>, []),
{ok, <<"b1">>} = rocksdb:get(Db, DefaultH, <<"b">>, []),

ok = rocksdb:delete(Db, DefaultH, <<"b">>, []),
not_found = rocksdb:get(Db, DefaultH, <<"b">>, []),

{ok, TestH} = rocksdb:create_column_family(Db, "test", []),
rocksdb:put(Db, TestH, <<"a">>, <<"a2">>, []),
{ok,  <<"a1">>} = rocksdb:get(Db, DefaultH, <<"a">>, []),
{ok,  <<"a2">>} = rocksdb:get(Db, TestH, <<"a">>, []),
rocksdb:close(Db)
```

## iterator operations

```erlang
{ok, Ref, [DefaultH]} = rocksdb:open("ltest", [{create_if_missing, true}], [{"default", []}]),
{ok, TestH} = rocksdb:create_column_family(Ref, "test", []),
try
rocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
rocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
rocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
rocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

{ok, DefaultIt} = rocksdb:iterator(Ref, DefaultH, []),
{ok, TestIt} = rocksdb:iterator(Ref, TestH, []),

?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, <<>>)),
?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, <<>>)),
?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(DefaultIt, next)),
?assertEqual({ok, <<"b">>, <<"y1">>},rocksdb:iterator_move(TestIt, next)),
?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, prev)),
?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, prev)),
ok = rocksdb:iterator_close(TestIt),
ok = rocksdb:iterator_close(DefaultIt)
after
rocksdb:close(Ref)
end.
```