The bitset operator maintains a fixed-size bitmap. Use it when you want to set and clear single bits with merges instead of reading and rewriting the whole value.

`{bitset_merge_operator, Bits}` sizes the bitmap to `Bits` bits. Each merge operand is one of:

* `<<"+N">>` sets bit `N`
* `<<"-N">>` clears bit `N`
* `<<>>` clears every bit

`N` is a decimal position, starting at 0, and must be inside the bitmap. Any other operand makes the merge fail: reads of that key return `{error, {corruption, _}}`, the rest of the database is not affected.


Ex:

```erlang

 {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                        [{create_if_missing, true},
                         {merge_operator, {bitset_merge_operator, 1024}}]),

Bitmap = << 0:1024/unsigned >>,
ok = rocksdb:put(Db, <<"bitmap">>, Bitmap, []),
ok = rocksdb:merge(Db, <<"bitmap">>, <<"+2">>, []),
{ok, << 32, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
ok = rocksdb:merge(Db, <<"bitmap">>, <<"-2">>, []),
{ok, << 0, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
ok = rocksdb:merge(Db, <<"bitmap">>, <<"+11">>, []),
{ok, << 0, 16, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
ok = rocksdb:merge(Db, <<"bitmap">>, <<"+10">>, []),
{ok, << 0, 48, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),

ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+2">>, []),
{ok, << 32, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"-2">>, []),
{ok, << 0, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+11">>, []),
{ok, << 0, 16, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+10">>, []),
{ok, << 0, 48, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),

ok = rocksdb:close(Db),
```