# Encrypted Env

An encrypted env encrypts all of a database's on-disk data transparently, using AES-256 in CTR mode through RocksDB's encryption provider. You read and write keys and values as usual; the bytes that reach the filesystem (SST files, WAL, MANIFEST) are ciphertext.

Use it when you need data-at-rest encryption and you control a 32-byte key. It does not encrypt data in memory, and it is not authenticated (see Notes).

## Create an env and open a database

The key must be a 32-byte binary (AES-256). Pass the returned handle as `{env, Env}` in the open options.

```erlang
Key = <<"0123456789abcdef0123456789abcdef">>,  %% exactly 32 bytes
{ok, Env} = rocksdb:new_env({encrypted, Key}),
{ok, Db} = rocksdb:open("/tmp/erocksdb", [{create_if_missing, true}, {env, Env}]),
ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
{ok, <<"v">>} = rocksdb:get(Db, <<"k">>, []),
ok = rocksdb:close(Db).
```

The map form is equivalent:

```erlang
{ok, Env} = rocksdb:new_env(#{encrypted => Key}).
```

## Reopen

Reopen the same directory with an env built from the same key. Reuse the env handle, or create a new one from the same key.

```erlang
{ok, Env} = rocksdb:new_env({encrypted, Key}),
{ok, Db} = rocksdb:open("/tmp/erocksdb", [{env, Env}]).
```

## Notes

- The key must be exactly 32 bytes. Any other length raises `badarg`.
- Wrong-key detection is checksum-based, not authenticated. CTR decryption with the wrong key produces garbage that RocksDB's per-block checksums reject as a corruption error. There is no MAC, so a deliberately tampered file is not guaranteed to be detected.
- Keep the key; data written with one key cannot be recovered with another.
- The encrypted env requires OpenSSL (libcrypto), which is a build dependency of the NIF.
- Encryption covers on-disk data only. Keys and values in memory are not encrypted.
