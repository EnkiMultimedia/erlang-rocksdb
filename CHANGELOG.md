## erlang-rocksdb 2.5.0, released on 2026/01/07

### New Features

- add posting list V2 format with roaring64 bitmaps for fast set operations:
  - New binary format: version byte + serialized roaring bitmap + sorted keys
  - Keys automatically stored in lexicographic (binary memcmp) order
  - Automatic V1 to V2 migration on first merge operation
  - Binary API functions:
    - `posting_list_version/1`: get format version (1 or 2)
    - `posting_list_intersection/2`: AND two posting lists
    - `posting_list_union/2`: OR two posting lists
    - `posting_list_difference/2`: subtract posting lists (A - B)
    - `posting_list_intersection_count/2`: fast cardinality via bitmap
    - `posting_list_bitmap_contains/2`: fast hash-based existence check
    - `posting_list_intersect_all/1`: intersect multiple posting lists
  - CRoaring library integration for efficient bitmap operations

- add postings resource API for fast repeated lookups (Lucene-style naming):
  - Parse once, lookup many times with ~0.1 us/lookup performance
  - `postings_open/1`: parse binary into resource
  - `postings_contains/2`: O(log n) exact lookup
  - `postings_bitmap_contains/2`: O(1) hash lookup
  - `postings_count/1`: get key count
  - `postings_keys/1`: get sorted keys
  - `postings_intersection/2`: AND (accepts binary or resource)
  - `postings_union/2`: OR (accepts binary or resource)
  - `postings_difference/2`: A - B (accepts binary or resource)
  - `postings_intersection_count/2`: fast cardinality
  - `postings_intersect_all/1`: multi-way AND
  - `postings_to_binary/1`: convert resource back to binary

- add enhanced TTL support with column family operations (PR #7, thanks @nyo16):
  - `open_with_ttl_cf/4`: open database with multiple column families, each with its own TTL
  - `get_ttl/2`: get current TTL for a column family
  - `set_ttl/2`: set default TTL for the database
  - `set_ttl/3`: set TTL for a specific column family
  - `create_column_family_with_ttl/4`: create a new column family with a specific TTL
  - See `guides/ttl.md` for documentation

### Bug Fixes

- fix compaction filter for `value_empty` and `always_delete` rules:
  - Use FilterV2 API instead of Filter for direct Decision control
  - Fix 1-tuple rule parsing using strcmp
  - Use `enif_is_identical()` for atom comparison

### Improvements

- posting list keys are now returned in sorted order (lexicographic)
- set operations use roaring bitmap for O(1) existence checks

## erlang-rocksdb 2.4.1, released on 2026/01/04

### Bug Fixes

- fix posting list merge operator not recognizing operands in `batch_merge`:
  - Fixed atom comparison in `ParseOperand` to use `enif_is_identical()` instead of direct `==` comparison
  - Atoms decoded via `enif_binary_to_term` require proper NIF comparison functions
  - Added `posting_list_batch_merge_test` to verify batch operations work correctly

## erlang-rocksdb 2.4.0, released on 2026/01/04

### New Features

- add posting list merge operator for inverted indexes and tagging systems:
  - New merge operator: `posting_list_merge_operator`
  - Merge operations: `{posting_add, Key}` and `{posting_delete, Key}`
  - Tombstones automatically cleaned during merge operations (no compaction filter needed)
  - Binary format: `<<KeyLength:32/big, Flag:8, KeyData:KeyLength/binary>>` - simple, fast to parse
  - Helper functions (pure Erlang):
    - `posting_list_decode/1`: decode binary to list of entries
    - `posting_list_fold/3`: fold over all entries
  - Helper functions (NIF for efficiency):
    - `posting_list_keys/1`: get active keys only (deduped, tombstones filtered)
    - `posting_list_contains/2`: check if key is active
    - `posting_list_find/2`: find key with tombstone status
    - `posting_list_count/1`: count active keys
    - `posting_list_to_map/1`: get full state as map
  - See `guides/posting_lists.md` for documentation

- add Erlang compaction filter support with two modes:
  - **Declarative rules mode** (fast C++ execution):
    - `{key_prefix, Binary}`: delete keys with matching prefix
    - `{key_suffix, Binary}`: delete keys with matching suffix
    - `{key_contains, Binary}`: delete keys containing pattern
    - `{value_empty}`: delete keys with empty values
    - `{value_prefix, Binary}`: delete keys with matching value prefix
    - `{ttl_from_key, Offset, Length, TTLSeconds}`: delete expired keys based on timestamp in key
    - `{always_delete}`: delete all keys (use with caution)
  - **Erlang callback mode** (flexible):
    - Register an Erlang process as compaction filter handler
    - Handler receives `{compaction_filter, BatchRef, Keys}` messages
    - Reply with `rocksdb:compaction_filter_reply(BatchRef, Decisions)`
    - Supports `keep`, `remove`, and `{change_value, NewBinary}` decisions
    - Configurable batch_size and timeout with safe fallback behavior
- add `bottommost_level_compaction` option to `compact_range/4,5`:
  - `skip`: skip bottommost level compaction
  - `if_have_compaction_filter`: compact bottommost only if filter is configured
  - `force`: force compaction filter to run on all data
  - `force_optimized`: force with optimizations

### Example Usage

```erlang
%% Posting list for inverted index
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {merge_operator, posting_list_merge_operator}
]).

%% Add keys to posting list
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_add, <<"doc1">>}, []),
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_add, <<"doc2">>}, []).

%% Delete a key (adds tombstone, cleaned on next read)
ok = rocksdb:merge(Db, <<"term:erlang">>, {posting_delete, <<"doc1">>}, []).

%% Read and process - tombstones automatically removed
{ok, Binary} = rocksdb:get(Db, <<"term:erlang">>, []),
Keys = rocksdb:posting_list_keys(Binary).   %% [<<"doc2">>]
```

```erlang
%% Declarative rules mode - delete temporary and expired keys
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_filter, #{
        rules => [
            {key_prefix, <<"tmp_">>},
            {key_suffix, <<"_expired">>},
            {ttl_from_key, 0, 8, 3600}  % 1 hour TTL from key bytes
        ]
    }}
]).

%% Erlang callback mode - custom filter logic
%% Handler must be a PID of a running process
HandlerPid = spawn(fun filter_loop/0),
{ok, Db} = rocksdb:open("mydb", [
    {compaction_filter, #{
        handler => HandlerPid,  % PID of the handler process
        batch_size => 100,
        timeout => 5000
    }}
]).

%% Force compaction filter to run on all data
ok = rocksdb:compact_range(Db, undefined, undefined, [
    {bottommost_level_compaction, force}
]).
```

### Bug Fixes

- fix compaction filter resource lifetime bug: keep native reference until callback completes
  to prevent use-after-free when handler times out or dies
- fix FreeBSD build: add system include path for malloc_np.h in RocksDB build
- replace std::atomic with mutex-protected bool for better portability

### Improvements

- FreeBSD CI now uses erlang-runtime28 with LLVM 18 for full C++20 support

## erlang-rocksdb 2.3.0, released on 2026/01/03

### New Features

- add SST file support:
  - SST File Writer: `sst_file_writer_open/2,3`, `sst_file_writer_put/3`, `sst_file_writer_merge/3`, `sst_file_writer_delete/2`, `sst_file_writer_delete_range/3`, `sst_file_writer_finish/1,2`
  - Ingest External File: `ingest_external_file/3,4` for loading SST files into database
  - SST File Reader: `sst_file_reader_open/2`, `sst_file_reader_iterator/2`, `sst_file_reader_table_properties/1`, `sst_file_reader_verify_checksum/1`
  - Merge operator support in SST files (counter, erlang, bitset)
- add `sst_file_manager_tracked_files/1` API to get files tracked by SST File Manager
- add transaction batch get operations:
  - Optimistic transactions: `transaction_get_for_update/3,4`, `transaction_multi_get/3,4`, `transaction_multi_get_for_update/3,4`
  - Pessimistic transactions: `pessimistic_transaction_multi_get/3,4`, `pessimistic_transaction_multi_get_for_update/3,4`
- add new read options:
  - `readahead_size`: configure readahead buffer size for sequential reads
  - `async_io`: enable asynchronous I/O for iterators

### Bug Fixes

- fix memory leak in CoalescingIterator error paths
- fix memory leak in TransactionIterator error paths
- fix memory leak in PessimisticTransactionIterator error paths
- add null check for enif_alloc_env in NewBatch

### Improvements

- add FreeBSD CI testing using vmactions/freebsd-vm

### Documentation

- add SST files guide
- update backup guide with correct function names and additional functions
- improve transactions documentation
- add multi_get to getting started guide
- fix documentation links and typos

## erlang-rocksdb 2.2.0, released on 2025/12/30

### New Features

- add `multi_get/2,3,4` API for efficient batch key retrieval
- add extended statistics support with 45+ tickers and 13+ histograms:
  - Block cache statistics: `block_cache_miss`, `block_cache_hit`, `block_cache_bytes_read/write`
  - Read/write operation statistics: `bytes_written/read`, `number_keys_written/read/updated`
  - Compaction statistics: `compact_read/write_bytes`, `num_files_in_single_compaction`
  - Memtable statistics: `memtable_hit/miss`, `write_done_by_self/other`, `wal_synced`
  - Transaction statistics: `txn_overhead_mutex`, `txn_duplicate_key_overhead`
  - Core operation histograms: `db_get`, `db_write`, `db_seek`, `flush_time`, `compaction_time`
  - I/O histograms: `sst_read/write_micros`, `wal_file_sync_micros`, `bytes_per_read/write`
  - Transaction histograms: `num_commit/filter_per_transaction`
- add BlobDB enhancements:
  - `get_blob_statistics/1,2`: BlobDB tickers (blob_bytes_read/written, blob_gc_*)
  - `get_blob_histogram/2,3`: BlobDB histograms (blob_db_get/put/seek_micros)
  - `get_blob_cache_properties/1`: blob cache usage and capacity
  - `allow_unprepared_value` iterator option for lazy blob loading
  - `iterator_prepare_value/1` for explicit blob value loading
  - `get_column_family_metadata/1,2` with blob file information

### Bug Fixes

- fix memory leak in `parse_iterator_options` on error paths
- fix null pointer dereference in `Iterators()` for invalid column family handles
- fix null check for `enif_alloc` in `NewBatch()` preventing crash on OOM
- fix double-free in `Iterators()` when using shared bound slices
- fix exception handling in `Iterators()` to return proper error tuple
- fix typo PREPOLUATE -> PREPOPULATE in blob cache atom

### Documentation

- add statistics guide
- add multi_get usage to getting started guide

## erlang-rocksdb 2.1.0, released on 2025/12/28

- add Pessimistic Transaction support for high-contention workloads:
  - `open_pessimistic_transaction_db/2,3`: open a TransactionDB with row-level locking
  - `pessimistic_transaction/2,3`: begin a new transaction
  - `pessimistic_transaction_put/3,4`: put with lock acquisition
  - `pessimistic_transaction_get/3,4`: read without lock
  - `pessimistic_transaction_get_for_update/3,4`: read with exclusive lock
  - `pessimistic_transaction_delete/2,3`: delete with lock
  - `pessimistic_transaction_iterator/2,3`: create transaction iterator
  - `pessimistic_transaction_commit/1`: commit transaction
  - `pessimistic_transaction_rollback/1`: rollback transaction
  - `release_pessimistic_transaction/1`: release resources
- add Pessimistic Transaction savepoint support:
  - `pessimistic_transaction_set_savepoint/1`: mark a savepoint
  - `pessimistic_transaction_rollback_to_savepoint/1`: rollback to last savepoint
  - `pessimistic_transaction_pop_savepoint/1`: discard savepoint without rollback
- add Pessimistic Transaction introspection:
  - `pessimistic_transaction_get_id/1`: get unique transaction ID
  - `pessimistic_transaction_get_waiting_txns/1`: get lock contention info
- TransactionDB options: lock_timeout, deadlock_detect, max_num_locks, num_stripes
- Transaction options: set_snapshot, deadlock_detect, lock_timeout
- all pessimistic transaction operations use dirty NIFs to prevent blocking the Erlang scheduler

## erlang-rocksdb 2.0.0, released on 2025/12/28

- bump to rocksdb version [10.7.5](https://github.com/facebook/rocksdb/releases/tag/v10.7.5)
- add Wide-Column Entity API for structured data storage:
  - `put_entity/4,5`: store key with multiple named columns
  - `get_entity/3,4`: retrieve entity columns as proplist
  - `iterator_columns/1`: get columns from current iterator position
  - `delete_entity/3,4`: convenience wrapper for delete
- add `coalescing_iterator/3` for efficient multi-column-family iteration
- add `auto_readahead_size` iterator option for automatic prefetch tuning
- add `auto_refresh_iterator_with_snapshot` iterator option
- fix CMake 3.31 compatibility
- fix bundled snappy linking on Linux (RTTI mismatch)
- fix compiler warnings

## erlang-rocksdb 1.9.0, released on 2025/02/01

- bump to rocksdb version [9.10.0]((https://github.com/facebook/rocksdb/releases/tag/v9.10.0)
- bump to snappy 1.2.1
- fix write_options
- fix memory leak
- handle manual_wal_flush option


## erlang-rocksdb 1.8.0, released on 2022/11/04

- bring compatibility with with Apple Mac M1 and Erlang OTP 25 
- bump to rocksdb version [7.7.3]((https://github.com/facebook/rocksdb/releases/tag/v7.7.3)
- add support for BLOB databases
- add `rocksdb:transaction_rollback/1` function
- add `rocksdb:release_transaction/1` function
- fix: bind transactions OPs that require it to IO threads.
- fix transaction iterator
- fix transaction leaks: ensure the resource is always closed
- fix specs

** BREAKING CHANGES **

- rocksdb:transction_commit doesn't delete the transaction
- `rocksdb_transaction:iterators`: database handle is removed from the functions parameter. Instead only use the
transaction resource.


## erlang-rocksdb 1.7.0, released on 2021/11/03

- bring compatibility with with Apple Mac M1 and Erlang OTP 24
- bump to rocksdb version [6.25.3]((https://github.com/facebook/rocksdb/releases/tag/v6.25.3)
- add `rocksdb:open_readonly/3` function to open databases in read only mode (useful for concurrency)
- add support for read options `read_tier`
- add support for direct IO  options `use_direct_reads` & `use_direct_io_for_flush_and_compaction`
- new staistics API, exposing `rocksdb::Statistics` object.
- fix list merge operaion when empty
- add `unordered_write` and & `two_write_queues` open options
- fix `transaction_get`  when Options is unset.
- fix iterator behaviour

### REAKING CHANGES

- return an invalid iterator if the user try to use next or prev without
having set the initial location of the iterator:
  - calling next, and prev without having put the iterator at a specific
  key or location (first, last) will now return the tuple `{error, invalid_iterator}`
  - fix crash when moving to next key on a new iterator.
  - fix behavior of Refresh. Refresh is supposed to reset completely the
  state of the iterator. Fix test accordingly.

- `transction_get/2` has been removed, and is replaced by `transction_get/3` with the third argument to spass the options.
- `transaction_get/3` to get using a column family is now `transaction_get/4`



## erlang-rocksdb 1.6.0, released on 2020/10/16

- bump to rocksdb version [6.13.3](https://github.com/facebook/rocksdb/releases/tag/v6.13.3)
- improve compression support: add comprssion_opts and bottomost_compression support
- fix memory leaks: RateLimiter, Snapshots, BitsetMergeOperator, Iterators, WriteBinaryUpdayte
- fix build for OTP 23

## erlang-rocksdb 1.5.1, released on 2020/02/09

- fix memory leaks in erocksdb::WriteBinaryUpdate, erokcsdb::CompactRange & erocksdb::Repair
- fix: use code:lib_dir/1 to find correct ei dir

## erlang-rocksdb 1.5.0, released on 2020/01/21

- Fix lz4 and snappy lib install dir to work on systems using lib64
- Add `atomic_flush` db option
- bump rocksdb to version [6.5.2](https://github.com/facebook/rocksdb/releases/tag/v6.5.2)

## erlang-rocksdb 1.4.0, released on 2019/11/04

- fix build on OpenBSD
- bump rocksdb to version [6.4.6](https://github.com/facebook/rocksdb/releases/tag/v6.4.6)


## erlang-rocksdb 1.3.2, released on 2019/09/13

- fix build using installed rocksdb on the system

## erlang-rocksdb 1.3.1, released on 2019/08/28

- fix cache_info segfault

## erlang-rocksdb 1.3.0, released on 2019/08/26

- bump rocksdb to version [6.2.2](https://github.com/facebook/rocksdb/releases/tag/v6.2.2)
- new: support of the windows platform (build using MSVC)
- fix: transaction log iterator

## erlang-rocksdb 1.2.0, released on 2019/06/11

- bump rocksdb to version [6.1.2](https://github.com/facebook/rocksdb/releases/tag/v6.1.2)
- use C++11 atomics to replace pthreads and GCC specific threading and atomics
- fix: parralelism build options
- fix: miscellaneous dialyze and C warnings

## erlang-rocksdb 1.1.1, released on 2019/04/12

- fix spec

erlang-rocksdb 1.1.0, released on 2019/03/26

- bump rocksdb to version [5.18.3](https://github.com/facebook/rocksdb/releases/tag/v5.18.3)
- add preliminary support for optimistic transactions
- make build more parallel when cores are available
- fix iterate_upper_bound type.

## erlang-rocksdb 1.0.0, released on 2019/01/31


- first stable version
- miscellaneous build improvements with [new build options and optimisations](https://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/customize_rocksdb_build.md)
- stable API (refactored in #87, #88)

## erlang-rocksdb 0.26.2, released on 2019/01/24

- fix `batch_merge/3` to call the right function

## erlang-rocksdb 0.26.1, released on 2018/12/11

- fix build on freebsd
- support build with the Thread Building Blocks library (TBB)

## erlang-rocksdb 0.26.0, released on 2018/12/05

- allows [customized builds](http://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/customize_rocksdb_build.md)
- fix compilations warnings

### BREAKING CHANGES

- build is now using cmake instead of autotools
- corruption errors return `{error, {corruption, Reaason :: string()}}` inst#ead of `corruption`.

## erlang-rocksdb 0.25.0, released on 2018/11/21

- bump to rocksdb 0.17.2
- bump snappy to 1.1.7
- bump lz4  to 1.8.3
- add `rocksdb:open_with_ttl/2`.
- add `no_slowdown` and `low_pri` write options
- add `manual_wal_flush`  db option
- add support for WriteBuffer (#64)
- add `sst_file_manger()` resource to provide SSTFileManager support and manage disk space. (#74)
- fix `aproximate_sizes_test` and `approximate_memtable_stats_test`

### BREAKING CHANGES

- handle flush options in rocksdb:flush/{2, 3}
  With this change `rocksdb:flush/1` has been removed while second parameter
  of `rocksdb:flush/2` are now `FlushOptions::flush_options()`.
  Use `rocksdb:flush/3` to flush a column family
- change signature of `rocksdb:get_approximate_memtable_stats/{2,3}`
  to `rocksdb:get_approximate_memtable_stats/4` .


## erlang-rocksdb 0.24.0, released on 2018/11/06

- add `rocksdb:get_approximate_sizes/{3,4}`: amroximate the size of a range of keys
- add `rocksdb:get_approximate_memtable_stats/{2,3}` : aproximate the size and the 
  number of keys in a memtable
- add `rocksdb:compact_range/{4,5}`: Compact the underlying storage for a key


## erlang-rocksdb 0.23.3, released on 2018/10/31

- fix possible memory leaks when trying to open a bad CF resource.


## erlang-rocksdb 0.23.2, released on 2018/10/30

- fix memory leaks


## erlang-rocksdb 0.23.1, released on 2018/10/08

- fix bitset merge operator under load


## erlang-rocksdb 0.23.0, released on 2018/10/05

- add bytewise and reverse bytewise comparators
- add iterate_upper_bound, iterate_lower_bound_options and prefix_same_as_start to iterator options
- improve: prevernt garbage collection in batch
- fix bitset merge operator, handle exceptions
- fix counter merge operator, handle exceptions


## erlang-rocksdb 0.22.0, released on 2018/09/25

- fix NIF memory leak in `erocksdb::Get`
- add counter_merge_operator


## erlang-rocksdb 0.21.0, released on 2018/09/16

- bump to rocksdb 5.15.10
- add: erlang merge operator
- add: erlang bitset merge operator
- add: rocksdb:batch_data_size/1
- add: prefix_transform option


## erlang-rocksdb 0.20.0, released on 2018/08/03

- bump to rocksdb 5.14.2


## erlang-rocksdb 0.19.0, released on 2018/07/04

- bump to rocksdb 5.13.4
- add `rocksdb:set_strict_capacity_limit/2` for cache
- add `optimize_filters_for_hits` database option
- add `new_table_reader_for_compaction_inputs` database option
- add `max_subcompactions` database option


## erlang-rocksdb 0.18.0, released on 2018/05/03

- bump to rocksdb 5.12.4


## erlang-rocksdb 0.17.0, released on 2018/03/25

- bump ro rocksdb 5.11.3
- improve build to make it faster, only use makefiles
- optimise single operations Get/Put/Delete/DeleteSing: don't call batch
- optimise write: use batch and do less operations in the the nif: make scheduler happy
- fix leak in batch
- breaking change: rename `close_batch/1`  in `relase_batch/1`


## erlang-rocksdb 0.16.0, released on 2018/03/13

- add single_delete operation (to key/value and batch api)
- add iterator_refresh function
- add support for level_compaction_dynamic_level_bytes for column family options
- add rate_limiter support
- many optimisations to the source code and build
- bump to rocksdb 5.10.4


## erlang-rocksdb 0.15.0, released on 2018/02/10

- add support for lz4 compression
- add new database option `max_background_jobs`
- fix: potential memory leak


## erlang-rocksdb 0.14.0, released on 2018/01/31

- bump to rocksdb 5.7.5
- bump to snappy 1.1.4


## erlang-rocksdb 0.13.1, released on 2017/09/07

- fix: remove memory leaks spotted with valgrind

## erlang-rocksdb 0.13.0, released on 2017/09/06

* add new cache api

	-  new_lru_cache/1,
	-  new_clock_cache/1,
	-  get_usage/1,
	-  get_pinned_usage/1,
	-  set_capacity/2,
	-  get_capacity/1
	-  release_cache/1

* add new env api

	- default_env/0,
	- mem_env/0,
	- set_env_background_threads/{2, 3},
	- destroy_env/1

* remove default shared block cache in private env to reduce the vm memory usage.


## erlang-rocksdb 0.12.0, released on 2017/09/05

- add get_block_cache_usage/1
- add block_cache_capacity/{1, 0}
- add new db option {env, Env} where Env can be `default` or `memenv`.
- add new db option {db_write_buffer_size, INT}
- fix: cache usahe, correctly reuse the cache among instances
- fix: options parsing, reuse code where we can and correctly
handle db optinos

### BREAKING CHANGE

The following cache functions has been removed:

-  new_lru_cache/1,
-  new_clock_cache/1,
-  get_usage/1,
-  get_pinned_usage/1,
-  set_capacity/2,
-  get_capacity/1

instead use the new block_cache function to change the
size of the shared cache. A new api to setup different caches
will come in the next version.

The following functions has been removed:

- default_env/0,
- mem_env/0,
- set_background_threads/2, set_background_threads/3,
- destroy_env/1]).

For now it's not possible to create a shared environment. also
the {in_memory, true} setting has been removed.

To set a memory environement use the option {env, memenv}.


## erlang-rocksdb 0.11.0, released on 2017/07/29

- add rocksdb:get_snapshot_sequence/1


## erlang-rocksdb 0.10.0, released on 2017/07/28

- bump to rocksdb 5.7.2
- add: rocksdb:delete_range/{4,5}
- add: support for the backup api
- add: rocksdb:destroy_column_family/1
- proper fix to handle an iterator with column family issue


## erlang-rocksdb 0.9.1, released on 2017/07/26

- fix iterator with column familly issue
- fix mutex usage igit push --tagsn the db object


## erlang-rocksdb 0.9.0, released on 2017/05/31

- bump to rocksdb 5.4.5
- remove `disable_data_sync` db option, optimisations removed in rocksdb
- remove `timeout_hint_us` db option, was useless since 2 years
- remove `verify_checksums_in_compaction` db option. it's always done.
- remove `skip_table_builder_flush` db option
- remove `allow_os_buffer` db option
- add `{seek_for_prev, Prev}` db option
- allows to fold a column familly


## erlang-rocksdb 0.8.2, released on 2017/05/17

- fix: fix default_env/0 & mem_env/0 spec


## erlang-rocksdb 0.8.1, released on 2017/05/17

- fix: use an uint64 to set the capacity of LRU or CLOCK caches.


## erlang-rocksdb 0.8.0, released on 2017/05/17

- replace custom pool by dirty-nif calls
- allows a cache to be shared between databases (option `block_cache` from the block options)
- allows an environment to be set and shared across databases (option `env`)
- add support of batch operations, allows you to incrementatly update it in memory
- add support for transaction log operations (iteration and replication)

