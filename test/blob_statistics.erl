-module(blob_statistics).

-export([ticker_test/0]).

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Value size larger than min_blob_size to ensure blobs are written
-define(VALUE_SIZE, 100).

ticker_test() ->
  ?rm_rf("test_blob_stats"),
  {ok, Stats} = rocksdb:new_statistics(),
  %% Create a blob cache for cache statistics
  {ok, BlobCache} = rocksdb:new_cache(lru, 1048576),
  {ok, Db} =
    rocksdb:open(
      "test_blob_stats",
      [{create_if_missing, true}
      ,{enable_blob_files, true}
      ,{min_blob_size, 0}  %% All values go to blob files
      ,{blob_cache, BlobCache}
      ,{prepopulate_blob_cache, flush_only}  %% Populate cache on flush
      ,{statistics, Stats}]
    ),
  try
    %% Check initial blob file bytes written is 0
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_blob_file_bytes_written),

    %% Write some data (large enough to be stored as blobs)
    Value1 = list_to_binary(lists:duplicate(?VALUE_SIZE, $a)),
    Value2 = list_to_binary(lists:duplicate(?VALUE_SIZE, $b)),
    ok = rocksdb:put(Db, <<"key1">>, Value1, []),
    ok = rocksdb:put(Db, <<"key2">>, Value2, []),
    ok = rocksdb:flush(Db, []),

    %% Check blob file bytes written increased
    {ok, BlobBytesWritten} = rocksdb:statistics_ticker(Stats, blob_db_blob_file_bytes_written),
    true = BlobBytesWritten > 0,

    %% With prepopulate_blob_cache=flush_only, blobs should be in cache after flush
    {ok, CacheAdd} = rocksdb:statistics_ticker(Stats, blob_db_cache_add),
    true = CacheAdd >= 2,  %% 2 blobs added to cache

    %% Read data - should hit cache
    {ok, Value1} = rocksdb:get(Db, <<"key1">>, []),
    {ok, Value2} = rocksdb:get(Db, <<"key2">>, []),

    %% Check cache hit count
    {ok, CacheHit} = rocksdb:statistics_ticker(Stats, blob_db_cache_hit),
    true = CacheHit >= 2,

    %% Check cache bytes read
    {ok, CacheBytesRead} = rocksdb:statistics_ticker(Stats, blob_db_cache_bytes_read),
    true = CacheBytesRead > 0,

    %% Check cache miss should be 0 or minimal (since we prepopulated)
    {ok, CacheMiss} = rocksdb:statistics_ticker(Stats, blob_db_cache_miss),
    true = CacheMiss =:= 0,

    %% Test other tickers can be read (they may be 0 for integrated BlobDB)
    {ok, _BlobFileBytesRead} = rocksdb:statistics_ticker(Stats, blob_db_blob_file_bytes_read),
    {ok, _BlobFileSynced} = rocksdb:statistics_ticker(Stats, blob_db_blob_file_synced),

    %% GC tickers (will be 0 since no GC occurred)
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_gc_num_files),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_gc_num_new_files),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_gc_failures),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_gc_num_keys_relocated),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_gc_bytes_relocated),

    %% Test cache failure ticker (should be 0)
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_cache_add_failures),

    %% Legacy BlobDB tickers (not used by integrated BlobDB, but should return 0)
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_put),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_get),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_write),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_multiget),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_seek),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_next),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_prev),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_keys_written),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_num_keys_read),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_bytes_written),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_bytes_read),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_write_inlined),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_write_inlined_ttl),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_write_blob),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_write_blob_ttl),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_blob_index_expired_count),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_blob_index_expired_size),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_blob_index_evicted_count),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_blob_index_evicted_size),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_fifo_num_files_evicted),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_fifo_num_keys_evicted),
    {ok, 0} = rocksdb:statistics_ticker(Stats, blob_db_fifo_bytes_evicted)
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_statistics(Stats),
    ok = rocksdb:release_cache(BlobCache),
    ?rm_rf("test_blob_stats")
  end,
  ok.
