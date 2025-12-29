-module(blob_db).

-export([basic_test/0
        ,cache_test/0
        ,cache_properties_test/0]).
-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).


basic_test() ->
  ?rm_rf("test_blobdb"),
  {ok, Db} = 
    rocksdb:open(
      "test_blobdb", 
      [{create_if_missing, true}
      ,{enable_blob_files, true}
      ,{min_blob_size, 0}
      ,{blob_garbage_collection_age_cutoff, 0.25}]
    ),
  try
    ok = rocksdb:put(Db, <<"key">>, <<"blob_value">>, []),
    ok = rocksdb:flush(Db, []),
    {ok, <<"blob_value">>} = rocksdb:get(Db, <<"key">>, [])
  after
    ok = rocksdb:close(Db),
    ?rm_rf("test_blobdb")
  end,
  ok.

cache_test() ->
  ?rm_rf("test_cacheblobdb"),
  {ok, CHandle} = rocksdb:new_cache(lru, 2097152),
  {ok, Db} =
    rocksdb:open(
      "test_cacheblobdb",
      [{create_if_missing, true}
      ,{enable_blob_files, true}
      ,{blob_cache, CHandle}]
    ),
  try
    ok = rocksdb:put(Db, <<"key">>, <<"blob_value">>, []),
    ok = rocksdb:flush(Db, []),
    {ok, <<"blob_value">>} = rocksdb:get(Db, <<"key">>, [])
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_cache(CHandle),
    ?rm_rf("test_cacheblobdb")
  end,
  ok.

cache_properties_test() ->
  ?rm_rf("test_blob_cache_props"),
  CacheSize = 2097152,  %% 2MB
  {ok, BlobCache} = rocksdb:new_cache(lru, CacheSize),
  {ok, Db} =
    rocksdb:open(
      "test_blob_cache_props",
      [{create_if_missing, true}
      ,{enable_blob_files, true}
      ,{min_blob_size, 0}  %% All values go to blob files
      ,{blob_cache, BlobCache}
      ,{prepopulate_blob_cache, flush_only}]
    ),
  try
    %% Test blob cache capacity property
    {ok, CapacityBin} = rocksdb:get_property(Db, <<"rocksdb.blob-cache-capacity">>),
    Capacity = binary_to_integer(CapacityBin),
    true = Capacity =:= CacheSize,

    %% Initially usage should be minimal (just overhead)
    {ok, UsageBin1} = rocksdb:get_property(Db, <<"rocksdb.blob-cache-usage">>),
    Usage1 = binary_to_integer(UsageBin1),
    true = is_integer(Usage1),

    %% Initially pinned usage should be 0 or minimal
    {ok, PinnedBin1} = rocksdb:get_property(Db, <<"rocksdb.blob-cache-pinned-usage">>),
    Pinned1 = binary_to_integer(PinnedBin1),
    true = is_integer(Pinned1),

    %% Write some data to populate the blob cache
    Value = list_to_binary(lists:duplicate(100, $a)),
    ok = rocksdb:put(Db, <<"key1">>, Value, []),
    ok = rocksdb:put(Db, <<"key2">>, Value, []),
    ok = rocksdb:flush(Db, []),

    %% After flush with prepopulate_blob_cache=flush_only, cache should have data
    {ok, UsageBin2} = rocksdb:get_property(Db, <<"rocksdb.blob-cache-usage">>),
    Usage2 = binary_to_integer(UsageBin2),
    true = Usage2 >= Usage1,  %% Usage should have increased or stayed same

    %% Verify capacity hasn't changed
    {ok, CapacityBin2} = rocksdb:get_property(Db, <<"rocksdb.blob-cache-capacity">>),
    Capacity2 = binary_to_integer(CapacityBin2),
    true = Capacity2 =:= CacheSize
  after
    ok = rocksdb:close(Db),
    ok = rocksdb:release_cache(BlobCache),
    ?rm_rf("test_blob_cache_props")
  end,
  ok.
