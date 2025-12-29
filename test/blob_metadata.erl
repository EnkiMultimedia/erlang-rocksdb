-module(blob_metadata).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

basic_test() ->
  ?rm_rf("test_blob_meta"),
  {ok, Db} =
    rocksdb:open(
      "test_blob_meta",
      [{create_if_missing, true}
      ,{enable_blob_files, true}
      ,{min_blob_size, 0}]),  %% All values go to blob files
  try
    %% Write some data and flush to create blob files
    Value = list_to_binary(lists:duplicate(100, $a)),
    ok = rocksdb:put(Db, <<"key1">>, Value, []),
    ok = rocksdb:put(Db, <<"key2">>, Value, []),
    ok = rocksdb:flush(Db, []),

    %% Get column family metadata
    {ok, Meta} = rocksdb:get_column_family_metadata(Db),

    %% Verify metadata structure
    true = is_map(Meta),
    #{size := Size, file_count := FileCount, name := Name,
      blob_file_size := BlobFileSize, blob_files := BlobFiles} = Meta,

    true = is_integer(Size),
    true = is_integer(FileCount),
    true = is_binary(Name),
    ?assertEqual(<<"default">>, Name),
    true = is_integer(BlobFileSize),
    true = BlobFileSize > 0,  %% Should have blob data
    true = is_list(BlobFiles),
    true = length(BlobFiles) >= 1,  %% At least one blob file

    %% Verify blob file metadata structure
    [FirstBlob | _] = BlobFiles,
    true = is_map(FirstBlob),
    #{blob_file_number := BlobNum, blob_file_name := BlobName,
      blob_file_path := BlobPath, size := BlobSize,
      total_blob_count := BlobCount, total_blob_bytes := BlobBytes,
      garbage_blob_count := GarbageCount, garbage_blob_bytes := GarbageBytes} = FirstBlob,

    true = is_integer(BlobNum),
    true = is_binary(BlobName),
    true = is_binary(BlobPath),
    true = is_integer(BlobSize),
    true = BlobSize > 0,
    true = is_integer(BlobCount),
    true = BlobCount >= 2,  %% We wrote 2 keys
    true = is_integer(BlobBytes),
    true = BlobBytes > 0,
    true = is_integer(GarbageCount),
    true = is_integer(GarbageBytes)
  after
    ok = rocksdb:close(Db),
    ?rm_rf("test_blob_meta")
  end,
  ok.

column_family_test() ->
  ?rm_rf("test_blob_meta_cf"),
  {ok, Db, [DefaultH]} =
    rocksdb:open_with_cf(
      "test_blob_meta_cf",
      [{create_if_missing, true}],
      [{"default", [{enable_blob_files, true}, {min_blob_size, 0}]}]),
  {ok, TestH} = rocksdb:create_column_family(Db, "test",
    [{enable_blob_files, true}, {min_blob_size, 0}]),
  try
    %% Write to different column families
    Value = list_to_binary(lists:duplicate(100, $x)),
    ok = rocksdb:put(Db, DefaultH, <<"key1">>, Value, []),
    ok = rocksdb:put(Db, TestH, <<"key2">>, Value, []),
    ok = rocksdb:flush(Db, []),

    %% Get metadata for default column family
    {ok, DefaultMeta} = rocksdb:get_column_family_metadata(Db, DefaultH),
    #{name := DefaultName} = DefaultMeta,
    ?assertEqual(<<"default">>, DefaultName),

    %% Get metadata for test column family
    {ok, TestMeta} = rocksdb:get_column_family_metadata(Db, TestH),
    #{name := TestName} = TestMeta,
    ?assertEqual(<<"test">>, TestName)
  after
    ok = rocksdb:close(Db),
    ?rm_rf("test_blob_meta_cf")
  end,
  ok.
