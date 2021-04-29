-module(batch).

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

-define(KEY_1, <<"k1">>).
-define(VAL_1_BIN, <<"v1">>).
-define(VAL_1_IOL, [["v"], <<"1">>]).

-define(KEY_2, <<"k2">>).
-define(VAL_2_BIN, <<"v2">>).
-define(VAL_2_IOL, [["v"], <<"2">>]).

-define(KEY_3, <<"k3">>).
-define(VAL_3_BIN, <<"v3">>).
-define(VAL_3_IOL, [["v"], <<"3">>]).

destroy_reopen(DbName, Options) ->
  _ = rocksdb:destroy(DbName, []),
  {ok, Db} = rocksdb:open(DbName, Options),
  Db.

close_destroy(Db, DbName) ->
  rocksdb:close(Db),
  rocksdb:destroy(DbName, []).

basic_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),

  {ok, Batch} = rocksdb:batch(),

  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_3, ?VAL_3_BIN),
  ?assertEqual(3, rocksdb:batch_count(Batch)),

  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_2, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_3, [])),

  ok = rocksdb:write_batch(Db, Batch, []),

  ?assertEqual({ok, ?VAL_1_BIN}, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual({ok, ?VAL_2_BIN}, rocksdb:get(Db, ?KEY_2, [])),
  ?assertEqual({ok, ?VAL_3_BIN}, rocksdb:get(Db, ?KEY_3, [])),

  ok = rocksdb:release_batch(Batch),

  close_destroy(Db, "test.db"),
  ok.

delete_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),

  {ok, Batch} = rocksdb:batch(),

  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_IOL),
  ok = rocksdb:batch_delete(Batch, ?KEY_2),
  ok = rocksdb:batch_put(Batch, ?KEY_3, ?VAL_3_BIN),
  ?assertEqual(4, rocksdb:batch_count(Batch)),

  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_2, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_3, [])),

  ok = rocksdb:write_batch(Db, Batch, []),

  ?assertEqual({ok, ?VAL_1_BIN}, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_2, [])),
  ?assertEqual({ok, ?VAL_3_BIN}, rocksdb:get(Db, ?KEY_3, [])),

  ok = rocksdb:release_batch(Batch),

  close_destroy(Db, "test.db"),
  ok.

single_delete_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  {ok, Batch} = rocksdb:batch(),
  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:write_batch(Db, Batch, []),
  ?assertEqual({ok, ?VAL_1_BIN}, rocksdb:get(Db, ?KEY_1, [])),
  ok = rocksdb:release_batch(Batch),
  {ok, Batch1} = rocksdb:batch(),
  ok = rocksdb:batch_single_delete(Batch1, ?KEY_1),
  ok = rocksdb:write_batch(Db, Batch1, []),
  ok = rocksdb:release_batch(Batch1),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_1, [])),
  close_destroy(Db, "test.db"),
  ok.

delete_with_notfound_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),

  {ok, Batch} = rocksdb:batch(),

  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_BIN),
  ok = rocksdb:batch_delete(Batch, ?KEY_3),
  ?assertEqual(3, rocksdb:batch_count(Batch)),

  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual(not_found, rocksdb:get(Db, ?KEY_2, [])),

  ok = rocksdb:write_batch(Db, Batch, []),

  ?assertEqual({ok, ?VAL_1_BIN}, rocksdb:get(Db, ?KEY_1, [])),
  ?assertEqual({ok, ?VAL_2_BIN}, rocksdb:get(Db, ?KEY_2, [])),

  ok = rocksdb:release_batch(Batch),

  close_destroy(Db, "test.db"),
  ok.

tolist_test() ->
  {ok, Batch} = rocksdb:batch(),
  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_3, ?VAL_3_BIN),
  ?assertEqual(3, rocksdb:batch_count(Batch)),
  ?assertEqual([{put, ?KEY_1, ?VAL_1_BIN},
                {put, ?KEY_2, ?VAL_2_BIN},
                {put, ?KEY_3, ?VAL_3_BIN}], rocksdb:batch_tolist(Batch)),
  ok = rocksdb:release_batch(Batch),
  ok.

rollback_test() ->
  {ok, Batch} = rocksdb:batch(),
  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_BIN),
  ok = rocksdb:batch_savepoint(Batch),
  ok = rocksdb:batch_put(Batch, ?KEY_3, ?VAL_3_IOL),
  ?assertEqual(3, rocksdb:batch_count(Batch)),
  ?assertEqual([{put, ?KEY_1, ?VAL_1_BIN},
                {put, ?KEY_2, ?VAL_2_BIN},
                {put, ?KEY_3, ?VAL_3_BIN}], rocksdb:batch_tolist(Batch)),
  ok = rocksdb:batch_rollback(Batch),
  ?assertEqual(2, rocksdb:batch_count(Batch)),
  ?assertEqual([{put, ?KEY_1, ?VAL_1_BIN},
                {put, ?KEY_2, ?VAL_2_BIN}], rocksdb:batch_tolist(Batch)),
  ok = rocksdb:release_batch(Batch).


rollback_over_savepoint_test() ->
  {ok, Batch} = rocksdb:batch(),
  ok = rocksdb:batch_put(Batch, ?KEY_1, ?VAL_1_IOL),
  ok = rocksdb:batch_put(Batch, ?KEY_2, ?VAL_2_BIN),
  ok = rocksdb:batch_savepoint(Batch),
  ok = rocksdb:batch_put(Batch, ?KEY_3, ?VAL_3_IOL),
  ?assertEqual(3, rocksdb:batch_count(Batch)),
  ?assertEqual([{put, ?KEY_1, ?VAL_1_BIN},
                {put, ?KEY_2, ?VAL_2_BIN},
                {put, ?KEY_3, ?VAL_3_BIN}], rocksdb:batch_tolist(Batch)),
  ok = rocksdb:batch_rollback(Batch),
  ?assertEqual(2, rocksdb:batch_count(Batch)),
  ?assertEqual([{put, ?KEY_1, ?VAL_1_BIN},
                {put, ?KEY_2, ?VAL_2_BIN}], rocksdb:batch_tolist(Batch)),

  ?assertMatch({error, _}, rocksdb:batch_rollback(Batch)),

  ok = rocksdb:release_batch(Batch).

merge_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}, {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"i">>, term_to_binary(0), []),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),
  {ok, Batch} = rocksdb:batch(),

  ok = rocksdb:batch_merge(Batch, <<"i">>, term_to_binary({int_add, 1})),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),

  ok = rocksdb:batch_merge(Batch, <<"i">>, term_to_binary({int_add, 2})),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),

  ok = rocksdb:batch_merge(Batch, <<"i">>, term_to_binary({int_add, -1})),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),

  ok = rocksdb:write_batch(Db, Batch, []),
  {ok, IBin1} = rocksdb:get(Db, <<"i">>, []),
  2 = binary_to_term(IBin1),

  ok = rocksdb:release_batch(Batch),

  close_destroy(Db, "test.db").
