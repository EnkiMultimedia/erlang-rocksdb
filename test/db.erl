-module(db).

-include_lib("eunit/include/eunit.hrl").


open_test() -> [{open_test_Z(), l} || l <- lists:seq(1, 20)].
open_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.open.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.open.test", [{create_if_missing, true}]),
  true = rocksdb:is_empty(Ref),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  false = rocksdb:is_empty(Ref),
  {ok, <<"123">>} = rocksdb:get(Ref, <<"abc">>, []),
  {ok, 1} = rocksdb:count(Ref),
  not_found = rocksdb:get(Ref, <<"def">>, []),
  ok = rocksdb:delete(Ref, <<"abc">>, []),
  not_found = rocksdb:get(Ref, <<"abc">>, []),
  true = rocksdb:is_empty(Ref).

fold_test() -> [{fold_test_Z(), l} || l <- lists:seq(1, 20)].
fold_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.fold.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.fold.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  ok = rocksdb:put(Ref, <<"hij">>, <<"789">>, []),
  [{<<"abc">>, <<"123">>},
  {<<"def">>, <<"456">>},
  {<<"hij">>, <<"789">>}] = lists:reverse(rocksdb:fold(Ref,
  fun({K, V}, Acc) ->
    [{K, V} | Acc]
  end,
  [], [])).

fold_keys_test() -> [{fold_keys_test_Z(), l} || l <- lists:seq(1, 20)].
fold_keys_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.fold.keys.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.fold.keys.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  ok = rocksdb:put(Ref, <<"abc">>, <<"123">>, []),
  ok = rocksdb:put(Ref, <<"hij">>, <<"789">>, []),
  [<<"abc">>, <<"def">>, <<"hij">>] = lists:reverse(rocksdb:fold_keys(Ref,
  fun(K, Acc) -> [K | Acc] end,
  [], [])).

destroy_test() -> [{destroy_test_Z(), l} || l <- lists:seq(1, 20)].
destroy_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.destroy.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.destroy.test", [{create_if_missing, true}]),
  ok = rocksdb:put(Ref, <<"def">>, <<"456">>, []),
  {ok, <<"456">>} = rocksdb:get(Ref, <<"def">>, []),
  rocksdb:close(Ref),
  ok = rocksdb:destroy("/tmp/erocksdb.destroy.test", []),
  {error, {db_open, _}} = rocksdb:open("/tmp/erocksdb.destroy.test", [{error_if_exists, true}]).

compression_test() -> [{compression_test_Z(), l} || l <- lists:seq(1, 20)].
compression_test_Z() ->
  CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
  os:cmd("rm -rf /tmp/erocksdb.compress.0 /tmp/erocksdb.compress.1"),
  {ok, Ref0} = rocksdb:open("/tmp/erocksdb.compress.0", [{create_if_missing, true}, {compression, none}]),
  [ok = rocksdb:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  {ok, Ref1} = rocksdb:open("/tmp/erocksdb.compress.1", [{create_if_missing, true}, {compression, snappy}]),
  [ok = rocksdb:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  %% Check both of the LOG files created to see if the compression option was correctly
  %% passed down
  MatchCompressOption =
  fun(File, Expected) ->
    {ok, Contents} = file:read_file(File),
    case re:run(Contents, "Options.compression: " ++ Expected) of
    {match, _} -> match;
    nomatch -> nomatch
    end
  end,
  Log0Option = MatchCompressOption("/tmp/erocksdb.compress.0/LOG", "0"),
  Log1Option = MatchCompressOption("/tmp/erocksdb.compress.1/LOG", "1"),
  ?assert(Log0Option =:= match andalso Log1Option =:= match).

close_test() -> [{close_test_Z(), l} || l <- lists:seq(1, 20)].
close_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.close.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.close.test", [{create_if_missing, true}], []),
  ?assertEqual(ok, rocksdb:close(Ref)),
  ?assertEqual({error, einval}, rocksdb:close(Ref)).

close_fold_test() -> [{close_fold_test_Z(), l} || l <- lists:seq(1, 20)].
close_fold_test_Z() ->
  os:cmd("rm -rf /tmp/erocksdb.close_fold.test"),
  {ok, Ref} = rocksdb:open("/tmp/erocksdb.close_fold.test", [{create_if_missing, true}], []),
  ok = rocksdb:put(Ref, <<"k">>,<<"v">>,[]),
  ?assertException(throw, {iterator_closed, ok}, % ok is returned by close as the acc
  rocksdb:fold(Ref, fun(_,_A) -> rocksdb:close(Ref) end, undefined, [])).

destroy_reopen(DbName, Options) ->
  _ = rocksdb:destroy(DbName, []),
  _ = os:cmd("rm -rf " ++ DbName),
  {ok, Db} = rocksdb:open(DbName, Options),
  Db.

randomstring(Len) ->
  list_to_binary([rand:uniform(95) || _I <- lists:seq(0, Len - 1)]).

key(I) when is_integer(I) ->
  <<I:128/unsigned>>.

approximate_size_test() ->
  Db = destroy_reopen("erocksdb_approximate_size.db",
                      [{create_if_missing, true},
                       {write_buffer_size, 100000000},
                       {compression, none}]),
  try
    N = 128,
    rand:seed_s(exsplus),
    lists:foreach(fun(I) ->
                      ok = rocksdb:put(Db, key(I), randomstring(1024), [])
                  end, lists:seq(0, N)),
    Start = key(50),
    End = key(60),
    Size = rocksdb:get_approximate_size(Db, Start, End, true),
    ?assert(Size >= 6000),
    ?assert(Size =< 204800),
    Size2 = rocksdb:get_approximate_size(Db, Start, End, false),
    ?assertEqual(0, Size2),
    Start2 = key(500),
    End2 = key(600),
    Size3 = rocksdb:get_approximate_size(Db, Start2, End2, true),
    ?assertEqual(0, Size3),
    lists:foreach(fun(I) ->
                      ok = rocksdb:put(Db, key(I+1000), randomstring(1024), [])
                  end, lists:seq(0, N)),
    Size4 = rocksdb:get_approximate_size(Db, Start2, End2, true),
    ?assertEqual(0, Size4),
    Start3 = key(1000),
    End3 = key(1020),
    Size5 = rocksdb:get_approximate_size(Db, Start3, End3, true),
    ?assert(Size5 >= 6000)
  after
    rocksdb:close(Db)
  end.
