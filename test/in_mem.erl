%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2016, Benoit Chesneau
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2016 21:49
%%%-------------------------------------------------------------------
-module(in_mem).
-author("benoitc").

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").


basic_test() ->
  {ok, MemEnv} = rocksdb:mem_env(),
  {ok, Db} = rocksdb:open(MemEnv, "test", [{create_if_missing, true}]),
  ok = rocksdb:put(Db, <<"a">>, <<"1">>, []),
  ?assertEqual({ok, <<"1">>}, rocksdb:get(Db, <<"a">>, [])),
  {ok, Db1} = rocksdb:open(MemEnv, "test1", [{create_if_missing, true}]),
  ok = rocksdb:put(Db1, <<"a">>, <<"2">>, []),
  ?assertEqual({ok, <<"1">>}, rocksdb:get(Db, <<"a">>, [])),
  ?assertEqual({ok, <<"2">>}, rocksdb:get(Db1, <<"a">>, [])),
  ok = rocksdb:close(Db),
  ok = rocksdb:close(Db1),
  ok.

prev_test() ->
  {ok, MemEnv} = rocksdb:mem_env(),
  os:cmd("rm -rf ltest"),  % NOTE
  {ok, Ref} = rocksdb:open(MemEnv, "ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, <<"b">>, <<"y">>, []),
    {ok, I} = rocksdb:iterator(Ref, []),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, prev)),
    ?assertEqual(ok, rocksdb:iterator_close(I))
  after
    rocksdb:close(Ref)
  end.
