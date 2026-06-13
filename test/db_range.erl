%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017-2020, Benoit Chesneau
%%% @doc
%%%
%%% @end
%%% Created : 28. Aug 2017 20:39
%%%-------------------------------------------------------------------
-module(db_range).
-author("benoitc").

%% API
-include_lib("eunit/include/eunit.hrl").


delete_range_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"1">>, []),
    rocksdb:put(Ref, <<"b">>, <<"2">>, []),
    rocksdb:put(Ref, <<"c">>, <<"3">>, []),
    rocksdb:put(Ref, <<"d">>, <<"4">>, []),
    rocksdb:put(Ref, <<"e">>, <<"5">>, []),

    ok = rocksdb:delete_range(Ref, <<"b">>, <<"e">>, []),

    [<<"a">>, <<"e">>] = lists:reverse(
      rocksdb_test_util:fold_keys(
        Ref,
        fun(K, Acc) -> [K | Acc] end,
        [],
        []
      )
    )
  after
    rocksdb:close(Ref),
    rocksdb:destroy("ltest", []),
    rocksdb_test_util:rm_rf("ltest")
  end.

cf_delete_range_test() ->
  rocksdb_test_util:rm_rf("ltest"), %% NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  {ok, TestH} = rocksdb:create_column_family(Ref, "test", []),
  try
    rocksdb:put(Ref, TestH, <<"a">>, <<"1">>, []),
    rocksdb:put(Ref, TestH, <<"b">>, <<"2">>, []),
    rocksdb:put(Ref, TestH, <<"c">>, <<"3">>, []),
    rocksdb:put(Ref, TestH, <<"d">>, <<"4">>, []),
    rocksdb:put(Ref, TestH, <<"e">>, <<"5">>, []),

    ok = rocksdb:delete_range(Ref, TestH, <<"b">>, <<"e">>, []),

    [<<"a">>, <<"e">>] = lists:reverse(
      rocksdb_test_util:fold_keys(
        Ref,
        TestH,
        fun(K, Acc) -> [K | Acc] end,
        [],
        []
      )
    )
  after
    rocksdb:close(Ref),
    rocksdb:destroy("ltest", []),
    rocksdb_test_util:rm_rf("ltest")

  end.

