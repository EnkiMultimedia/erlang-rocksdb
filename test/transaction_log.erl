%% Copyright (c) 2016-2020 Benoît Chesneau.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

-module(transaction_log).
-author("benoitc").

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

destroy_reopen(DbName, Options) ->
  rocksdb_test_util:rm_rf(DbName),
  {ok, Db} = rocksdb:open(DbName, Options),
  Db.

close_destroy(Db, DbName) ->
  ok = rocksdb:close(Db),
  timer:sleep(500),
  ok = rocksdb:destroy(DbName, []),
  timer:sleep(500),
  ok.

basic_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  Db1 = destroy_reopen("test1.db", [{create_if_missing, true}]),

  ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
  ?assertEqual(1, rocksdb:get_latest_sequence_number(Db)),
  ?assertEqual(0, rocksdb:get_latest_sequence_number(Db1)),

  {ok, Itr} = rocksdb:tlog_iterator(Db, 0),
  {ok, Last, TransactionBin} = rocksdb:tlog_next_binary_update(Itr),
  ?assertEqual(1, Last),

  ?assertEqual(not_found, rocksdb:get(Db1, <<"a">>, [])),
  ok = rocksdb:write_binary_update(Db1, TransactionBin, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual(1, rocksdb:get_latest_sequence_number(Db1)),
  ok = rocksdb:tlog_iterator_close(Itr),

  close_destroy(Db, "test.db"),
  close_destroy(Db1, "test1.db"),
  ok.

iterator_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  Db1 = destroy_reopen("test1.db", [{create_if_missing, true}]),

  ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
  ?assertEqual(1, rocksdb:get_latest_sequence_number(Db)),

  ok = rocksdb:put(Db, <<"b">>, <<"v2">>, []),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),
  ?assertEqual(2, rocksdb:get_latest_sequence_number(Db)),

  {ok, Itr} = rocksdb:tlog_iterator(Db, 0),
  {ok, Last, TransactionBin} = rocksdb:tlog_next_binary_update(Itr),
  ?assertEqual(1, Last),

  ?assertEqual(not_found, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"b">>, [])),


  ok = rocksdb:write_binary_update(Db1, TransactionBin, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"b">>, [])),

  {ok, Last2, TransactionBin2} = rocksdb:tlog_next_binary_update(Itr),
  ?assertEqual(2, Last2),
  ok = rocksdb:write_binary_update(Db1, TransactionBin2, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db1, <<"b">>, [])),

  ?assertEqual({error, invalid_iterator}, rocksdb:tlog_next_binary_update(Itr)),

  ok = rocksdb:tlog_iterator_close(Itr),

  close_destroy(Db, "test.db"),
  close_destroy(Db1, "test1.db"),
  ok.


iterator_with_batch_test() ->

  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  Db1 = destroy_reopen("test1.db", [{create_if_missing, true}]),

  ok = rocksdb_test_util:write(Db, [{put, <<"a">>, <<"v1">>},
    {put, <<"b">>, <<"v2">>}], []),
  ?assertEqual(2, rocksdb:get_latest_sequence_number(Db)),

  ok = rocksdb_test_util:write(Db, [{put, <<"c">>, <<"v3">>}, {delete, <<"a">>}], []),

  ?assertEqual(4, rocksdb:get_latest_sequence_number(Db)),

  {ok, Itr} = rocksdb:tlog_iterator(Db, 0),

  {ok, Last, TransactionBin} = rocksdb:tlog_next_binary_update(Itr),
  ?assertEqual(1, Last),
  ok = rocksdb:write_binary_update(Db1, TransactionBin, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db1, <<"b">>, [])),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"c">>, [])),
  ?assertEqual(2, rocksdb:get_latest_sequence_number(Db1)),

  {ok, Last2, TransactionBin2} = rocksdb:tlog_next_binary_update(Itr),
  ?assertEqual(3, Last2),
  ok = rocksdb:write_binary_update(Db1, TransactionBin2, []),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db1, <<"b">>, [])),
  ?assertEqual({ok, <<"v3">>}, rocksdb:get(Db1, <<"c">>, [])),

  ok = rocksdb:tlog_iterator_close(Itr),
  ?assertEqual(4, rocksdb:get_latest_sequence_number(Db1)),

  close_destroy(Db, "test.db"),
  close_destroy(Db1, "test1.db"),
  ok.

iterator_with_batch2_test() ->

  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  Db1 = destroy_reopen("test1.db", [{create_if_missing, true}]),

  W1 = [{put, <<"a">>, <<"v1">>}, {put, <<"b">>, <<"v2">>}, {put, <<"c">>, <<"v3">>}],
  W2 = [{put, <<"d">>, <<"v4">>}, {delete, <<"a">>}, {single_delete, <<"c">>}],

  ok = rocksdb_test_util:write(Db, W1, []),
  ?assertEqual(3, rocksdb:get_latest_sequence_number(Db)),

  ok = rocksdb_test_util:write(Db, W2, []),

  ?assertEqual(6, rocksdb:get_latest_sequence_number(Db)),

  {ok, Itr} = rocksdb:tlog_iterator(Db, 0),

  {ok, Last, Log, BinLog} = rocksdb:tlog_next_update(Itr),
  ?assertEqual(1, Last),
  ?assertEqual(W1, Log),

  ok = rocksdb:write_binary_update(Db1, BinLog, []),
  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db1, <<"b">>, [])),
  ?assertEqual({ok, <<"v3">>}, rocksdb:get(Db1, <<"c">>, [])),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"d">>, [])),
  ?assertEqual(3, rocksdb:get_latest_sequence_number(Db1)),
  {ok, Last2, Log2, BinLog2} = rocksdb:tlog_next_update(Itr),
  ?assertEqual(4, Last2),
  ?assertEqual(W2, Log2),
  ok = rocksdb:write_binary_update(Db1, BinLog2, []),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"a">>, [])),
  ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db1, <<"b">>, [])),
  ?assertEqual(not_found, rocksdb:get(Db1, <<"c">>, [])),
  ?assertEqual({ok, <<"v4">>}, rocksdb:get(Db1, <<"d">>, [])),
  ?assertEqual(6, rocksdb:get_latest_sequence_number(Db1)),

  ok = rocksdb:tlog_iterator_close(Itr),

  close_destroy(Db, "test.db"),
  close_destroy(Db1, "test1.db"),
  ok.
