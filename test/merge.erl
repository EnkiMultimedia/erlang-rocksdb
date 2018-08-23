%% Copyright (c) 2018 Benoit Chesneau
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
%%
%% -------------------------------------------------------------------

-module(merge).

-include_lib("eunit/include/eunit.hrl").


merge_int_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"i">>, term_to_binary(0), []),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),

  ok = rocksdb:merge(Db, <<"i">>, term_to_binary({int_add, 1}), []),
  {ok, IBin1} = rocksdb:get(Db, <<"i">>, []),
  1 = binary_to_term(IBin1),

  ok = rocksdb:merge(Db, <<"i">>, term_to_binary({int_add, 2}), []),
  {ok, IBin2} = rocksdb:get(Db, <<"i">>, []),
  3 = binary_to_term(IBin2),

  ok = rocksdb:merge(Db, <<"i">>, term_to_binary({int_add, -1}), []),
  {ok, IBin3} = rocksdb:get(Db, <<"i">>, []),
  2 = binary_to_term(IBin3),

  ok = rocksdb:put(Db, <<"i">>, term_to_binary(0), []),
  {ok, IBin4} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin4),


  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_list_append_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_append, [c, d]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d] = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_append, [d, e]}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, d, e] = binary_to_term(Bin2),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_list_substract_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_substract, [c, a]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [b, d, e] = binary_to_term(Bin1),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).



merge_list_set_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_set, 2, 'c1'}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [a, b, 'c1', d, e] = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_set, 4, 'e1'}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"list">>, []),
  [a, b, 'c1', d, 'e1'] = binary_to_term(Bin2),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_set, 5, error}), []),
  corruption = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_list_delete_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, f, g]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e, f, g] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 2}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [a, b, d, e, f, g] = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 2, 4}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"list">>, []),
  [a, b, g] = binary_to_term(Bin2),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 6}), []),
  corruption = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_list_insert_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, f, g]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e, f, g] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_insert, 2, [h, i]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [a, b, h, i, c, d, e, f, g] = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_insert, 9, [j]}), []),
  corruption = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_binary_append_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"bin">>, <<"test">>, []),
  {ok, <<"test">>} = rocksdb:get(Db, <<"bin">>, []),

  ok = rocksdb:merge(Db, <<"bin">>, term_to_binary({binary_append, <<"abc">>}), []),
  {ok, <<"testabc">>} = rocksdb:get(Db, <<"bin">>, []),

  ok = rocksdb:merge(Db, <<"bin">>, term_to_binary({binary_append, <<"de">>}), []),
  {ok, <<"testabcde">>} = rocksdb:get(Db, <<"bin">>, []),


  ok = rocksdb:put(Db, <<"encbin">>, term_to_binary(<<"test">>), []),
  {ok, Bin} = rocksdb:get(Db, <<"encbin">>, []),
  <<"test">> = binary_to_term(Bin),

  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_append, <<"abc">>}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"encbin">>, []),
  <<"testabc">> = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_append, <<"de">>}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"encbin">>, []),
  <<"testabcde">> = binary_to_term(Bin2),

  ok = rocksdb:merge(Db, <<"empty">>, term_to_binary({binary_append, <<"abc">>}), []),
  {ok, <<"abc">>} = rocksdb:get(Db, <<"empty">>, []),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_binary_replace_test() ->

  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"bin">>, <<"The quick brown fox jumps over the lazy dog.">>, []),
  {ok, <<"The quick brown fox jumps over the lazy dog.">>} = rocksdb:get(Db, <<"bin">>, []),

  ok = rocksdb:merge(Db, <<"bin">>, term_to_binary({binary_replace, 10, 5, <<"red">>}), []),
  ok = rocksdb:merge(Db, <<"bin">>, term_to_binary({binary_replace, 0, 3, <<"A">>}), []),
  {ok, <<"A quick red fox jumps over the lazy dog.">>} = rocksdb:get(Db, <<"bin">>, []),


  ok = rocksdb:put(Db, <<"encbin">>, term_to_binary(<<"The quick brown fox jumps over the lazy dog.">>), []),
  {ok, Bin} = rocksdb:get(Db, <<"encbin">>, []),
  <<"The quick brown fox jumps over the lazy dog.">> = binary_to_term(Bin),

  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 10, 5, <<"red">>}), []),
  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 0, 3, <<"A">>}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"encbin">>, []),
  <<"A quick red fox jumps over the lazy dog.">> = binary_to_term(Bin1),


  ok = rocksdb:put(Db, <<"bitmap">>, <<1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1>>, []),
  {ok, <<1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1>>} = rocksdb:get(Db, <<"bitmap">>, []),

  ok = rocksdb:merge(Db, <<"bitmap">>, term_to_binary({binary_replace, 2, 1, <<0>>}), []),
  {ok, <<1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1>>} = rocksdb:get(Db, <<"bitmap">>, []),

  ok = rocksdb:merge(Db, <<"bitmap">>, term_to_binary({binary_replace, 6, 1, <<0>>}), []),
  {ok, <<1,1,0,1,1,1,0,1,1,1,1,1,1,1,1,1>>} = rocksdb:get(Db, <<"bitmap">>, []),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_binary_erase_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"erase">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"erase">>, term_to_binary({binary_erase, 2, 4}), []),
  {ok, <<"abghij">>} = rocksdb:get(Db, <<"erase">>, []),

   ok = rocksdb:put(Db, <<"erase">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"erase">>, term_to_binary({binary_erase, 9, 1}), []),
  {ok, <<"abcdefghi">>} = rocksdb:get(Db, <<"erase">>, []),


  ok = rocksdb:put(Db, <<"corrupted">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"corrupted">>, term_to_binary({binary_erase, 9, 2}), []),
  corruption = rocksdb:get(Db, <<"corrupted">>, []),

  ok = rocksdb:put(Db, <<"corrupted">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"corrupted">>, term_to_binary({binary_erase, 2, 9}), []),
  corruption = rocksdb:get(Db, <<"corrupted">>, []),

  ok = rocksdb:put(Db, <<"erase_to_end">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"erase_to_end">>, term_to_binary({binary_erase, 2, 8}), []),
  {ok, <<"ab">>} = rocksdb:get(Db, <<"erase_to_end">>, []),


  ok = rocksdb:put(Db, <<"eraseterm">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"eraseterm">>, term_to_binary({binary_erase, 2, 4}), []),
  {ok, Bin} = rocksdb:get(Db, <<"eraseterm">>, []),
  <<"abghij">> = binary_to_term(Bin),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).

merge_binary_insert_test() ->
  [] = os:cmd("rm -rf /tmp/rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("/tmp/rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"insert">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 2, <<"1234">>}), []),
  {ok, <<"ab1234cdefghij">>} = rocksdb:get(Db, <<"insert">>, []),

  ok = rocksdb:put(Db, <<"insert">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 9, <<"1234">>}), []),
  {ok, <<"abcdefghi1234j">>} = rocksdb:get(Db, <<"insert">>, []),

  ok = rocksdb:put(Db, <<"insert">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 10, <<"1234">>}), []),
  {ok, <<"abcdefghij1234">>} = rocksdb:get(Db, <<"insert">>, []),


  ok = rocksdb:put(Db, <<"insert">>, <<"abcdefghij">>, []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 11, <<"1234">>}), []),
  corruption = rocksdb:get(Db, <<"insert">>, []),


  ok = rocksdb:put(Db, <<"insertterm">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"insertterm">>, term_to_binary({binary_insert, 2, <<"1234">>}), []),
  {ok, Bin} = rocksdb:get(Db, <<"insertterm">>, []),
  <<"ab1234cdefghij">> = binary_to_term(Bin),

  ok = rocksdb:close(Db),
  ok = rocksdb:destroy("/tmp/rocksdb_merge_db.test", []).
