%% Copyright (c) 2018-2020 Benoit Chesneau
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
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"i">>, term_to_binary(0), []),
  {ok, IBin0} = rocksdb:get(Db, <<"i">>, []),
  0 = binary_to_term(IBin0),

  % Wrap in list and add dummy empties just to test that an iolist is accepted:
  ok = rocksdb:merge(Db, <<"i">>, [term_to_binary({int_add, 1}), <<>>, [<<>>]], []),
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
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_list_append_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
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

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_append, []}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_list_substract_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, a, b, c]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e, a, b, c] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_substract, [c, a]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [b, d, e, a, b, c] = binary_to_term(Bin1),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_list_set_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
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
  {ok, Bin2}  = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_list_delete_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
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

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 3}), []),
  {ok, Bin2}  = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 6}), []),
  {ok, Bin2}  = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_delete, 2}), []),
  {ok, Bin3} = rocksdb:get(Db, <<"list">>, []),
  [a, b] = binary_to_term(Bin3),
  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_list_insert_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"list">>, term_to_binary([a, b, c, d, e, f, g]), []),
  {ok, Bin0} = rocksdb:get(Db, <<"list">>, []),
  [a, b, c, d, e, f, g] = binary_to_term(Bin0),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_insert, 2, [h, i]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),
  [a, b, h, i, c, d, e, f, g] = binary_to_term(Bin1),

  ok = rocksdb:merge(Db, <<"list">>, term_to_binary({list_insert, 9, [j]}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"list">>, []),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_binary_append_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

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
  {ok, Bin3} = rocksdb:get(Db, <<"empty">>, []),
  <<"abc">> = binary_to_term(Bin3),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_binary_replace_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"encbin">>, term_to_binary(<<"The quick brown fox jumps over the lazy dog.">>), []),
  {ok, Bin} = rocksdb:get(Db, <<"encbin">>, []),
  <<"The quick brown fox jumps over the lazy dog.">> = binary_to_term(Bin),

  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 10, 5, <<"red">>}), []),
  ok = rocksdb:merge(Db, <<"encbin">>, term_to_binary({binary_replace, 0, 3, <<"A">>}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"encbin">>, []),
  <<"A quick red fox jumps over the lazy dog.">> = binary_to_term(Bin1),

  ok = rocksdb:put(Db, <<"bitmap">>, term_to_binary(<<1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1>>), []),
  ok = rocksdb:merge(Db, <<"bitmap">>, term_to_binary({binary_replace, 2, 1, <<0>>}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"bitmap">>, []),
  <<1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1>> = binary_to_term(Bin2),

  ok = rocksdb:merge(Db, <<"bitmap">>, term_to_binary({binary_replace, 6, 1, <<0>>}), []),
  {ok, Bin3} = rocksdb:get(Db, <<"bitmap">>, []),
  <<1,1,0,1,1,1,0,1,1,1,1,1,1,1,1,1>> = binary_to_term(Bin3),

  ok = rocksdb:put(Db, <<"encbin1">>, term_to_binary(<<"abcdef">>), []),
  ok = rocksdb:merge(Db, <<"encbin1">>, term_to_binary({binary_replace, 6, 1, <<"A">>}), []),
  {ok, Bin4} = rocksdb:get(Db, <<"encbin1">>, []),
  <<"abcdef">> = binary_to_term(Bin4),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_binary_erase_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),


  ok = rocksdb:put(Db, <<"eraseterm">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"eraseterm">>, term_to_binary({binary_erase, 2, 4}), []),
  {ok, Bin} = rocksdb:get(Db, <<"eraseterm">>, []),
  <<"abghij">> = binary_to_term(Bin),

  ok = rocksdb:put(Db, <<"erase_end">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"erase_end">>, term_to_binary({binary_erase, 9, 2}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"erase_end">>, []),
  <<"abcdefghi">> = binary_to_term(Bin1),

  ok = rocksdb:put(Db, <<"erase_to_end1">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"erase_to_end1">>, term_to_binary({binary_erase, 2, 9}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"erase_to_end1">>, []),
  <<"ab">> = binary_to_term(Bin2),

  ok = rocksdb:put(Db, <<"erase_to_end2">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"erase_to_end2">>, term_to_binary({binary_erase, 2, 8}), []),
  {ok, Bin3} = rocksdb:get(Db, <<"erase_to_end2">>, []),
  <<"ab">> = binary_to_term(Bin3),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).

merge_binary_insert_test() ->
  rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, erlang_merge_operator}]),

  ok = rocksdb:put(Db, <<"insertterm">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"insertterm">>, term_to_binary({binary_insert, 2, <<"1234">>}), []),
  {ok, Bin} = rocksdb:get(Db, <<"insertterm">>, []),
  <<"ab1234cdefghij">> = binary_to_term(Bin),

  ok = rocksdb:put(Db, <<"insert">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 9, <<"1234">>}), []),
  {ok, Bin1} = rocksdb:get(Db, <<"insert">>, []),
  <<"abcdefghi1234j">> = binary_to_term(Bin1),

  ok = rocksdb:put(Db, <<"insert">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 10, <<"1234">>}), []),
  {ok, Bin2} = rocksdb:get(Db, <<"insert">>, []),
  <<"abcdefghij1234">> = binary_to_term(Bin2),

  ok = rocksdb:put(Db, <<"insert">>, term_to_binary(<<"abcdefghij">>), []),
  ok = rocksdb:merge(Db, <<"insert">>, term_to_binary({binary_insert, 11, <<"1234">>}), []),
  {ok, Bin3} = rocksdb:get(Db, <<"insert">>, []),
  <<"abcdefghij">> = binary_to_term(Bin3),


  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).


merge_bitset_test() ->
 rocksdb_test_util:rm_rf("rocksdb_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, {bitset_merge_operator, 1024}}]),

  Bitmap = << 0:1024/unsigned >>,
  ok = rocksdb:put(Db, <<"bitmap">>, Bitmap, []),
  ok = rocksdb:merge(Db, <<"bitmap">>, <<"+2">>, []),
  {ok, << 32, _/binary>> = Bin1 } = rocksdb:get(Db, <<"bitmap">>, []),
  1024 = bit_size(Bin1),
  ok = rocksdb:merge(Db, <<"bitmap">>, <<"-2">>, []),
  {ok, << 0, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
  ok = rocksdb:merge(Db, <<"bitmap">>, <<"+11">>, []),
  {ok, << 0, 16, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
  ok = rocksdb:merge(Db, <<"bitmap">>, <<"+10">>, []),
  {ok, << 0, 48, _/binary>> } = rocksdb:get(Db, <<"bitmap">>, []),
  ok = rocksdb:merge(Db, <<"bitmap">>, <<"">>, []),
  {ok, << 0, 0, _/binary>> = Bin2 } = rocksdb:get(Db, <<"bitmap">>, []),
  1024 = bit_size(Bin2),

  ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+2">>, []),
  {ok, << 32, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
  ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"-2">>, []),
  {ok, << 0, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
  ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+11">>, []),
  {ok, << 0, 16, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),
  ok = rocksdb:merge(Db, <<"unsetbitmap">>, <<"+10">>, []),
  {ok, << 0, 48, _/binary>> } = rocksdb:get(Db, <<"unsetbitmap">>, []),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_merge_db.test", []).


merge_counter_test() ->
  rocksdb_test_util:rm_rf("rocksdb_counter_merge_db.test"),
  {ok, Db} = rocksdb:open("rocksdb_counter_merge_db.test",
                           [{create_if_missing, true},
                            {merge_operator, counter_merge_operator}]),


  ok = rocksdb:merge(Db, <<"c">>, << "1" >>, []),
  {ok, << "1" >>} = rocksdb:get(Db, <<"c">>, []),

  ok = rocksdb:merge(Db, <<"c">>, << "2" >>, []),
  {ok, << "3" >>} = rocksdb:get(Db, <<"c">>, []),

  ok = rocksdb:merge(Db, <<"c">>, <<"-1">> , []),
  {ok, <<"2">>} = rocksdb:get(Db, <<"c">>, []),

  ok = rocksdb:put(Db, <<"c">>, <<"0">>, []),
  {ok, <<"0">>} = rocksdb:get(Db, <<"c">>, []),

  ok = rocksdb:close(Db),
  ok = destroy_and_rm("rocksdb_counter_merge_db.test", []).


destroy_and_rm(Dir, Options) ->
  rocksdb:destroy(Dir, Options),
  rocksdb_test_util:rm_rf(Dir).
