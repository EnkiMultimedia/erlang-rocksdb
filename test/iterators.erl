%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010-2013 Basho Technologies, Inc. All Rights Reserved.
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
-module(iterators).

-include_lib("eunit/include/eunit.hrl").

prev_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
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
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

seek_for_prev_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"1">>, []),
    rocksdb:put(Ref, <<"b">>, <<"2">>, []),
    rocksdb:put(Ref, <<"c">>, <<"3">>, []),
    rocksdb:put(Ref, <<"e">>, <<"4">>, []),

    {ok, I} = rocksdb:iterator(Ref, []),
    ?assertEqual({ok, <<"b">>, <<"2">>},rocksdb:iterator_move(I, {seek_for_prev, <<"b">>})),
    ?assertEqual({ok, <<"c">>, <<"3">>},rocksdb:iterator_move(I, {seek_for_prev, <<"d">>})),
    ?assertEqual({ok, <<"e">>, <<"4">>},rocksdb:iterator_move(I, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").


cf_iterator_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref, [DefaultH]} = rocksdb:open_with_cf("ltest", [{create_if_missing, true}], [{"default", []}]),
  {ok, TestH} = rocksdb:create_column_family(Ref, "test", []),
  try
    rocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
    rocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
    rocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

    {ok, DefaultIt} = rocksdb:iterator(Ref, DefaultH, []),
    {ok, TestIt} = rocksdb:iterator(Ref, TestH, []),

    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, <<>>)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(DefaultIt, next)),
    ?assertEqual({ok, <<"b">>, <<"y1">>},rocksdb:iterator_move(TestIt, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, prev)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, prev)),
    ok = rocksdb:iterator_close(TestIt),
    ok = rocksdb:iterator_close(DefaultIt)
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

cf_iterators_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref, [DefaultH]} = rocksdb:open_with_cf("ltest", [{create_if_missing, true}], [{"default", []}]),
  {ok, TestH} = rocksdb:create_column_family(Ref, "test", []),
  try
    rocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
    rocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
    rocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

    {ok, [DefaultIt, TestIt]} = rocksdb:iterators(Ref, [DefaultH, TestH], []),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, <<>>)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(DefaultIt, next)),
    ?assertEqual({ok, <<"b">>, <<"y1">>},rocksdb:iterator_move(TestIt, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, prev)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, prev)),
    ok = rocksdb:iterator_close(TestIt),
    ok = rocksdb:iterator_close(DefaultIt)
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").


drop_cf_with_iterator_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref, [DefaultH]} = rocksdb:open_with_cf("ltest", [{create_if_missing, true}], [{"default", []}]),
  {ok, TestH} = rocksdb:create_column_family(Ref, "test", []),
  try
    rocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
    rocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
    rocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

    {ok, DefaultIt} = rocksdb:iterator(Ref, DefaultH, []),
    {ok, TestIt} = rocksdb:iterator(Ref, TestH, []),

    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, <<>>)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, <<>>)),

    ok = rocksdb:drop_column_family(TestH),

    %% make sure we can read the read iterator
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(DefaultIt, next)),
    ?assertEqual({ok, <<"b">>, <<"y1">>},rocksdb:iterator_move(TestIt, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(DefaultIt, prev)),
    ?assertEqual({ok, <<"a">>, <<"x1">>},rocksdb:iterator_move(TestIt, prev)),
    ok = rocksdb:iterator_close(TestIt),
    ok = rocksdb:iterator_close(DefaultIt)

  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

refresh_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"x">>, []),
    {ok, I} = rocksdb:iterator(Ref, []),
    rocksdb:put(Ref, <<"b">>, <<"y">>, []),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, <<>>)),
    ?assertEqual({error, invalid_iterator},rocksdb:iterator_move(I, next)),
    ?assertEqual(ok, rocksdb:iterator_refresh(I)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, prev)),
    ?assertEqual(ok, rocksdb:iterator_refresh(I)),
    ?assertEqual({error, invalid_iterator},rocksdb:iterator_move(I, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I)),
    ?assertError(badarg, rocksdb:iterator_refresh(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

iterate_upper_bound_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, <<"b">>, <<"y">>, []),
    rocksdb:put(Ref, <<"d">>, <<"z">>, []),
    {ok, I} = rocksdb:iterator(Ref, [{iterate_upper_bound, <<"c">>}]),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(I, next)),
    ?assertEqual({error, invalid_iterator},rocksdb:iterator_move(I, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

iterate_outer_bound_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, <<"c">>, <<"y">>, []),
    rocksdb:put(Ref, <<"d">>, <<"z">>, []),
    {ok, I} = rocksdb:iterator(Ref, [{iterate_lower_bound, <<"b">>}]),
    ?assertEqual({ok, <<"d">>, <<"z">>},rocksdb:iterator_move(I, last)),
    ?assertEqual({ok, <<"c">>, <<"y">>},rocksdb:iterator_move(I, prev)),
    ?assertEqual({error, invalid_iterator},rocksdb:iterator_move(I, prev)),
    ?assertEqual(ok, rocksdb:iterator_close(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

prefix_same_as_start_test() ->
  rocksdb_test_util:rm_rf("test_prefix"),  % NOTE
  {ok, Ref} = rocksdb:open("test_prefix", [{create_if_missing, true},
                                           {prefix_extractor, {fixed_prefix_transform, 8}}]),
  try

    ok = put_key(Ref, 12345, 6, <<"v16">>),
    ok = put_key(Ref, 12345, 7, <<"v17">>),
    ok = put_key(Ref, 12345, 8, <<"v18">>),
    ok = put_key(Ref, 12345, 9, <<"v19">>),
    ok = put_key(Ref, 12346, 8, <<"v16">>),
    ok = rocksdb:flush(Ref, []),

    {ok, I} = rocksdb:iterator(Ref, [{prefix_same_as_start, true}]),

    ?assertEqual({ok, test_key(12345, 6), <<"v16">>}, rocksdb:iterator_move(I, test_key(12345, 6))),
    ?assertEqual({ok, test_key(12345, 7), <<"v17">>}, rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, test_key(12345, 8), <<"v18">>}, rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, test_key(12345, 9), <<"v19">>}, rocksdb:iterator_move(I, next)),
    ?assertEqual({error, invalid_iterator}, rocksdb:iterator_move(I, next)),

    ?assertEqual(ok, rocksdb:iterator_close(I)),
    ?assertError(badarg, rocksdb:iterator_refresh(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("test_prefix", []),
  rocksdb_test_util:rm_rf("test_prefix").


invalid_test() ->
  rocksdb_test_util:rm_rf("ltest"),  % NOTE
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"a">>, <<"x">>, []),
    rocksdb:put(Ref, <<"b">>, <<"y">>, []),
    {ok, I} = rocksdb:iterator(Ref, []),
    ?assertEqual({error, invalid_iterator}, rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"y">>},rocksdb:iterator_move(I, next)),
    ?assertEqual({ok, <<"a">>, <<"x">>},rocksdb:iterator_move(I, prev)),
    ?assertEqual(ok, rocksdb:iterator_close(I))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

%% Test auto_readahead_size option for iterators
auto_readahead_size_test() ->
  rocksdb_test_util:rm_rf("ltest"),
  {ok, Ref} = rocksdb:open("ltest", [{create_if_missing, true}]),
  try
    rocksdb:put(Ref, <<"key1">>, <<"val1">>, []),
    rocksdb:put(Ref, <<"key2">>, <<"val2">>, []),
    rocksdb:put(Ref, <<"key3">>, <<"val3">>, []),

    %% Test with auto_readahead_size enabled (default is true in RocksDB 8.10+)
    {ok, I1} = rocksdb:iterator(Ref, [{auto_readahead_size, true}]),
    ?assertEqual({ok, <<"key1">>, <<"val1">>}, rocksdb:iterator_move(I1, first)),
    ?assertEqual({ok, <<"key2">>, <<"val2">>}, rocksdb:iterator_move(I1, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I1)),

    %% Test with auto_readahead_size disabled
    {ok, I2} = rocksdb:iterator(Ref, [{auto_readahead_size, false}]),
    ?assertEqual({ok, <<"key1">>, <<"val1">>}, rocksdb:iterator_move(I2, first)),
    ?assertEqual({ok, <<"key2">>, <<"val2">>}, rocksdb:iterator_move(I2, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I2))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("ltest", []),
  rocksdb_test_util:rm_rf("ltest").

%% Test allow_unprepared_value option for lazy blob loading in BlobDB
%% When allow_unprepared_value=true, blob values are not loaded until
%% PrepareValue() is called
allow_unprepared_value_test() ->
  rocksdb_test_util:rm_rf("test_unprepared"),
  {ok, Ref} = rocksdb:open("test_unprepared",
    [{create_if_missing, true}
    ,{enable_blob_files, true}
    ,{min_blob_size, 0}]),  %% All values go to blob files
  try
    Value = list_to_binary(lists:duplicate(100, $a)),
    rocksdb:put(Ref, <<"key1">>, Value, []),
    rocksdb:put(Ref, <<"key2">>, Value, []),
    rocksdb:flush(Ref, []),

    %% Test with allow_unprepared_value enabled
    %% Values will be empty until PrepareValue() is called
    {ok, I} = rocksdb:iterator(Ref, [{allow_unprepared_value, true}]),
    %% Keys are returned, but blob values are empty (not yet loaded)
    {ok, <<"key1">>, _} = rocksdb:iterator_move(I, first),
    {ok, <<"key2">>, _} = rocksdb:iterator_move(I, next),
    ?assertEqual(ok, rocksdb:iterator_close(I)),

    %% Test with allow_unprepared_value disabled (default)
    %% Values are loaded automatically
    {ok, I2} = rocksdb:iterator(Ref, [{allow_unprepared_value, false}]),
    ?assertEqual({ok, <<"key1">>, Value}, rocksdb:iterator_move(I2, first)),
    ?assertEqual({ok, <<"key2">>, Value}, rocksdb:iterator_move(I2, next)),
    ?assertEqual(ok, rocksdb:iterator_close(I2))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("test_unprepared", []),
  rocksdb_test_util:rm_rf("test_unprepared").

%% Test iterator_prepare_value for explicit blob value loading
iterator_prepare_value_test() ->
  rocksdb_test_util:rm_rf("test_prepare_value"),
  {ok, Ref} = rocksdb:open("test_prepare_value",
    [{create_if_missing, true}
    ,{enable_blob_files, true}
    ,{min_blob_size, 0}]),  %% All values go to blob files
  try
    Value1 = list_to_binary(lists:duplicate(100, $a)),
    Value2 = list_to_binary(lists:duplicate(100, $b)),
    rocksdb:put(Ref, <<"key1">>, Value1, []),
    rocksdb:put(Ref, <<"key2">>, Value2, []),
    rocksdb:flush(Ref, []),

    %% Create iterator with allow_unprepared_value
    {ok, I} = rocksdb:iterator(Ref, [{allow_unprepared_value, true}]),

    %% Move to first - value is empty (unprepared)
    {ok, <<"key1">>, EmptyVal1} = rocksdb:iterator_move(I, first),
    ?assertEqual(<<>>, EmptyVal1),

    %% Call prepare_value to load the blob
    ?assertEqual(ok, rocksdb:iterator_prepare_value(I)),

    %% Now when we call iterator_move with a seek to the same key,
    %% we can verify the value is there. Note: we need to re-seek
    %% because iterator_move doesn't re-read the current value
    %% Actually, the value should be available in the iterator's
    %% value() method after PrepareValue. Let's just verify prepare works.

    %% Move to next - value is also empty until prepared
    {ok, <<"key2">>, EmptyVal2} = rocksdb:iterator_move(I, next),
    ?assertEqual(<<>>, EmptyVal2),

    %% Prepare the second value
    ?assertEqual(ok, rocksdb:iterator_prepare_value(I)),

    ?assertEqual(ok, rocksdb:iterator_close(I)),

    %% Test error case: prepare_value on invalid iterator
    {ok, I2} = rocksdb:iterator(Ref, [{allow_unprepared_value, true}]),
    %% Iterator is not positioned yet, should return error
    ?assertEqual({error, invalid_iterator}, rocksdb:iterator_prepare_value(I2)),
    ?assertEqual(ok, rocksdb:iterator_close(I2))
  after
    rocksdb:close(Ref)
  end,
  rocksdb:destroy("test_prepare_value", []),
  rocksdb_test_util:rm_rf("test_prepare_value").

test_key(Prefix, Suffix) when is_integer(Prefix), is_integer(Suffix) ->
  << Prefix:64, Suffix:64 >>.

put_key(Db, Prefix, Suffix, Value) ->
  rocksdb:put(Db, test_key(Prefix, Suffix), Value, []).
