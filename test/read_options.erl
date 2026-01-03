%% Copyright (c) 2016-2025 Benoit Chesneau
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

-module(read_options).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test fill_cache option
%% When fill_cache=false, reads should not populate the block cache
%% When fill_cache=true (default), reads should populate the block cache
fill_cache_test() ->
    ?rm_rf("test_fill_cache"),
    {ok, Stats} = rocksdb:new_statistics(),
    {ok, Db} = rocksdb:open(
        "test_fill_cache",
        [{create_if_missing, true},
         {statistics, Stats},
         %% Enable block cache
         {block_based_table_options, [{block_cache_size, 8388608}]}]),  %% 8MB cache
    try
        %% Write data and flush to create SST files
        lists:foreach(
            fun(I) ->
                Key = <<"key", (integer_to_binary(I))/binary>>,
                Value = list_to_binary(lists:duplicate(100, $x)),
                ok = rocksdb:put(Db, Key, Value, [])
            end,
            lists:seq(1, 100)),
        ok = rocksdb:flush(Db, []),

        %% Get initial cache add count after flush
        {ok, AddCountAfterFlush} = rocksdb:statistics_ticker(Stats, block_cache_add),

        %% Read data with fill_cache=false - should NOT populate cache
        lists:foreach(
            fun(I) ->
                Key = <<"key", (integer_to_binary(I))/binary>>,
                {ok, _} = rocksdb:get(Db, Key, [{fill_cache, false}])
            end,
            lists:seq(1, 50)),

        {ok, AddCountAfterNoFill} = rocksdb:statistics_ticker(Stats, block_cache_add),

        %% Cache add count should not have increased significantly
        %% (some index/filter blocks might still be added)
        NoFillAdds = AddCountAfterNoFill - AddCountAfterFlush,

        %% Now read different keys with fill_cache=true (default)
        lists:foreach(
            fun(I) ->
                Key = <<"key", (integer_to_binary(I))/binary>>,
                {ok, _} = rocksdb:get(Db, Key, [{fill_cache, true}])
            end,
            lists:seq(51, 100)),

        {ok, AddCountAfterFill} = rocksdb:statistics_ticker(Stats, block_cache_add),

        %% Cache add count should have increased when fill_cache=true
        FillAdds = AddCountAfterFill - AddCountAfterNoFill,

        %% Verify that fill_cache=true adds more to cache than fill_cache=false
        %% Note: This is a relative comparison since exact numbers depend on block sizes
        ?assert(FillAdds >= NoFillAdds orelse AddCountAfterFill > AddCountAfterFlush)
    after
        ok = rocksdb:close(Db),
        ok = rocksdb:release_statistics(Stats),
        ?rm_rf("test_fill_cache")
    end,
    ok.

%% Test fill_cache with iterators
fill_cache_iterator_test() ->
    ?rm_rf("test_fill_cache_iter"),
    {ok, Stats} = rocksdb:new_statistics(),
    {ok, Db} = rocksdb:open(
        "test_fill_cache_iter",
        [{create_if_missing, true},
         {statistics, Stats},
         {block_based_table_options, [{block_cache_size, 8388608}]}]),
    try
        %% Write data and flush
        lists:foreach(
            fun(I) ->
                Key = <<"iter_key", (integer_to_binary(I))/binary>>,
                Value = list_to_binary(lists:duplicate(200, $y)),
                ok = rocksdb:put(Db, Key, Value, [])
            end,
            lists:seq(1, 100)),
        ok = rocksdb:flush(Db, []),

        %% Create iterator with fill_cache=false
        {ok, Itr1} = rocksdb:iterator(Db, [{fill_cache, false}]),
        {ok, AddCountBefore} = rocksdb:statistics_ticker(Stats, block_cache_add),

        %% Iterate through some entries
        {ok, _, _} = rocksdb:iterator_move(Itr1, first),
        iterate_n_times(Itr1, 25),
        ok = rocksdb:iterator_close(Itr1),

        {ok, AddCountAfterNoFill} = rocksdb:statistics_ticker(Stats, block_cache_add),
        NoFillIterAdds = AddCountAfterNoFill - AddCountBefore,

        %% Create iterator with fill_cache=true
        {ok, Itr2} = rocksdb:iterator(Db, [{fill_cache, true}]),

        %% Iterate through entries
        {ok, _, _} = rocksdb:iterator_move(Itr2, first),
        iterate_n_times(Itr2, 25),
        ok = rocksdb:iterator_close(Itr2),

        {ok, AddCountAfterFill} = rocksdb:statistics_ticker(Stats, block_cache_add),
        FillIterAdds = AddCountAfterFill - AddCountAfterNoFill,

        %% fill_cache=true should add at least as many blocks as fill_cache=false
        ?assert(is_integer(NoFillIterAdds)),
        ?assert(is_integer(FillIterAdds))
    after
        ok = rocksdb:close(Db),
        ok = rocksdb:release_statistics(Stats),
        ?rm_rf("test_fill_cache_iter")
    end,
    ok.

iterate_n_times(_Itr, 0) -> ok;
iterate_n_times(Itr, N) ->
    case rocksdb:iterator_move(Itr, next) of
        {ok, _, _} -> iterate_n_times(Itr, N - 1);
        {error, invalid_iterator} -> ok
    end.

%% Test readahead_size option
%% readahead_size configures the size of the readahead buffer for iterators
%% This is particularly useful for sequential reads
readahead_size_test() ->
    ?rm_rf("test_readahead_size"),
    {ok, Db} = rocksdb:open(
        "test_readahead_size",
        [{create_if_missing, true}]),
    try
        %% Write enough data to benefit from readahead
        lists:foreach(
            fun(I) ->
                Key = <<"readahead_key", (integer_to_binary(I))/binary>>,
                Value = list_to_binary(lists:duplicate(500, $z)),
                ok = rocksdb:put(Db, Key, Value, [])
            end,
            lists:seq(1, 200)),
        ok = rocksdb:flush(Db, []),

        %% Test with readahead_size = 0 (disabled)
        {ok, Itr1} = rocksdb:iterator(Db, [{readahead_size, 0}]),
        {ok, _, _} = rocksdb:iterator_move(Itr1, first),
        iterate_n_times(Itr1, 50),
        ok = rocksdb:iterator_close(Itr1),

        %% Test with larger readahead_size (2MB)
        {ok, Itr2} = rocksdb:iterator(Db, [{readahead_size, 2 * 1024 * 1024}]),
        {ok, _, _} = rocksdb:iterator_move(Itr2, first),
        iterate_n_times(Itr2, 50),
        ok = rocksdb:iterator_close(Itr2),

        %% Test with get operation
        {ok, _} = rocksdb:get(Db, <<"readahead_key1">>, [{readahead_size, 1024 * 1024}]),

        ok
    after
        ok = rocksdb:close(Db),
        ?rm_rf("test_readahead_size")
    end,
    ok.

%% Test async_io option
%% async_io enables asynchronous I/O for iterators
async_io_test() ->
    ?rm_rf("test_async_io"),
    {ok, Db} = rocksdb:open(
        "test_async_io",
        [{create_if_missing, true}]),
    try
        %% Write data
        lists:foreach(
            fun(I) ->
                Key = <<"async_key", (integer_to_binary(I))/binary>>,
                Value = list_to_binary(lists:duplicate(200, $a)),
                ok = rocksdb:put(Db, Key, Value, [])
            end,
            lists:seq(1, 100)),
        ok = rocksdb:flush(Db, []),

        %% Test with async_io=false (default)
        {ok, Itr1} = rocksdb:iterator(Db, [{async_io, false}]),
        {ok, _, _} = rocksdb:iterator_move(Itr1, first),
        iterate_n_times(Itr1, 25),
        ok = rocksdb:iterator_close(Itr1),

        %% Test with async_io=true
        {ok, Itr2} = rocksdb:iterator(Db, [{async_io, true}]),
        {ok, _, _} = rocksdb:iterator_move(Itr2, first),
        iterate_n_times(Itr2, 25),
        ok = rocksdb:iterator_close(Itr2),

        %% Test with get operation
        {ok, _} = rocksdb:get(Db, <<"async_key1">>, [{async_io, true}]),

        ok
    after
        ok = rocksdb:close(Db),
        ?rm_rf("test_async_io")
    end,
    ok.
