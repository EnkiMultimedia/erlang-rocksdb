%% Copyright (c) 2020 Benoît Chesneau.
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

-module(compression).
-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

open_with_snappy_test() ->
  ?rm_rf("erocksdb.snapppy.test"),
  {ok, Ref} = rocksdb:open("erocksdb.snapppy.test", [{create_if_missing, true}, {compression, snappy}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.snapppy.test", []),
  ok.

open_with_lz4_test() ->
  ?rm_rf("erocksdb.lz4.test"),
  {ok, Ref} = rocksdb:open("erocksdb.lz4.test", [{create_if_missing, true}, {compression, lz4}]),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.lz4.test", []),
  ok.

open_with_zstd_test() ->
  ?rm_rf("erocksdb.zstd.test"),
  {ok, Ref} = rocksdb:open("erocksdb.zstd.test", [{create_if_missing, true}, {compression, zstd}]),
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.zstd.test", []),
  ok.

compression_test() ->
  CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
  ?rm_rf("erocksdb.compress.0"),
  ?rm_rf("erocksdb.compress.1"),
  {ok, Ref0} = rocksdb:open("erocksdb.compress.0", [{create_if_missing, true}, {compression, none}]),
  [ok = rocksdb:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  {ok, Ref1} = rocksdb:open("erocksdb.compress.1", [{create_if_missing, true},
                                                    {compression, snappy},
                                                    {compression_opts, [{enabled, true},
                                                                        {window_bits, 8 * 1024},
                                                                        {strategy, 3},
                                                                        {level, 16},
                                                                        {max_dict_bytes, 32 * 1024}]}]),
  [ok = rocksdb:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],

  %% Check both of the LOG files created to see if the compression option was correctly passed down
  %% Check uncompressed DB Log
  {ok, Log0} = file:read_file("erocksdb.compress.0/LOG"),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression: NoCompression")),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression_opts.window_bits: -14")),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression_opts.level: 32767")),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression_opts.strategy: 0")),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression_opts.max_dict_bytes: 0")),

  %% Check compressed DB Log
  {ok, Log1} = file:read_file("erocksdb.compress.1/LOG"),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression: Snappy")),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression_opts.window_bits: 8192")),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression_opts.level: 16")),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression_opts.strategy: 3")),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression_opts.max_dict_bytes: 32768")),

  ok = rocksdb:close(Ref0),
  ok = rocksdb:close(Ref1),
  ?rm_rf("erocksdb.compress.0"),
  ?rm_rf("erocksdb.compress.1").


bottommost_compression_test() ->
  CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
  ?rm_rf("erocksdb.bm_compress.0"),
  ?rm_rf("erocksdb.bm_compress.1"),
  {ok, Ref0} = rocksdb:open("erocksdb.bm_compress.0", [{create_if_missing, true}, {compression, none}]),
  [ok = rocksdb:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],
  {ok, Ref1} = rocksdb:open("erocksdb.bm_compress.1", [{create_if_missing, true},
                                                       {compression, none},
                                                       {bottommost_compression, snappy},
                                                       {bottommost_compression_opts,
                                                        [{enabled, true},
                                                         {window_bits, 8 * 1024},
                                                         {strategy, 3},
                                                         {level, 16},
                                                         {max_dict_bytes, 32 * 1024}]}]),
  [ok = rocksdb:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
  I <- lists:seq(1,10)],

  %% Check both of the LOG files created to see if the compression option was correctly passed down
  %% Check uncompressed DB Log
  {ok, Log0} = file:read_file("erocksdb.bm_compress.0/LOG"),
  ?assertMatch({match, _}, re:run(Log0, "Options.compression: NoCompression")),
  ?assertMatch({match, _}, re:run(Log0, "Options.bottommost_compression: Disabled")),
  ?assertMatch({match, _}, re:run(Log0, "Options.bottommost_compression_opts.window_bits: -14")),
  ?assertMatch({match, _}, re:run(Log0, "Options.bottommost_compression_opts.level: 32767")),
  ?assertMatch({match, _}, re:run(Log0, "Options.bottommost_compression_opts.strategy: 0")),
  ?assertMatch({match, _}, re:run(Log0, "Options.bottommost_compression_opts.max_dict_bytes: 0")),

  %% Check compressed DB Log
  {ok, Log1} = file:read_file("erocksdb.bm_compress.1/LOG"),
  ?assertMatch({match, _}, re:run(Log1, "Options.compression: NoCompression")),
  ?assertMatch({match, _}, re:run(Log1, "Options.bottommost_compression: Snappy")),
  ?assertMatch({match, _}, re:run(Log1, "Options.bottommost_compression_opts.window_bits: 8192")),
  ?assertMatch({match, _}, re:run(Log1, "Options.bottommost_compression_opts.level: 16")),
  ?assertMatch({match, _}, re:run(Log1, "Options.bottommost_compression_opts.strategy: 3")),
  ?assertMatch({match, _}, re:run(Log1, "Options.bottommost_compression_opts.max_dict_bytes: 32768")),

  ok = rocksdb:close(Ref0),
  ok = rocksdb:close(Ref1),
  ?rm_rf("erocksdb.bm_compress.0"),
  ?rm_rf("erocksdb.bm_compress.1").


