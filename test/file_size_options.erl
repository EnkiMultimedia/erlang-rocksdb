%% Copyright (c) 2016-2026 Benoit Chesneau.
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

-module(file_size_options).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test opening DB with target_file_size_is_upper_bound=false (default)
target_file_size_is_upper_bound_false_test() ->
  ?rm_rf("erocksdb.file_size_default.test"),
  {ok, Ref} = rocksdb:open("erocksdb.file_size_default.test",
                           [{create_if_missing, true},
                            {target_file_size_is_upper_bound, false}]),
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  ok = rocksdb:close(Ref),

  rocksdb:destroy("erocksdb.file_size_default.test", []),
  ?rm_rf("erocksdb.file_size_default.test"),
  ok.

%% Test opening DB with target_file_size_is_upper_bound=true
target_file_size_is_upper_bound_true_test() ->
  ?rm_rf("erocksdb.file_size_upper.test"),
  {ok, Ref} = rocksdb:open("erocksdb.file_size_upper.test",
                           [{create_if_missing, true},
                            {target_file_size_is_upper_bound, true}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 100)],

  %% Verify data
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value100">>} = rocksdb:get(Ref, <<"key100">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.file_size_upper.test", []),
  ?rm_rf("erocksdb.file_size_upper.test"),
  ok.

%% Test with column families
target_file_size_is_upper_bound_cf_test() ->
  ?rm_rf("erocksdb.file_size_cf.test"),
  {ok, Ref, [_DefaultCF]} = rocksdb:open("erocksdb.file_size_cf.test",
                                          [{create_if_missing, true}],
                                          [{"default", [{target_file_size_is_upper_bound, true}]}]),
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.file_size_cf.test", []),
  ?rm_rf("erocksdb.file_size_cf.test"),
  ok.

%% Test combined with target_file_size_base
target_file_size_combined_test() ->
  ?rm_rf("erocksdb.file_size_combined.test"),
  {ok, Ref} = rocksdb:open("erocksdb.file_size_combined.test",
                           [{create_if_missing, true},
                            {target_file_size_base, 64 * 1024 * 1024},
                            {target_file_size_is_upper_bound, true}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 50)],

  %% Verify data
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value50">>} = rocksdb:get(Ref, <<"key50">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.file_size_combined.test", []),
  ?rm_rf("erocksdb.file_size_combined.test"),
  ok.
