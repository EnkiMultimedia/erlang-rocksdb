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

-module(manifest_options).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test opening DB with max_manifest_space_amp_pct option
max_manifest_space_amp_pct_test() ->
  ?rm_rf("erocksdb.manifest_pct.test"),
  %% Open with default value (500)
  {ok, Ref1} = rocksdb:open("erocksdb.manifest_pct.test",
                            [{create_if_missing, true},
                             {max_manifest_space_amp_pct, 500}]),
  ok = rocksdb:put(Ref1, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref1, <<"key1">>, []),
  ok = rocksdb:close(Ref1),

  %% Re-open with different value (100 for lower space amp)
  {ok, Ref2} = rocksdb:open("erocksdb.manifest_pct.test",
                            [{max_manifest_space_amp_pct, 100}]),
  {ok, <<"value1">>} = rocksdb:get(Ref2, <<"key1">>, []),
  ok = rocksdb:put(Ref2, <<"key2">>, <<"value2">>, []),
  ok = rocksdb:close(Ref2),

  rocksdb:destroy("erocksdb.manifest_pct.test", []),
  ?rm_rf("erocksdb.manifest_pct.test"),
  ok.

%% Test opening DB with high max_manifest_space_amp_pct (lower write amp)
max_manifest_space_amp_pct_high_test() ->
  ?rm_rf("erocksdb.manifest_pct_high.test"),
  %% Open with high value (10000 for very low write amp)
  {ok, Ref} = rocksdb:open("erocksdb.manifest_pct_high.test",
                           [{create_if_missing, true},
                            {max_manifest_space_amp_pct, 10000}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 100)],

  %% Verify data
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value100">>} = rocksdb:get(Ref, <<"key100">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.manifest_pct_high.test", []),
  ?rm_rf("erocksdb.manifest_pct_high.test"),
  ok.

%% Test with column families
max_manifest_space_amp_pct_cf_test() ->
  ?rm_rf("erocksdb.manifest_pct_cf.test"),
  {ok, Ref, [_DefaultCF]} = rocksdb:open("erocksdb.manifest_pct_cf.test",
                                          [{create_if_missing, true},
                                           {max_manifest_space_amp_pct, 200}],
                                          [{"default", []}]),
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.manifest_pct_cf.test", []),
  ?rm_rf("erocksdb.manifest_pct_cf.test"),
  ok.
