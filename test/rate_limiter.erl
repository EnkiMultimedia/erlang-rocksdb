
%% Copyright (c) 2018 Sergey Yelin
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
-module(rate_limiter).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

simple_rate_limiter_test() ->
  {ok, Limiter} = rocksdb:new_rate_limiter(83886080, true),
  {ok, Ref} = rocksdb:open("/tmp/rocksdb_rate_limiter.test",
    [{create_if_missing, true},{rate_limiter, Limiter}]),
  Key = <<"key">>,
  OriginalData = <<"sample_data">>,
  ok = rocksdb:put(Ref, Key, OriginalData, []),
  {ok, Data} = rocksdb:get(Ref, Key, []),
  ?assertEqual(Data, OriginalData),
  ok = rocksdb:close(Ref),
  ok = rocksdb:destroy("/tmp/rocksdb_rate_limiter.test", []),
  ok = rocksdb:release_rate_limiter(Limiter).
