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

-module(background_work).

-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% Test pause and continue background work
pause_continue_background_work_test() ->
  ?rm_rf("erocksdb.background_work.test"),
  {ok, Ref} = rocksdb:open("erocksdb.background_work.test", [{create_if_missing, true}]),

  %% Write some data
  ok = rocksdb:put(Ref, <<"key1">>, <<"value1">>, []),
  ok = rocksdb:put(Ref, <<"key2">>, <<"value2">>, []),

  %% Pause background work
  ok = rocksdb:pause_background_work(Ref),

  %% Write more data while paused
  ok = rocksdb:put(Ref, <<"key3">>, <<"value3">>, []),

  %% Resume background work
  ok = rocksdb:continue_background_work(Ref),

  %% Verify all data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value2">>} = rocksdb:get(Ref, <<"key2">>, []),
  {ok, <<"value3">>} = rocksdb:get(Ref, <<"key3">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.background_work.test", []),
  ?rm_rf("erocksdb.background_work.test"),
  ok.

%% Test disable and enable manual compaction
disable_enable_manual_compaction_test() ->
  ?rm_rf("erocksdb.manual_compaction.test"),
  {ok, Ref} = rocksdb:open("erocksdb.manual_compaction.test", [{create_if_missing, true}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 100)],

  %% Disable manual compaction
  ok = rocksdb:disable_manual_compaction(Ref),

  %% Re-enable manual compaction
  ok = rocksdb:enable_manual_compaction(Ref),

  %% Now compact_range should work
  ok = rocksdb:compact_range(Ref, undefined, undefined, []),

  %% Verify data is still readable
  {ok, <<"value1">>} = rocksdb:get(Ref, <<"key1">>, []),
  {ok, <<"value100">>} = rocksdb:get(Ref, <<"key100">>, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.manual_compaction.test", []),
  ?rm_rf("erocksdb.manual_compaction.test"),
  ok.

%% Test that compact_range is blocked when manual compaction is disabled
compaction_blocked_when_disabled_test() ->
  ?rm_rf("erocksdb.compaction_blocked.test"),
  {ok, Ref} = rocksdb:open("erocksdb.compaction_blocked.test", [{create_if_missing, true}]),

  %% Write some data
  [ok = rocksdb:put(Ref, <<"key", (integer_to_binary(I))/binary>>,
                    <<"value", (integer_to_binary(I))/binary>>, [])
   || I <- lists:seq(1, 50)],

  %% Disable manual compaction
  ok = rocksdb:disable_manual_compaction(Ref),

  %% Try to compact - should return an error indicating compaction is disabled
  Result = rocksdb:compact_range(Ref, undefined, undefined, []),
  %% When manual compaction is disabled, compact_range returns an error
  ?assertMatch({error, _}, Result),

  %% Re-enable manual compaction
  ok = rocksdb:enable_manual_compaction(Ref),

  %% Now compact_range should work
  ok = rocksdb:compact_range(Ref, undefined, undefined, []),

  ok = rocksdb:close(Ref),
  rocksdb:destroy("erocksdb.compaction_blocked.test", []),
  ?rm_rf("erocksdb.compaction_blocked.test"),
  ok.
