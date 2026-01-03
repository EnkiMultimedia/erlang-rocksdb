%%% -*- erlang -*-
%%
%% Copyright (c) 2018-2020 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
-module(sst_file_manager).

-include_lib("eunit/include/eunit.hrl").



basic_test() ->
    {ok, Env} = rocksdb:new_env(),
    {ok, Mgr} = rocksdb:new_sst_file_manager(Env),
    0.25 = rocksdb:sst_file_manager_info(Mgr, max_trash_db_ratio),
    0 = rocksdb:sst_file_manager_info(Mgr, delete_rate_bytes_per_sec),
    ok = rocksdb:sst_file_manager_flag(Mgr, delete_rate_bytes_per_sec, 1024),
    1024 = rocksdb:sst_file_manager_info(Mgr, delete_rate_bytes_per_sec),
    [{total_size, _},
     {delete_rate_bytes_per_sec, 1024},
     {max_trash_db_ratio, 0.25},
     {total_trash_size, _},
     {is_max_allowed_space_reached, _},
     {max_allowed_space_reached_including_compactions, _}] = rocksdb:sst_file_manager_info(Mgr),
    ok = rocksdb:release_sst_file_manager(Mgr),
    {ok, Mgr2} = rocksdb:new_sst_file_manager(Env, [{max_trash_db_ratio, 0.30}]),
    0.30 = rocksdb:sst_file_manager_info(Mgr2, max_trash_db_ratio),
    ok = rocksdb:release_sst_file_manager(Mgr2).

simple_options_test() ->
  {ok, Env} = rocksdb:default_env(),
  {ok, Mgr} = rocksdb:new_sst_file_manager(Env),
  Options = [{create_if_missing, true}, {env, Env}, {sst_file_manager, Mgr}],

  {ok, Ref} = rocksdb:open("rocksdb_sst_file_mgr.test", Options),
  Key = <<"key">>,
  OriginalData = <<"sample_data">>,
  ok = rocksdb:put(Ref, Key, OriginalData, []),
  {ok, Data} = rocksdb:get(Ref, Key, []),
  ?assertEqual(Data, OriginalData),
  ok = rocksdb:close(Ref),
  ok = rocksdb:release_sst_file_manager(Mgr),
  ok = rocksdb:destroy_env(Env),
  ok = rocksdb:destroy("rocksdb_sst_file_mgr.test", []),
  ok = rocksdb_test_util:rm_rf("rocksdb_sst_file_mgr.test"),
  ok.

tracked_files_test() ->
  {ok, Env} = rocksdb:default_env(),
  {ok, Mgr} = rocksdb:new_sst_file_manager(Env),
  Options = [{create_if_missing, true}, {env, Env}, {sst_file_manager, Mgr}],
  {ok, Db} = rocksdb:open("rocksdb_tracked_files.test", Options),

  %% Write some data to create SST files
  [ok = rocksdb:put(Db, <<"key", (integer_to_binary(I))/binary>>,
                    crypto:strong_rand_bytes(1000), [])
   || I <- lists:seq(1, 100)],
  ok = rocksdb:flush(Db, []),

  %% Check tracked files
  TrackedFiles = rocksdb:sst_file_manager_tracked_files(Mgr),
  ?assert(is_list(TrackedFiles)),
  %% After flush, there should be at least one SST file
  ?assert(length(TrackedFiles) > 0),
  lists:foreach(fun({Path, Size}) ->
      ?assert(is_binary(Path)),
      ?assert(is_integer(Size)),
      ?assert(Size > 0)
  end, TrackedFiles),

  ok = rocksdb:close(Db),
  ok = rocksdb:release_sst_file_manager(Mgr),
  ok = rocksdb:destroy_env(Env),
  ok = rocksdb:destroy("rocksdb_tracked_files.test", []),
  ok = rocksdb_test_util:rm_rf("rocksdb_tracked_files.test"),
  ok.
