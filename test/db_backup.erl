%% Copyright (c) 2017-2018 Benoit Chesneau. All Rights Reserved.
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

-module(db_backup).
-author("benoitc").

-export([
  simple_test/0,
  restore_latest_test/0
]).

-define(source, "/tmp/testsource").
-define(target, "/tmp/testtarget").
-define(latest, "/tmp/testatest").

simple_test() ->
  os:cmd("rm -rf " ++ ?source),
  os:cmd("rm -rf " ++ ?target),
  DB = open(),
  Backup = open_backup(),
  write(50, DB),

  ok = rocksdb:create_new_backup(Backup, DB),
  write(50, 100, DB),
  ok = rocksdb:create_new_backup(Backup, DB),

  {ok, BackupInfos} = rocksdb:get_backup_info(Backup),
  BackupsIds = lists:sort([Id || #{ backup_id := Id} <- BackupInfos]),
  [1, 2] =  lists:sort(BackupsIds),
  ok = rocksdb:verify_backup(Backup, 1),
  ok = rocksdb:verify_backup(Backup, 2),

  rocksdb:close_backup(Backup),
  rocksdb:close(DB),
  rocksdb:destroy(?source, []),
  rocksdb:destroy(?target, []),
  ok.

restore_latest_test() ->
  os:cmd("rm -rf " ++ ?source),
  os:cmd("rm -rf " ++ ?target),
  os:cmd("rm -rf " ++ ?latest),

  DB = open(),
  Backup = open_backup(),
  write(50, DB),

  ok = rocksdb:create_new_backup(Backup, DB),
  write(50, 100, DB),
  ok = rocksdb:create_new_backup(Backup, DB),

  {ok, BackupInfos} = rocksdb:get_backup_info(Backup),
  BackupsIds = lists:sort([Id || #{ backup_id := Id} <- BackupInfos]),
  [1, 2] =  lists:sort(BackupsIds),
  ok = rocksdb:verify_backup(Backup, 1),
  ok = rocksdb:verify_backup(Backup, 2),

  rocksdb:close_backup(Backup),
  rocksdb:close(DB),

  timer:sleep(500),
  Backup2 = open_backup(),
  rocksdb:restore_db_from_latest_backup(Backup2, ?latest),
  LatestDb = open_latest(),


  100 = rocksdb:count(LatestDb),

  rocksdb:close_backup(Backup2),
  rocksdb:close(LatestDb),

  rocksdb:destroy(?source, []),
  rocksdb:destroy(?target, []),
  rocksdb:destroy(?source, []),
  rocksdb:destroy(?latest, []),
  ok.

open() ->
  {ok, Ref} = rocksdb:open(?source, [{create_if_missing, true}]),
  Ref.

open_backup() ->
  {ok, Ref} = rocksdb:open_backup_engine(?target),
  Ref.

open_latest() ->
  {ok, Ref} = rocksdb:open(?latest, []),
  Ref.


write(N, DB) ->
  write(0, N, DB).


write(Same, Same, _DB) ->
  ok;
write(N, End, DB) ->
  rocksdb:put(DB, <<N:64/integer>>, <<N:64/integer>>, []),
  write(N+1, End, DB).
