%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
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

%% Test various scenarios that properly and improperly close LevelDB DB/iterator
%% handles and ensure everything cleans up properly.

-module(cleanup).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(COMMON_INSTANCE_DIR, "/tmp/erocksdb.cleanup.test").

-define(CF_INSTANCE_DIR, "/tmp/erocksdb_CF.cleanup.test").


%% Purposely reopen an already opened database to test failure assumption
assumption_test() ->
  os:cmd("rm -rf " ++ ?COMMON_INSTANCE_DIR),
  DB = open(),
  try
    io:format(user, "assumption_test: top\n", []),
    ok = failed_open(),
    io:format(user, "assumption_test: bottom\n", []),
    ok
  after
    rocksdb:close(DB),
    timer:sleep(500)
  end,
  rocksdb:destroy(?COMMON_INSTANCE_DIR, []).

%% Open/close
open_close_test() ->
  DB = open(),
  rocksdb:close(DB),
  check().

%% Open w/o close
open_exit_test() ->
  spawn_wait(fun() ->
             _DB = open()
         end),
  timer:sleep(500),
  check(),
  rocksdb:destroy(?COMMON_INSTANCE_DIR, []).

%% Iterator open/close
iterator_test() ->
  DB = open(),
  try
    write(100, DB),
    {ok, Itr} = rocksdb:iterator(DB, []),
    iterate(Itr),
    rocksdb:iterator_close(Itr),
    rocksdb:close(DB),
    check(),
    ok
  after
    catch rocksdb:close(DB),
    timer:sleep(500)
  end,
  rocksdb:destroy(?COMMON_INSTANCE_DIR, []).

%% Close DB while iterator running
%% Expected: reopen should fail while iterator reference alive
%%           however, iterator should fail after DB is closed
%%           once iterator process exits, open should succeed
iterator_db_close_test() ->
  DB = open(),
  try
    write(100, DB),
    Parent = self(),
    spawn_monitor(fun() ->
                {ok, Itr} = rocksdb:iterator(DB, []),
                Parent ! continue,
                try
                  iterate(Itr, 10)
                catch
                  error:badarg ->
                    ok
                end,
                try
                  rocksdb:iterator_close(Itr)
                catch
                  error:badarg ->
                    ok
                end
            end),
    receive continue -> ok end,
    rocksdb:close(DB),
    %%failed_open(),
    wait_down(),
    erlang:garbage_collect(),
    timer:sleep(500),
    check(),
    ok
  after
    catch rocksdb:close(DB),
    timer:sleep(500)
  end,
  rocksdb:destroy(?COMMON_INSTANCE_DIR, []).

%% Iterate open, iterator process exit w/o close
iterator_exit_test() ->
  DB = open(),
  try
    write(100, DB),
    spawn_wait(fun() ->
               {ok, Itr} = rocksdb:iterator(DB, []),
               iterate(Itr)
           end),
    rocksdb:close(DB),
    check(),
    ok
  after
    catch rocksdb:close(DB),
    timer:sleep(500)
  end,
  rocksdb:destroy(?COMMON_INSTANCE_DIR, []).


iterator_with_cf_exit_test() ->
  os:cmd("rm -rf " ++ ?CF_INSTANCE_DIR),
  {ok, DB, _} = open_cf(),
  {ok, CF} = rocksdb:create_column_family(DB, "test", []),
  try
    write(100, DB, CF),
    spawn_wait(fun() ->
      {ok, Itr} = rocksdb:iterator(DB, CF, []),
      iterate(Itr)
               end),
    rocksdb:close(DB),
    check_cf(),
    ok
  after
    catch rocksdb:close(DB),
    timer:sleep(500)
  end,
  rocksdb:destroy(?CF_INSTANCE_DIR, []).


spawn_wait(F) ->
  spawn_monitor(F),
  wait_down().

wait_down() ->
  receive {'DOWN', _, process, _, _} ->
      ok
  end.

check() ->
  timer:sleep(500),
  DB = open(),
  rocksdb:close(DB),
  timer:sleep(500),
  ok.

check_cf() ->
  timer:sleep(500),
  {ok, DB, _CFs} = open_cf(),
  rocksdb:close(DB),
  timer:sleep(500),
  ok.


open() ->
  {ok, Ref} = rocksdb:open(?COMMON_INSTANCE_DIR,
                [{create_if_missing, true}]),
  Ref.


open_cf() ->
  case filelib:is_dir(?CF_INSTANCE_DIR) of
    true ->
      {ok, CFs} = rocksdb:list_column_families(?CF_INSTANCE_DIR, []),
      rocksdb:open_with_cf(
        ?CF_INSTANCE_DIR,
        [{create_if_missing, true}],
        [{CF, []} || CF <- CFs]
      );
    false ->
      {ok, DB}= rocksdb:open( ?CF_INSTANCE_DIR, [{create_if_missing, true}]),
      {ok, DB, []}
  end.


failed_open() ->
  {error, {db_open, _}} = rocksdb:open(?COMMON_INSTANCE_DIR,
                      [{create_if_missing, true}]),
  ok.

write(N, DB) ->
  write(0, N, DB, nil).

write(N, DB, CF) ->
  write(0, N, DB, CF).


write(Same, Same, _DB, _CF) ->
  ok;
write(N, End, DB, nil) ->
  rocksdb:put(DB, <<N:64/integer>>, <<N:64/integer>>, []),
  write(N+1, End, DB, nil);
write(N, End, DB, CF) ->
  rocksdb:put(DB, CF, <<N:64/integer>>, <<N:64/integer>>, []),
  write(N+1, End, DB, CF).

iterate(Itr) ->
  iterate(Itr, 0).
iterate(Itr, Delay) ->
  do_iterate(rocksdb:iterator_move(Itr, <<0:64/integer>>), {Itr, 0, Delay}).

do_iterate({error, invalid_iterator}, _) ->
  ok;
do_iterate({ok, K, _V}, {Itr, Expected, Delay}) ->
  <<N:64/integer>> = K,
  ?assertEqual(Expected, N),
  (Delay == 0) orelse timer:sleep(Delay),
  do_iterate(rocksdb:iterator_move(Itr, next),
         {Itr, Expected + 1, Delay}).
