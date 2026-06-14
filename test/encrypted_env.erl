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

%% Tests for the AES-256-CTR encrypted env.
%%
%% Note: wrong-key detection here is checksum-based, not authenticated. CTR
%% decryption with a wrong key yields garbage, which RocksDB's per-block
%% checksums reject as a corruption error. There is no MAC.
-module(encrypted_env).

-compile([export_all/1, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(rm_rf(Dir), rocksdb_test_util:rm_rf(Dir)).

%% A recognizable plaintext marker that must never appear on disk when the
%% data is encrypted.
-define(MARKER, <<"PLAINTEXT_MARKER_DEADBEEF_0123456789">>).
-define(KEY, <<"0123456789abcdef0123456789abcdef">>).  %% exactly 32 bytes

%% Options that keep the marker intact on disk in the unencrypted case:
%% disable compression so the marker bytes are not transformed, and flush
%% aggressively so an SST file is produced.
open_opts(Env) ->
    [{create_if_missing, true},
     {env, Env},
     {compression, none}].

key_validation_test() ->
    %% Wrong key length must be rejected.
    ?assertError(badarg, rocksdb:new_env({encrypted, <<"tooshort">>})),
    {ok, _Env} = rocksdb:new_env({encrypted, ?KEY}),
    ok.

roundtrip_test() ->
    Dir = "encrypted_env_roundtrip.test",
    ?rm_rf(Dir),
    {ok, Env} = rocksdb:new_env({encrypted, ?KEY}),
    {ok, Db} = rocksdb:open(Dir, open_opts(Env)),
    ok = rocksdb:put(Db, <<"k">>, ?MARKER, [{sync, true}]),
    ?assertEqual({ok, ?MARKER}, rocksdb:get(Db, <<"k">>, [])),
    %% Force a flush so the value lands in an SST file on disk.
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:close(Db),

    %% Reopen with the same key and read it back.
    {ok, Db2} = rocksdb:open(Dir, open_opts(Env)),
    ?assertEqual({ok, ?MARKER}, rocksdb:get(Db2, <<"k">>, [])),
    ok = rocksdb:close(Db2),
    ?rm_rf(Dir),
    ok.

on_disk_encrypted_test() ->
    Dir = "encrypted_env_ondisk.test",
    ?rm_rf(Dir),
    {ok, Env} = rocksdb:new_env({encrypted, ?KEY}),
    {ok, Db} = rocksdb:open(Dir, open_opts(Env)),
    ok = rocksdb:put(Db, <<"k">>, ?MARKER, [{sync, true}]),
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:close(Db),

    %% No file on disk may contain the plaintext marker.
    Files = filelib:wildcard(filename:join(Dir, "*")),
    ?assert(length(Files) > 0),
    lists:foreach(
        fun(F) ->
            case file:read_file(F) of
                {ok, Bin} ->
                    ?assertEqual(
                        nomatch,
                        binary:match(Bin, ?MARKER),
                        lists:flatten(
                            io_lib:format("plaintext marker found in ~s", [F])));
                _ ->
                    ok
            end
        end,
        Files),
    ?rm_rf(Dir),
    ok.

%% Sanity check: with the default (unencrypted) env the marker IS present on
%% disk, proving the on-disk check above is meaningful.
on_disk_plaintext_present_default_test() ->
    Dir = "encrypted_env_default.test",
    ?rm_rf(Dir),
    {ok, Db} = rocksdb:open(Dir, [{create_if_missing, true}, {compression, none}]),
    ok = rocksdb:put(Db, <<"k">>, ?MARKER, [{sync, true}]),
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:close(Db),
    Files = filelib:wildcard(filename:join(Dir, "*")),
    Found = lists:any(
        fun(F) ->
            case file:read_file(F) of
                {ok, Bin} -> binary:match(Bin, ?MARKER) =/= nomatch;
                _ -> false
            end
        end,
        Files),
    ?assert(Found),
    ?rm_rf(Dir),
    ok.

%% Reopening with a different key must fail (corruption / checksum error).
%% CTR with the wrong key produces garbage; block checksums detect it.
wrong_key_test() ->
    Dir = "encrypted_env_wrongkey.test",
    ?rm_rf(Dir),
    {ok, Env} = rocksdb:new_env({encrypted, ?KEY}),
    {ok, Db} = rocksdb:open(Dir, open_opts(Env)),
    ok = rocksdb:put(Db, <<"k">>, ?MARKER, [{sync, true}]),
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:close(Db),

    WrongKey = <<"FEDCBA9876543210FEDCBA9876543210">>,  %% 32 bytes, != KEY
    {ok, WrongEnv} = rocksdb:new_env({encrypted, WrongKey}),
    %% Either the open itself fails, or the subsequent read fails, depending on
    %% which on-disk structure is touched first. Both are corruption errors.
    Result =
        case rocksdb:open(Dir, open_opts(WrongEnv)) of
            {error, _} = OpenErr ->
                OpenErr;
            {ok, BadDb} ->
                R = rocksdb:get(BadDb, <<"k">>, []),
                try rocksdb:close(BadDb) catch _:_ -> ok end,
                R
        end,
    ?assertMatch({error, _}, Result),
    ?rm_rf(Dir),
    ok.

%% The default env path still works unchanged.
default_env_test() ->
    Dir = "encrypted_env_plain.test",
    ?rm_rf(Dir),
    {ok, Env} = rocksdb:new_env(default),
    {ok, Db} = rocksdb:open(Dir, [{create_if_missing, true}, {env, Env}]),
    ok = rocksdb:put(Db, <<"k">>, <<"v">>, []),
    ?assertEqual({ok, <<"v">>}, rocksdb:get(Db, <<"k">>, [])),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir),
    ok.

%% Map form #{encrypted => Key} works too.
map_form_test() ->
    Dir = "encrypted_env_map.test",
    ?rm_rf(Dir),
    {ok, Env} = rocksdb:new_env(#{encrypted => ?KEY}),
    {ok, Db} = rocksdb:open(Dir, open_opts(Env)),
    ok = rocksdb:put(Db, <<"k">>, ?MARKER, []),
    ?assertEqual({ok, ?MARKER}, rocksdb:get(Db, <<"k">>, [])),
    ok = rocksdb:close(Db),
    ?rm_rf(Dir),
    ok.
