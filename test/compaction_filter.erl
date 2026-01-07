%% Copyright (c) 2024-2026 Benoit Chesneau
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

-module(compaction_filter).

-include_lib("eunit/include/eunit.hrl").

%% Test declarative rules: key prefix filter
filter_key_prefix_test() ->
    DbPath = "compaction_filter_prefix.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        %% Configure for easier compaction triggering
        {write_buffer_size, 64 * 1024},  % Small write buffer
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{key_prefix, <<"tmp_">>}]
        }}
    ]),

    %% Write more data to trigger flush and compaction
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["tmp_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["keep_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and compact with force to ensure filter runs on all data
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Check results - tmp_ keys should be deleted
    TmpResult = rocksdb:get(Db, <<"tmp_key50">>, []),
    KeepResult = rocksdb:get(Db, <<"keep_key50">>, []),

    %% Log results for debugging
    io:format("tmp_key50 result: ~p~n", [TmpResult]),
    io:format("keep_key50 result: ~p~n", [KeepResult]),

    %% Verify keep keys are still there
    {ok, _} = KeepResult,

    %% tmp_ keys should be deleted after forced compaction
    not_found = TmpResult,

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test declarative rules: key suffix filter
filter_key_suffix_test() ->
    DbPath = "compaction_filter_suffix.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{key_suffix, <<"_expired">>}]
        }}
    ]),

    %% Write enough data to trigger compaction
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N), "_expired"]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N), "_active"]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and compact with force to ensure filter runs on all data
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Verify active keys are kept
    {ok, _} = rocksdb:get(Db, <<"key50_active">>, []),

    %% Expired keys should be deleted after forced compaction
    not_found = rocksdb:get(Db, <<"key50_expired">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test declarative rules: value empty filter
%% Tests that keys with empty values are deleted during compaction.
filter_value_empty_test() ->
    DbPath = "compaction_filter_empty.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{value_empty}]
        }}
    ]),

    %% Write empty values with small padding in key to ensure SST creation (with sync)
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["empty_key", integer_to_list(N), binary:copy(<<"p">>, 100)]),
        ok = rocksdb:put(Db, Key, <<>>, [{sync, true}])
    end, lists:seq(1, 100)),

    %% Write non-empty values (with sync)
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["nonempty_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [{sync, true}])
    end, lists:seq(1, 100)),

    %% Flush and wait a moment
    ok = rocksdb:flush(Db, []),
    timer:sleep(100),

    %% Compact specific key ranges (not the whole db)
    ok = rocksdb:compact_range(Db, <<"empty_key">>, <<"empty_key~">>, [{bottommost_level_compaction, force}]),
    ok = rocksdb:compact_range(Db, <<"nonempty_key">>, <<"nonempty_key~">>, [{bottommost_level_compaction, force}]),

    %% ASSERT: Verify non-empty values are kept
    {ok, _} = rocksdb:get(Db, <<"nonempty_key50">>, []),
    {ok, _} = rocksdb:get(Db, <<"nonempty_key1">>, []),

    %% ASSERT: Empty value keys should be deleted
    EmptyKey1 = iolist_to_binary(["empty_key1", binary:copy(<<"p">>, 100)]),
    EmptyKey50 = iolist_to_binary(["empty_key50", binary:copy(<<"p">>, 100)]),
    not_found = rocksdb:get(Db, EmptyKey1, []),
    not_found = rocksdb:get(Db, EmptyKey50, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test declarative rules: multiple rules
%% Tests that multiple rules can be combined. Each rule should match
%% different patterns (prefix, suffix, empty value).
filter_multiple_rules_test() ->
    DbPath = "compaction_filter_multi.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [
                {key_prefix, <<"tmp_">>},
                {key_suffix, <<"_old">>}
            ]
        }}
    ]),

    %% Write data that matches prefix rule
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["tmp_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Write data that matches suffix rule
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N), "_old"]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Write data that should be kept
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["keep_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"z">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and compact with force to ensure filter runs on all data
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Verify kept keys are still there
    {ok, _} = rocksdb:get(Db, <<"keep_key50">>, []),

    %% Matching keys should be deleted after forced compaction
    not_found = rocksdb:get(Db, <<"tmp_key50">>, []),
    not_found = rocksdb:get(Db, <<"key50_old">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test TTL from key - expired keys
%% The TTL is extracted from the first 8 bytes of the key (big-endian timestamp)
filter_ttl_from_key_test() ->
    DbPath = "compaction_filter_ttl.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{ttl_from_key, 0, 8, 1}]  % First 8 bytes = timestamp, 1 second TTL
        }}
    ]),

    %% Create keys with expired timestamps (10 seconds ago to ensure expiry)
    ExpiredTs = erlang:system_time(second) - 10,
    lists:foreach(fun(N) ->
        Key = <<ExpiredTs:64/big, "expired_data", (integer_to_binary(N))/binary>>,
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Create keys with valid timestamps (1 hour in future)
    ValidTs = erlang:system_time(second) + 3600,
    lists:foreach(fun(N) ->
        Key = <<ValidTs:64/big, "valid_data", (integer_to_binary(N))/binary>>,
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% ASSERT: Valid keys should remain
    ValidKey1 = <<ValidTs:64/big, "valid_data1">>,
    ValidKey50 = <<ValidTs:64/big, "valid_data50">>,
    {ok, _} = rocksdb:get(Db, ValidKey1, []),
    {ok, _} = rocksdb:get(Db, ValidKey50, []),

    %% ASSERT: Expired keys should be deleted
    ExpiredKey1 = <<ExpiredTs:64/big, "expired_data1">>,
    ExpiredKey50 = <<ExpiredTs:64/big, "expired_data50">>,
    not_found = rocksdb:get(Db, ExpiredKey1, []),
    not_found = rocksdb:get(Db, ExpiredKey50, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test Erlang callback mode - basic handler
%% This test verifies that the Erlang handler actually filters keys during compaction.
filter_erlang_handler_test() ->
    DbPath = "compaction_filter_handler.test",
    rocksdb_test_util:rm_rf(DbPath),

    %% Start handler that removes keys starting with "delete_"
    Self = self(),
    Handler = spawn_link(fun() -> filter_handler_loop(Self) end),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            handler => Handler,
            batch_size => 10,
            timeout => 5000  % 5 second timeout
        }}
    ]),

    %% Write data to delete
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["delete_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 50)),

    %% Write data to keep
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["keep_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 50)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Wait for handler to process and collect total processed count
    TotalProcessed = collect_handler_processed(0, 2000),
    io:format("Handler processed ~p total keys~n", [TotalProcessed]),

    %% Verify handler was invoked - should have processed at least some keys
    ?assert(TotalProcessed > 0),

    %% ASSERT: delete_ keys should be removed
    not_found = rocksdb:get(Db, <<"delete_key1">>, []),
    not_found = rocksdb:get(Db, <<"delete_key25">>, []),
    not_found = rocksdb:get(Db, <<"delete_key50">>, []),

    %% ASSERT: keep_ keys should still exist
    {ok, _} = rocksdb:get(Db, <<"keep_key1">>, []),
    {ok, _} = rocksdb:get(Db, <<"keep_key25">>, []),
    {ok, _} = rocksdb:get(Db, <<"keep_key50">>, []),

    %% Clean up
    Handler ! stop,
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Helper to collect all handler_processed messages
collect_handler_processed(Total, Timeout) ->
    receive
        {handler_processed, Count} ->
            collect_handler_processed(Total + Count, Timeout)
    after Timeout ->
        Total
    end.

filter_handler_loop(Parent) ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            Decisions = lists:map(fun({_Level, Key, _Value}) ->
                case Key of
                    <<"delete_", _/binary>> -> remove;
                    _ -> keep
                end
            end, Keys),
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            Parent ! {handler_processed, length(Keys)},
            filter_handler_loop(Parent);
        stop ->
            ok
    after 60000 ->
        Parent ! {handler_processed, 0}
    end.

%% Test timeout handling - handler that doesn't respond should not crash
filter_handler_timeout_test() ->
    DbPath = "compaction_filter_timeout.test",
    rocksdb_test_util:rm_rf(DbPath),

    %% Handler that never responds
    SlowHandler = spawn(fun() ->
        receive _ -> timer:sleep(infinity) end
    end),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            handler => SlowHandler,
            timeout => 100  % 100ms timeout
        }}
    ]),

    %% Write enough data to trigger compaction
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and force compaction - should NOT hang or crash
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Keys should still exist (timeout = keep)
    {ok, _} = rocksdb:get(Db, <<"key50">>, []),

    exit(SlowHandler, kill),
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test dead handler doesn't crash
filter_handler_dead_test() ->
    DbPath = "compaction_filter_dead.test",
    rocksdb_test_util:rm_rf(DbPath),

    %% Handler that dies immediately
    Handler = spawn(fun() -> ok end),
    timer:sleep(50),  % Ensure it's dead

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{handler => Handler}}
    ]),

    %% Write enough data to trigger compaction
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and force compaction - should NOT crash
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Keys preserved (dead handler = keep)
    {ok, _} = rocksdb:get(Db, <<"key50">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test handler change_value decision - handler can modify values during compaction
filter_handler_change_value_test() ->
    DbPath = "compaction_filter_change_value.test",
    rocksdb_test_util:rm_rf(DbPath),

    Self = self(),
    Handler = spawn_link(fun() -> change_value_handler_loop(Self) end),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            handler => Handler,
            batch_size => 10,
            timeout => 5000
        }}
    ]),

    %% Write data to transform
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["transform_key", integer_to_list(N)]),
        Value = iolist_to_binary(["original_value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 50)),

    %% Write data to keep unchanged
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["normal_key", integer_to_list(N)]),
        Value = iolist_to_binary(["normal_value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 50)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Wait for handler to process
    TotalProcessed = collect_handler_processed(0, 2000),
    io:format("Change value handler processed ~p total keys~n", [TotalProcessed]),
    ?assert(TotalProcessed > 0),

    %% ASSERT: transform_ keys should have modified values
    {ok, Value1} = rocksdb:get(Db, <<"transform_key1">>, []),
    ?assertEqual(<<"MODIFIED">>, Value1),
    {ok, Value25} = rocksdb:get(Db, <<"transform_key25">>, []),
    ?assertEqual(<<"MODIFIED">>, Value25),

    %% ASSERT: normal_ keys should have original values (unchanged)
    {ok, NormalValue} = rocksdb:get(Db, <<"normal_key1">>, []),
    ?assertMatch(<<"normal_value1", _/binary>>, NormalValue),

    Handler ! stop,
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

change_value_handler_loop(Parent) ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            Decisions = lists:map(fun({_Level, Key, _Value}) ->
                case Key of
                    <<"transform_", _/binary>> -> {change_value, <<"MODIFIED">>};
                    _ -> keep
                end
            end, Keys),
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            Parent ! {handler_processed, length(Keys)},
            change_value_handler_loop(Parent);
        stop ->
            ok
    after 60000 ->
        ok
    end.

%% Test that handler is actually invoked with all keys
filter_handler_invocation_test() ->
    DbPath = "compaction_filter_invocation.test",
    rocksdb_test_util:rm_rf(DbPath),

    Self = self(),
    %% Use ETS to track received keys
    Tab = ets:new(received_keys, [set, public]),
    Handler = spawn_link(fun() -> tracking_handler_loop(Self, Tab) end),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            handler => Handler,
            batch_size => 20,
            timeout => 5000
        }}
    ]),

    %% Write known keys - need enough data to trigger actual compaction
    ExpectedKeys = [iolist_to_binary(["key", integer_to_list(N)]) || N <- lists:seq(1, 100)],
    lists:foreach(fun(Key) ->
        Value = iolist_to_binary([Key, binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, ExpectedKeys),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Wait for handler
    TotalProcessed = collect_handler_processed(0, 2000),
    io:format("Tracking handler processed ~p keys~n", [TotalProcessed]),
    ?assert(TotalProcessed > 0),

    %% Verify all keys were seen by handler
    ReceivedKeys = [K || {K} <- ets:tab2list(Tab)],
    io:format("Received keys count: ~p~n", [length(ReceivedKeys)]),

    %% All expected keys should have been received
    lists:foreach(fun(Key) ->
        ?assert(lists:member(Key, ReceivedKeys))
    end, ExpectedKeys),

    Handler ! stop,
    ets:delete(Tab),
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

tracking_handler_loop(Parent, Tab) ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            %% Track all received keys
            lists:foreach(fun({_Level, Key, _Value}) ->
                ets:insert(Tab, {Key})
            end, Keys),
            %% Keep all keys
            Decisions = [keep || _ <- Keys],
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            Parent ! {handler_processed, length(Keys)},
            tracking_handler_loop(Parent, Tab);
        stop ->
            ok
    after 60000 ->
        ok
    end.

%% Test key_contains rule
filter_key_contains_test() ->
    DbPath = "compaction_filter_contains.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{key_contains, <<"_session_">>}]
        }}
    ]),

    %% Write keys containing _session_ pattern
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["user_session_", integer_to_list(N), "_data"]),
        Value = iolist_to_binary(["session_data", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Write keys NOT containing the pattern
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["user_data_", integer_to_list(N)]),
        Value = iolist_to_binary(["user_data", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% ASSERT: keys containing _session_ should be deleted
    not_found = rocksdb:get(Db, <<"user_session_50_data">>, []),
    not_found = rocksdb:get(Db, <<"user_session_1_data">>, []),

    %% ASSERT: keys NOT containing pattern should remain
    {ok, _} = rocksdb:get(Db, <<"user_data_50">>, []),
    {ok, _} = rocksdb:get(Db, <<"user_data_1">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test value_prefix rule
filter_value_prefix_test() ->
    DbPath = "compaction_filter_value_prefix.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{value_prefix, <<"DELETED:">>}]
        }}
    ]),

    %% Write keys with values starting with DELETED:
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["marked_key", integer_to_list(N)]),
        Value = iolist_to_binary(["DELETED:", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Write keys with normal values
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["normal_key", integer_to_list(N)]),
        Value = iolist_to_binary(["ACTIVE:", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% ASSERT: keys with DELETED: value prefix should be removed
    not_found = rocksdb:get(Db, <<"marked_key50">>, []),
    not_found = rocksdb:get(Db, <<"marked_key1">>, []),

    %% ASSERT: keys with normal values should remain
    {ok, _} = rocksdb:get(Db, <<"normal_key50">>, []),
    {ok, _} = rocksdb:get(Db, <<"normal_key1">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test always_delete rule
filter_always_delete_test() ->
    DbPath = "compaction_filter_always_delete.test",
    rocksdb_test_util:rm_rf(DbPath),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            rules => [{always_delete}]
        }}
    ]),

    %% Write various keys with sync to ensure durability
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [{sync, true}])
    end, lists:seq(1, 100)),

    %% Flush and wait a moment
    ok = rocksdb:flush(Db, []),
    timer:sleep(100),

    %% Compact specific key range (not the whole db)
    ok = rocksdb:compact_range(Db, <<"key">>, <<"key~">>, [{bottommost_level_compaction, force}]),

    %% ASSERT: ALL keys should be deleted
    not_found = rocksdb:get(Db, <<"key1">>, []),
    not_found = rocksdb:get(Db, <<"key50">>, []),
    not_found = rocksdb:get(Db, <<"key100">>, []),

    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

%% Test handler with invalid decisions - should default to keep
filter_handler_invalid_decision_test() ->
    DbPath = "compaction_filter_invalid.test",
    rocksdb_test_util:rm_rf(DbPath),

    Self = self(),
    Handler = spawn_link(fun() -> invalid_decision_handler_loop(Self) end),

    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {write_buffer_size, 64 * 1024},
        {level0_file_num_compaction_trigger, 1},
        {compaction_filter, #{
            handler => Handler,
            batch_size => 10,
            timeout => 5000
        }}
    ]),

    %% Write data
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, Key, Value, [])
    end, lists:seq(1, 50)),

    %% Flush and force compaction
    ok = rocksdb:flush(Db, []),
    ok = rocksdb:compact_range(Db, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Wait for handler
    TotalProcessed = collect_handler_processed(0, 2000),
    io:format("Invalid decision handler processed ~p keys~n", [TotalProcessed]),

    %% ASSERT: All keys should be kept (invalid decisions default to keep)
    {ok, _} = rocksdb:get(Db, <<"key1">>, []),
    {ok, _} = rocksdb:get(Db, <<"key25">>, []),
    {ok, _} = rocksdb:get(Db, <<"key50">>, []),

    Handler ! stop,
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

invalid_decision_handler_loop(Parent) ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            %% Return invalid decisions - should default to keep
            Decisions = [invalid_decision || _ <- Keys],
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            Parent ! {handler_processed, length(Keys)},
            invalid_decision_handler_loop(Parent);
        stop ->
            ok
    after 60000 ->
        ok
    end.

%% Test compaction filter with column families
filter_column_family_test() ->
    DbPath = "compaction_filter_cf.test",
    rocksdb_test_util:rm_rf(DbPath),

    %% First create the DB with column families
    {ok, Db0} = rocksdb:open(DbPath, [{create_if_missing, true}]),
    {ok, _CF1} = rocksdb:create_column_family(Db0, "cf1", []),
    {ok, _CF2} = rocksdb:create_column_family(Db0, "cf2", []),
    ok = rocksdb:close(Db0),

    %% Reopen with CF-specific filters
    Self = self(),
    Handler = spawn_link(fun() -> cf_filter_handler_loop(Self) end),

    CF1Opts = [
        {write_buffer_size, 64 * 1024},
        {compaction_filter, #{rules => [{key_prefix, <<"tmp_">>}]}}
    ],
    CF2Opts = [
        {write_buffer_size, 64 * 1024},
        {compaction_filter, #{handler => Handler, batch_size => 10, timeout => 5000}}
    ],

    {ok, Db, [_DefaultCF, CF1, CF2]} = rocksdb:open_with_cf(DbPath, [
        {create_if_missing, true}
    ], [
        {"default", []},
        {"cf1", CF1Opts},
        {"cf2", CF2Opts}
    ]),

    %% Write to CF1 (rule-based filter) - need enough data for actual compaction
    lists:foreach(fun(N) ->
        TmpKey = iolist_to_binary(["tmp_key", integer_to_list(N)]),
        KeepKey = iolist_to_binary(["keep_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"x">>, 1000)]),
        ok = rocksdb:put(Db, CF1, TmpKey, Value, []),
        ok = rocksdb:put(Db, CF1, KeepKey, Value, [])
    end, lists:seq(1, 100)),

    %% Write to CF2 (handler-based filter removes old_ prefix)
    lists:foreach(fun(N) ->
        OldKey = iolist_to_binary(["old_key", integer_to_list(N)]),
        NewKey = iolist_to_binary(["new_key", integer_to_list(N)]),
        Value = iolist_to_binary(["value", integer_to_list(N), binary:copy(<<"y">>, 1000)]),
        ok = rocksdb:put(Db, CF2, OldKey, Value, []),
        ok = rocksdb:put(Db, CF2, NewKey, Value, [])
    end, lists:seq(1, 100)),

    %% Flush and compact both CFs
    ok = rocksdb:flush(Db, CF1, []),
    ok = rocksdb:flush(Db, CF2, []),
    ok = rocksdb:compact_range(Db, CF1, undefined, undefined, [{bottommost_level_compaction, force}]),
    ok = rocksdb:compact_range(Db, CF2, undefined, undefined, [{bottommost_level_compaction, force}]),

    %% Wait for handler
    _ = collect_handler_processed(0, 2000),

    %% ASSERT CF1: tmp_ keys deleted, keep_ keys remain
    not_found = rocksdb:get(Db, CF1, <<"tmp_key15">>, []),
    {ok, _} = rocksdb:get(Db, CF1, <<"keep_key15">>, []),

    %% ASSERT CF2: old_ keys deleted by handler, new_ keys remain
    not_found = rocksdb:get(Db, CF2, <<"old_key15">>, []),
    {ok, _} = rocksdb:get(Db, CF2, <<"new_key15">>, []),

    Handler ! stop,
    ok = rocksdb:close(Db),
    ok = destroy_and_rm(DbPath).

cf_filter_handler_loop(Parent) ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            Decisions = lists:map(fun({_Level, Key, _Value}) ->
                case Key of
                    <<"old_", _/binary>> -> remove;
                    _ -> keep
                end
            end, Keys),
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            Parent ! {handler_processed, length(Keys)},
            cf_filter_handler_loop(Parent);
        stop ->
            ok
    after 60000 ->
        ok
    end.

%% Helper function
destroy_and_rm(DbPath) ->
    rocksdb:destroy(DbPath, []),
    rocksdb_test_util:rm_rf(DbPath).
