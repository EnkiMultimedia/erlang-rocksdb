-module(get_property).
-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

-define(DB, "test_getproperty.db").

%% Test get_property/2 (default CF) works
default_cf_test() ->
    destroy_and_rm(?DB, []),
    {ok, Db} = rocksdb:open(?DB, [{create_if_missing, true}]),
    ok = rocksdb:put(Db, <<"k1">>, <<"v1">>, []),
    {ok, Val} = rocksdb:get_property(Db, <<"rocksdb.estimate-num-keys">>),
    ?assert(binary_to_integer(Val) >= 1),
    rocksdb:close(Db),
    destroy_and_rm(?DB, []),
    ok.

%% Test get_property/3 returns correct per-CF count (the core bug)
cf_property_test() ->
    destroy_and_rm(?DB, []),
    CFs = [{"default", []}, {"data", []}],
    {ok, Db, [DefaultH, DataH]} = rocksdb:open(?DB,
        [{create_if_missing, true}, {create_missing_column_families, true}], CFs),

    %% Write 5 keys to "data" CF, nothing to "default"
    [ok = rocksdb:put(Db, DataH, <<"k", (N + $0)>>, <<"v">>, [])
     || N <- lists:seq(1, 5)],
    ok = rocksdb:flush(Db, DataH, [{wait, true}]),

    %% get_property/3 on "data" CF must reflect the 5 keys
    {ok, DataCount} = rocksdb:get_property(Db, DataH, <<"rocksdb.estimate-num-keys">>),
    ?assert(binary_to_integer(DataCount) >= 5),

    %% get_property/3 on "default" CF should be 0
    {ok, DefaultCount} = rocksdb:get_property(Db, DefaultH, <<"rocksdb.estimate-num-keys">>),
    ?assertEqual(0, binary_to_integer(DefaultCount)),

    rocksdb:close(Db),
    destroy_and_rm(?DB, []),
    ok.

destroy_and_rm(Dir, Options) ->
    rocksdb:destroy(Dir, Options),
    rocksdb_test_util:rm_rf(Dir).
