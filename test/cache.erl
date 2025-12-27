-module(cache).


-include_lib("eunit/include/eunit.hrl").

cache_info_test() ->
  {ok, CHandle} = rocksdb:new_cache(lru, 101400),
  ok = rocksdb:set_strict_capacity_limit(CHandle, true),
  [{capacity,101400},
   {strict_capacity,true},
   {usage,0},
   {pinned_usage,0}] = rocksdb:cache_info(CHandle),
  ok = rocksdb:release_cache(CHandle).

%% Test clock cache (uses HyperClockCache internally in RocksDB 10.x)
clock_cache_test() ->
  {ok, CHandle} = rocksdb:new_cache(clock, 4 * 1024 * 1024),
  ?assert(is_reference(CHandle)),
  ?assertEqual(4 * 1024 * 1024, rocksdb:cache_info(CHandle, capacity)),
  ok = rocksdb:release_cache(CHandle).
