A database handle is a shared, refcounted nif resource, not a process. Any process holding the handle term can use it, and every blocking call runs on a dirty scheduler, so calls from many processes proceed in parallel. What you do need is somewhere to keep the handle for the lifetime of your application, because the database closes as soon as nothing references it any more. Read this when you are wiring `rocksdb` into an OTP application and you have to decide where the handle lives and who closes it.

## Own the lifetime, not the calls

Put one process in charge of opening and closing the database, and let every other process call `rocksdb` directly. Do not route database calls through that process: a `gen_server` in the data path serializes work that the nif already runs concurrently.

```erlang
-module(mydb).
-behaviour(gen_server).

-export([start_link/0, handle/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(HANDLE, {?MODULE, handle}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Every caller reads the handle through this function.
handle() ->
    persistent_term:get(?HANDLE).

init([]) ->
    %% trap exits so terminate/2 runs on a supervisor shutdown
    process_flag(trap_exit, true),
    {ok, Path} = application:get_env(myapp, db_path),
    {ok, Db} = rocksdb:open(Path, [{create_if_missing, true}]),
    persistent_term:put(?HANDLE, Db),
    {ok, #{db => Db}}.

handle_call(_Request, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, #{db := Db}) ->
    persistent_term:erase(?HANDLE),
    ok = rocksdb:close(Db).
```

Start it from your top-level supervisor:

```erlang
init([]) ->
    Children = [
        #{id => mydb,
          start => {mydb, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker}
    ],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.
```

Then use it from anywhere:

```erlang
ok = rocksdb:put(mydb:handle(), <<"k">>, <<"v">>, []),
{ok, <<"v">>} = rocksdb:get(mydb:handle(), <<"k">>, []).
```

Notes:

* Put the owner first in the child list and use `rest_for_one` if your workers cache anything derived from the handle, so they restart with it.
* You do not need a separate supervisor per database. Add one only when a database also owns a pool or other children that must restart with it.

## Where to keep the handle

Use `persistent_term` when the set of databases is fixed at boot. Reads are free, and a handle written once at startup never pays the write cost.

```erlang
persistent_term:put({mydb, handle}, Db),
Db = persistent_term:get({mydb, handle}).
```

Use ETS when databases come and go at runtime, because every `persistent_term:put/2` and `erase/1` triggers a global garbage collection scan.

```erlang
%% in the owner's init/1
_ = ets:new(mydb_registry, [named_table, set, protected, {read_concurrency, true}]),
true = ets:insert(mydb_registry, {Name, Db}),

%% in any caller
[{Name, Db}] = ets:lookup(mydb_registry, Name).
```

An ETS table owned by the owner process disappears when that process dies, so callers fail immediately instead of reading a handle to a closed database. A `persistent_term` entry outlives the owner, which is why the example erases it in `terminate/2`.

## Several collections

Prefer column families in one database over several databases. You get atomic writes across collections, one set of files to manage and one handle to publish.

```erlang
CFs = [{"default", []}, {"users", []}, {"events", []}],
DbOpts = [{create_if_missing, true}, {create_missing_column_families, true}],
{ok, Db, [_Default, Users, Events]} = rocksdb:open(Path, DbOpts, CFs),
persistent_term:put({mydb, handle}, #{db => Db, users => Users, events => Events}).
```

To find out what a database already contains before opening it with column families:

```erlang
{ok, Names} = rocksdb:list_column_families(Path, []).
```

Add one at runtime with `rocksdb:create_column_family/3`, and keep the returned handle next to the database handle. See [Column Families](column_families.md) for the read and write functions that take a cf handle.

## What to watch out for

* Keep the handle reachable. If the last reference to it is garbage collected, the database is closed and its iterators and column families go with it.
* After `rocksdb:close/1`, calls from any other holder of that handle raise `badarg`, they do not return an error tuple. Read the handle from `persistent_term` or ETS on each call rather than caching it in your own process state, so an owner restart does not leave you holding a dead handle.
* Calls on one iterator, transaction or SST file writer from several processes are safe, but they run one at a time. Create one iterator per process that needs to iterate, and close it with `rocksdb:iterator_close/1` when you are done.
* Do not open the same directory twice. RocksDB takes a file lock, so while the first handle is alive a second `rocksdb:open/2` returns `{error, {db_open, "IO error: lock hold by current process..."}}`. One owner per directory.
* `rocksdb:close/1` returns once the database is closed and its lock released, so you can open the same directory again right after. It waits for calls still running on the database, such as a long `rocksdb:compact_range/4`: give the owner a shutdown timeout long enough for them.
