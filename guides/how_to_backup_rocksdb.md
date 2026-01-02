## Backup API

It's possible to setup your Erlang application to handle incremental backup directly using the `backup*` functions from the API.


## Creating and verifying a backup

```erlang

Path = "/tmp/erocksdb.fold.test",
Options = [{create_if_missing, true}],
{ok, DB} = rocksdb:open(Path, Options).

BackupDir = "/tmp/rocksdb_backup",

{ok, Backup} = rocksdb:open_backup_engine(BackupDir),

rocksdb:put(...), %% do your thing

ok = rocksdb:create_new_backup(Backup, DB),

rocksdb:put(...), %% make some more changes
ok = rocksdb:create_new_backup(Backup, DB),

%% you can get IDs from backup_info if there are more than two
{ok, BackupInfos} = rocksdb:get_backup_info(Backup),
  
ok = rocksdb:verify_backup(Backup, 1),
ok = rocksdb:verify_backup(Backup, 2),

rocksdb:close_backup_engine(Backup),
rocksdb:close(DB).

```

This simple example will create a couple backups in "/tmp/rocksdb_backup". Note that you can create and verify multiple backups using the same engine. **Backups are incremental.**

> more options will be supported in near future.

Once you have some backups saved, you can issue `rocksdb:get_backup_info/1` call to get a list of all backups together with information on timestamp of the backup and the size (please note that sum of all backups' sizes is bigger than the actual size of the backup directory because some data is shared by multiple backups). Backups are identified by their always-increasing IDs.

When `rocksdb:verify_backup/2` is called, it checks the file sizes in the backup directory against the original sizes of the corresponding files in the db directory. However, we do not verify checksums since it would require reading all the data. Note that the only valid use case for `rocksdb:verify_backup/2` is invoking it on a backup engine after that same engine was used for creating backup(s) because it uses state captured during backup time.

## Restoring a backup

Restoring is easy:

```erlang 
{ok, Backup2} = rocksdb:open_backup_engine("/tmp/rocksdb_backup"),
ok = rocksdb:restore_db_from_backup(Backup2, 1, "/tmp/rocksdb", "/tmp/rocksdb"),
```

This code will restore the first backup back to "/tmp/rocksdb". The first parameter of `rocksdb:restore_db_from_backup/4` is the backup ID, second is target DB directory, and third is the target location of log files (in some DBs they are different from DB directory, but usually they are the same). `rocksdb:restore_db_from_latest_backup/3` will restore the DB from the latest backup, i.e., the one with the highest ID.

Checksum is calculated for any restored file and compared against the one stored during the backup time. If a checksum mismatch is detected, the restore process is aborted and an error is returned.

## Managing Backups

### Deleting Specific Backups

To delete a specific backup by its ID:

```erlang
ok = rocksdb:delete_backup(Backup, BackupId).
```

### Purging Old Backups

To keep only the N most recent backups and delete older ones:

```erlang
%% Keep only the 5 most recent backups
ok = rocksdb:purge_old_backup(Backup, 5).
```

### Garbage Collection

Run garbage collection to clean up obsolete backup files:

```erlang
ok = rocksdb:gc_backup_engine(Backup).
```

### Stopping a Backup

If you need to abort an in-progress backup from another process:

```erlang
ok = rocksdb:stop_backup(Backup).
```

For more infos about the backups internals have a look in the [rocksdb documentation](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F#backup-directory-structure)