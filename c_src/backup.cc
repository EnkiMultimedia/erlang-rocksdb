// -------------------------------------------------------------------
// Copyright (c) 2017-2025 Benoit Chesneau. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <vector>

#include "erl_nif.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/db.h"
#include "rocksdb/utilities/backup_engine.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"


namespace erocksdb {


ERL_NIF_TERM
OpenBackupEngine(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
  char path[4096];
  BackupEngineObject * backup_engine_ptr;
  rocksdb::BackupEngine* backup_engine;
  rocksdb::Status s;

  if(!enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_LATIN1) ||!enif_is_list(env, argv[0]))
  {
    return enif_make_badarg(env);
  }

  s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupEngineOptions(path), &backup_engine);
  if(!s.ok())
    return error_tuple(env, ATOM_ERROR_BACKUP_ENGINE_OPEN, s);

  backup_engine_ptr = BackupEngineObject::CreateBackupEngineObject(backup_engine);
  ERL_NIF_TERM result = enif_make_resource(env, backup_engine_ptr);
  enif_release_resource(backup_engine_ptr);
  return enif_make_tuple2(env, ATOM_OK, result);
} // OpenBackupEngine

ERL_NIF_TERM
GCBackupEngine(ErlNifEnv *env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    rocksdb::Status s;

    if (!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    s = backup_engine_ptr->m_BackupEngine->GarbageCollect();
    if (s.ok())
    {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}

ERL_NIF_TERM
CloseBackupEngine(ErlNifEnv *env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    BackupEngineObject *backup_engine_ptr;
    backup_engine_ptr = BackupEngineObject::RetrieveBackupEngineObject(env, argv[0]);

    if (NULL == backup_engine_ptr)
        return enif_make_badarg(env);

    // set closing flag
    ErlRefObject::InitiateCloseRequest(backup_engine_ptr);
    backup_engine_ptr = NULL;
    return ATOM_OK;
}

ERL_NIF_TERM
CreateNewBackup(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    ReferencePtr<DbObject> db_ptr;
    rocksdb::Status s;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    if(!enif_get_db(env, argv[1], &db_ptr))
            return enif_make_badarg(env);

    s = backup_engine_ptr->m_BackupEngine->CreateNewBackup(db_ptr->m_Db);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}

ERL_NIF_TERM
GetBackupInfo(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{

    ReferencePtr<BackupEngineObject> backup_engine_ptr;

    std::vector<rocksdb::BackupInfo> backup_infos;
    rocksdb::Status s;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    backup_engine_ptr->m_BackupEngine->GetBackupInfo(&backup_infos);

    ERL_NIF_TERM info_list = enif_make_list(env, 0);
    for (auto & backup_info : backup_infos) {
        ERL_NIF_TERM backup_id = enif_make_uint(env, backup_info.backup_id);
        ERL_NIF_TERM timestamp = enif_make_uint64(env, backup_info.timestamp);
        ERL_NIF_TERM size = enif_make_uint64(env, backup_info.size);
        ERL_NIF_TERM number_files = enif_make_uint(env, backup_info.number_files);

        ERL_NIF_TERM info = enif_make_new_map(env);
        enif_make_map_put(env, info, ATOM_BACKUP_INFO_ID, backup_id, &info);
        enif_make_map_put(env, info, ATOM_BACKUP_INFO_TIMESTAMP, timestamp, &info);
        enif_make_map_put(env, info, ATOM_BACKUP_INFO_SIZE, size, &info);
        enif_make_map_put(env, info, ATOM_BACKUP_INFO_NUMBER_FILES, number_files, &info);

        info_list = enif_make_list_cell(env, info, info_list);
    }

    return enif_make_tuple2(env, ATOM_OK, info_list);

}


ERL_NIF_TERM
VerifyBackup(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    rocksdb::BackupID backup_id;
    rocksdb::Status s;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    if(!enif_get_uint(env, argv[1], &backup_id))
        return enif_make_badarg(env);

    s = backup_engine_ptr->m_BackupEngine->VerifyBackup(backup_id);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}

ERL_NIF_TERM
StopBackup(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    backup_engine_ptr->m_BackupEngine->StopBackup();
    return ATOM_OK;
}

ERL_NIF_TERM
DeleteBackup(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    rocksdb::BackupID backup_id;
    rocksdb::Status s;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    if(!enif_get_uint(env, argv[1], &backup_id))
        return enif_make_badarg(env);

    s = backup_engine_ptr->m_BackupEngine->DeleteBackup(backup_id);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}

ERL_NIF_TERM
PurgeOldBackup(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    uint32_t num_backups_to_keep;
    rocksdb::Status s;

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    if(!enif_get_uint(env, argv[1], &num_backups_to_keep))
        return enif_make_badarg(env);

    s = backup_engine_ptr->m_BackupEngine->PurgeOldBackups(num_backups_to_keep);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}




ERL_NIF_TERM
RestoreDBFromBackup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    rocksdb::BackupID backup_id;
    rocksdb::Status s;
    char db_dir[4096];
    char wal_dir[4096];

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);

    if(!enif_get_uint(env, argv[1], &backup_id))
        return enif_make_badarg(env);


    if(!enif_get_string(env, argv[2], db_dir, sizeof(db_dir), ERL_NIF_LATIN1) || !enif_is_list(env, argv[2])) {
        return enif_make_badarg(env);
    }

    if(argc == 4) {

        if(!enif_get_string(env, argv[3], wal_dir, sizeof(wal_dir), ERL_NIF_LATIN1) || !enif_is_list(env, argv[3])) {
            return enif_make_badarg(env);
        }

    } else {
        strncpy(wal_dir, db_dir, sizeof(db_dir));
    }



    s = backup_engine_ptr->m_BackupEngine->RestoreDBFromBackup(backup_id, db_dir, wal_dir);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}

ERL_NIF_TERM
RestoreDBFromLatestBackup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<BackupEngineObject> backup_engine_ptr;
    rocksdb::Status s;
    char db_dir[4096];
    char wal_dir[4096];

    if(!enif_get_backup_engine(env, argv[0], &backup_engine_ptr))
        return enif_make_badarg(env);


    if(!enif_get_string(env, argv[1], db_dir, sizeof(db_dir), ERL_NIF_LATIN1) || !enif_is_list(env, argv[1])) {
        return enif_make_badarg(env);
    }

    if(argc == 3) {

        if(!enif_get_string(env, argv[2], wal_dir, sizeof(wal_dir), ERL_NIF_LATIN1) || !enif_is_list(env, argv[2])) {
            return enif_make_badarg(env);
        }

    } else {
        strncpy(wal_dir, db_dir, sizeof(db_dir));
    }

    s = backup_engine_ptr->m_BackupEngine->RestoreDBFromLatestBackup(db_dir, wal_dir);
    if (s.ok()) {
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, s);
}




}
