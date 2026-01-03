// -------------------------------------------------------------------
// Copyright (c) 2016-2026 Benoit Chesneau. All Rights Reserved.
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

#include "erl_nif.h"
#include "atoms.h"
#include "refobjects.h"
#include "util.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice_transform.h"

namespace erocksdb {

ERL_NIF_TERM
Snapshot(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    SnapshotObject* snapshot_ptr;
    const rocksdb::Snapshot* snapshot;

    // create snapshot
    snapshot = db_ptr->m_Db->GetSnapshot();
    snapshot_ptr = SnapshotObject::CreateSnapshotObject(db_ptr.get(), snapshot);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, snapshot_ptr);
    // clear the automatic reference from enif_alloc_resource in SnapshotObject
    enif_release_resource(snapshot_ptr);
    snapshot = NULL;

    return enif_make_tuple2(env, ATOM_OK, result);
}   // erocksdb::SnapShot


ERL_NIF_TERM
ReleaseSnapshot(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& handle_ref = argv[0];
    ReferencePtr<SnapshotObject> snapshot_ptr;
    snapshot_ptr.assign(SnapshotObject::RetrieveSnapshotObject(env, handle_ref));

    if(NULL==snapshot_ptr.get())
    {
        return ATOM_OK;
    }

    // release snapshot object
    SnapshotObject* snapshot = snapshot_ptr.get();

    // set closing flag
    ErlRefObject::InitiateCloseRequest(snapshot);

    return ATOM_OK;
}   // erocksdb::ReleaseSnapShot

ERL_NIF_TERM
GetSnapshotSequenceNumber(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& handle_ref = argv[0];
    ReferencePtr<SnapshotObject> snapshot_ptr;
    snapshot_ptr.assign(SnapshotObject::RetrieveSnapshotObject(env, handle_ref));

    if(NULL==snapshot_ptr.get())
        return enif_make_badarg(env);

    // release snapshot object
    SnapshotObject* snapshot = snapshot_ptr.get();

    return enif_make_uint64(env, snapshot->m_Snapshot->GetSequenceNumber());
}


}
