// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016-2025 Benoit Chesneau/
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

#include "rocksdb/utilities/backup_engine.h"
#include "refobjects.h"
#include "detail.hpp"

namespace erocksdb {

/**
 * RefObject Functions
 */

RefObject::RefObject()
    : m_RefCount(0)
{
}   // RefObject::RefObject


RefObject::~RefObject()
{
}   // RefObject::~RefObject


uint32_t
RefObject::RefInc()
{

    return ++m_RefCount;

}   // RefObject::RefInc


uint32_t
RefObject::RefDec()
{
    uint32_t current_refs;

    current_refs = --m_RefCount;
    if (0==current_refs)
        delete this;

    return(current_refs);

}   // RefObject::RefDec


/**
 * Erlang reference object
 */

ErlRefObject::ErlRefObject()
    : m_CloseRequested(0)
{
    m_CloseMutex = new std::mutex();
    m_CloseCond = new std::condition_variable();

    return;

}   // ErlRefObject::ErlRefObject


ErlRefObject::~ErlRefObject()
{

    std::unique_lock<std::mutex> lk(*m_CloseMutex);
    m_CloseRequested=3;
    m_CloseCond->notify_all();
    lk.unlock();

    // DO NOT DESTROY m_CloseMutex or m_CloseCond here

}   // ErlRefObject::~ErlRefObject


bool
ErlRefObject::InitiateCloseRequest(
    ErlRefObject * Object)
{
    bool ret_flag;

    ret_flag=false;

    // special handling since destructor may have been called
    if (NULL!=Object && 0==Object->m_CloseRequested)
        ret_flag=erocksdb::compare_and_swap(Object->m_CloseRequested, 0u, 1u);

    // vtable is still good, this thread is initiating close
    //   ask object to clean-up
    if (ret_flag)
    {
        Object->Shutdown();
    }   // if

    return(ret_flag);

}   // ErlRefObject::InitiateCloseRequest


void
ErlRefObject::AwaitCloseAndDestructor(
    ErlRefObject * Object)
{
    // NOTE:  it is possible, actually likely, that this
    //        routine is called AFTER the destructor is called
    //        Don't panic.

    if (NULL!=Object)
    {
        // quick test if any work pending
        if (3!=Object->m_CloseRequested)
        {
            std::unique_lock<std::mutex> lk(*(Object->m_CloseMutex));

            // retest after mutex helc
            while (3!=Object->m_CloseRequested)
            {
                Object->m_CloseCond->wait(lk);
            }   // while
            lk.unlock();
        }   // if

        delete Object->m_CloseMutex;
        delete Object->m_CloseCond;
    }   // if

    return;

}   // ErlRefObject::AwaitCloseAndDestructor


uint32_t
ErlRefObject::RefDec()
{
    uint32_t cur_count;

    cur_count = --m_RefCount;
    // this the last active after close requested?
    //  (atomic swap should be unnecessary ... but going for safety)
    if (0==cur_count && compare_and_swap(m_CloseRequested, 1u, 2u))
    {
        // deconstruct, but let erlang deallocate memory later
        this->~ErlRefObject();
    }   // if

    return(cur_count);

}   // DbObject::RefDec



/**
 * DbObject Functions
 */

ErlNifResourceType * DbObject::m_Db_RESOURCE(NULL);


void
DbObject::CreateDbObjectType(
    ErlNifEnv * Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Db_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_DbObject",
                                            &DbObject::DbObjectResourceCleanup,
                                            flags, NULL);

    return;

}   // DbObject::CreateDbObjectType


DbObject *
DbObject::CreateDbObject(rocksdb::DB * Db, bool IsPessimistic)
{
    DbObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Db_RESOURCE, sizeof(DbObject));
    ret_ptr=new (alloc_ptr) DbObject(Db, IsPessimistic);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc();

    // see OpenTask::operator() for release of reference count

    return(ret_ptr);

}   // DbObject::CreateDbObject

DbObject *
DbObject::RetrieveDbObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & DbTerm)
{
    DbObject * ret_ptr;

    ret_ptr=NULL;

    if (enif_get_resource(Env, DbTerm, m_Db_RESOURCE, (void **)&ret_ptr))
    {
        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
            ret_ptr=NULL;
        }   // else
    }   // if

    return(ret_ptr);

}   // DbObject::RetrieveDbObject


void
DbObject::DbObjectResourceCleanup(
    ErlNifEnv * /*env*/,
    void * arg)
{
    DbObject * db_ptr;

    db_ptr=(DbObject *)arg;

    // YES, the destructor may already have been called
    InitiateCloseRequest(db_ptr);

    // YES, the destructor may already have been called
    AwaitCloseAndDestructor(db_ptr);

    return;

}   // DbObject::DbObjectResourceCleanup


DbObject::DbObject(rocksdb::DB * DbPtr, bool IsPessimistic)
    : m_Db(DbPtr), m_IsPessimistic(IsPessimistic)
    {}   // DbObject::DbObject


DbObject::~DbObject()
{

    // close the db
    delete m_Db;
    m_Db=NULL;

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // DbObject::~DbObject


void
DbObject::Shutdown()
{
#if 1
    bool again;
    ColumnFamilyObject *column_family_ptr;
    ItrObject * itr_ptr;
    SnapshotObject * snapshot_ptr;
    TLogItrObject *tlog_ptr;
    TransactionObject *tx_ptr;

    do
    {
        again=false;
        itr_ptr=NULL;

        // lock the ItrList
        {
            std::lock_guard<std::mutex> lock(m_ItrMutex);

            if (!m_ItrList.empty())
            {
                again=true;
                itr_ptr=m_ItrList.front();
                m_ItrList.pop_front();
            }   // if
        }

        // must be outside lock so ItrObject can attempt
        //  RemoveReference
        if (again)
            ItrObject::InitiateCloseRequest(itr_ptr);

    } while(again);

    // clean columns families
    do {
        again = false;
        column_family_ptr = NULL;

        // lock the SnapshotList
        {
            std::lock_guard<std::mutex> lock(m_ColumnFamilyMutex);

            if (!m_ColumnFamilyList.empty()) {
                again = true;
                column_family_ptr = m_ColumnFamilyList.front();
                m_ColumnFamilyList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            ColumnFamilyObject::InitiateCloseRequest(column_family_ptr);

    } while (again);

    // clean snapshots linked to the database object
    do
    {
        again=false;
        snapshot_ptr=NULL;

        // lock the SnapshotList
        {
            std::lock_guard<std::mutex> lock(m_SnapshotMutex);

            if (!m_SnapshotList.empty())
            {
                again=true;
                snapshot_ptr=m_SnapshotList.front();
                m_SnapshotList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            SnapshotObject::InitiateCloseRequest(snapshot_ptr);

    } while(again);

    // clean transaction log iterators
    do {
        again = false;
        tlog_ptr = NULL;

        // lock the SnapshotList
        {
            std::lock_guard<std::mutex> lock(m_TLogItrMutex);

            if (!m_TLogItrList.empty()) {
                again = true;
                tlog_ptr = m_TLogItrList.front();
                m_TLogItrList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            TLogItrObject::InitiateCloseRequest(tlog_ptr);

    } while (again);

     // clean transactions
     do {
        again = false;
        tx_ptr = NULL;

        // lock the SnapshotList
        {
            std::lock_guard<std::mutex> lock(m_TransactionMutex);

            if (!m_TransactionList.empty()) {
                again = true;
                tx_ptr = m_TransactionList.front();
                m_TransactionList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            TransactionObject::InitiateCloseRequest(tx_ptr);

    } while (again);



#endif

    RefDec();

    return;

}   // DbObject::Shutdown

void
DbObject::AddColumnFamilyReference(
        ColumnFamilyObject *ColumnFamilyPtr) {
    std::lock_guard<std::mutex> lock(m_ColumnFamilyMutex);
    m_ColumnFamilyList.push_back(ColumnFamilyPtr);
    return;
}   // DbObject::ColumnFamilyReference


void
DbObject::RemoveColumnFamilyReference(
        ColumnFamilyObject *ColumnFamilyPtr) {
    std::lock_guard<std::mutex> lock(m_ColumnFamilyMutex);
    m_ColumnFamilyList.remove(ColumnFamilyPtr);
    return;
}   // DbObject::RemoveColumnFamilyReference

bool
DbObject::AddReference(
    ItrObject * ItrPtr)
{
    bool ret_flag;
    std::lock_guard<std::mutex> lock(m_ItrMutex);

    ret_flag=(0==m_CloseRequested);
    if (ret_flag)
        m_ItrList.push_back(ItrPtr);

    return(ret_flag);
}   // DbObject::AddReference


void
DbObject::RemoveReference(
    ItrObject * ItrPtr)
{
    std::lock_guard<std::mutex> lock(m_ItrMutex);
    m_ItrList.remove(ItrPtr);
    return;

}   // DbObject::RemoveReference

void
DbObject::AddSnapshotReference(
    SnapshotObject * SnapshotPtr)
{
    std::lock_guard<std::mutex> lock(m_SnapshotMutex);
    m_SnapshotList.push_back(SnapshotPtr);
    return;

}   // DbObject::AddSnapshotReference


void
DbObject::RemoveSnapshotReference(
    SnapshotObject * SnapshotPtr)
{
    std::lock_guard<std::mutex> lock(m_SnapshotMutex);

    m_SnapshotList.remove(SnapshotPtr);

    return;
}   // DbObject::RemoveSnapshotReference

void
DbObject::AddTLogReference(TLogItrObject *TLogItrPtr) {
    std::lock_guard<std::mutex> lock(m_TLogItrMutex);

    m_TLogItrList.push_back(TLogItrPtr);

    return;

}   // DbObject::AddTLogReference


void
DbObject::RemoveTLogReference(TLogItrObject *TLogItrPtr) {
    std::lock_guard<std::mutex> lock(m_TLogItrMutex);

    m_TLogItrList.remove(TLogItrPtr);

    return;

}   // DbObject::RemoveTLogReference

void
DbObject::AddTransactionReference(TransactionObject *TxPtr) {
    std::lock_guard<std::mutex> lock(m_TransactionMutex);

    m_TransactionList.push_back(TxPtr);

    return;

}   // DbObject::AddTransactioneference


void
DbObject::RemoveTransactionReference(TransactionObject *TxPtr) {
    std::lock_guard<std::mutex> lock(m_TransactionMutex);

    m_TransactionList.remove(TxPtr);

    return;

}   // DbObject::RemoveTransactionference


/**
* ColumnFamily object
*/

ErlNifResourceType *ColumnFamilyObject::m_ColumnFamily_RESOURCE(NULL);


void
ColumnFamilyObject::CreateColumnFamilyObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_ColumnFamily_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_ColumnFamilyObject",
                                                      &ColumnFamilyObject::ColumnFamilyObjectResourceCleanup,
                                                      flags, NULL);
    return;

}   // ColumnFamilyObject::CreateSnapshotObjectType


ColumnFamilyObject *
ColumnFamilyObject::CreateColumnFamilyObject(
        DbObject *DbPtr,
        rocksdb::ColumnFamilyHandle *Handle) {
    ColumnFamilyObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_ColumnFamily_RESOURCE, sizeof(ColumnFamilyObject));
    ret_ptr = new(alloc_ptr) ColumnFamilyObject(DbPtr, Handle);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // ColumnFamilyObject::ColumnFamilySnapshotObject


ColumnFamilyObject *
ColumnFamilyObject::RetrieveColumnFamilyObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &ColumnFamilyTerm) {
    ColumnFamilyObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, ColumnFamilyTerm, m_ColumnFamily_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // ColumnFamilyObject::RetrieveColumnFamilyObject


void
ColumnFamilyObject::ColumnFamilyObjectResourceCleanup(
        ErlNifEnv * /*env*/,
        void *arg) {
    ColumnFamilyObject *handle_ptr;
    handle_ptr = (ColumnFamilyObject *)arg;
    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(handle_ptr);
    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(handle_ptr);
    return;
}   // ColumnFamilyObject::ColumnFamilyObjectResourceCleanup


ColumnFamilyObject::ColumnFamilyObject(
        DbObject *DbPtr,
        rocksdb::ColumnFamilyHandle *Handle)
        : m_ColumnFamily(Handle), m_DbPtr(DbPtr) {


    if (NULL != DbPtr)
        DbPtr->AddColumnFamilyReference(this);
}   // ColumnFamilyObject::ColumnFamilyObject


ColumnFamilyObject::~ColumnFamilyObject() {
    if (NULL != m_ColumnFamily) {
        m_DbPtr->m_Db->DestroyColumnFamilyHandle(m_ColumnFamily);
        m_ColumnFamily = NULL;
    }
    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveColumnFamilyReference(this);
    return;
}   // ColumnFamilyObject::~ColumnFamilyObject


void
ColumnFamilyObject::Shutdown() {
    RefDec();
    return;
}   // ColumnFamilyObject::CloseRequest


/**
 * snapshot object
 */

ErlNifResourceType* SnapshotObject::m_DbSnapshot_RESOURCE(NULL);


void
SnapshotObject::CreateSnapshotObjectType(
    ErlNifEnv* Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_DbSnapshot_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_SnapshotObject",
                                             &SnapshotObject::SnapshotObjectResourceCleanup,
                                             flags, NULL);

    return;

}   // SnapshotObject::CreateSnapshotObjectType


SnapshotObject *
SnapshotObject::CreateSnapshotObject(
    DbObject* DbPtr,
    const rocksdb::Snapshot* Snapshot)
{
    SnapshotObject* ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_DbSnapshot_RESOURCE, sizeof(SnapshotObject));

    ret_ptr=new (alloc_ptr) SnapshotObject(DbPtr, Snapshot);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return(ret_ptr);

}   // SnapshotObject::CreateSnapshotObject


SnapshotObject *
SnapshotObject::RetrieveSnapshotObject(
    ErlNifEnv* Env,
    const ERL_NIF_TERM & SnapshotTerm)
{
    SnapshotObject* ret_ptr;

    ret_ptr=NULL;

    if (enif_get_resource(Env, SnapshotTerm, m_DbSnapshot_RESOURCE, (void **)&ret_ptr))
    {
        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
            ret_ptr=NULL;
        }   // else
    }   // if

    return(ret_ptr);

}   // SnapshotObject::RetrieveSnapshotObject


void
SnapshotObject::SnapshotObjectResourceCleanup(
    ErlNifEnv* /*env*/,
    void * arg)
{
    SnapshotObject* snapshot_ptr;

    snapshot_ptr=(SnapshotObject *)arg;

    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(snapshot_ptr);

    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(snapshot_ptr);

    return;

}   // SnapshotObject::SnapshotObjectResourceCleanup


SnapshotObject::SnapshotObject(
    DbObject* DbPtr,
    const rocksdb::Snapshot* Snapshot)
    : m_Snapshot(Snapshot), m_DbPtr(DbPtr)
{
    if (NULL!=DbPtr)
        DbPtr->AddSnapshotReference(this);

}   // SnapshotObject::SnapshotObject


SnapshotObject::~SnapshotObject()
{

    if (NULL!=m_DbPtr.get())
    {
        if (m_Snapshot) {
            m_DbPtr->m_Db->ReleaseSnapshot(m_Snapshot);
            m_Snapshot = nullptr;
        }
        m_DbPtr->RemoveSnapshotReference(this);
    }

    m_Snapshot=NULL;

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // SnapshotObject::~SnapshotObject


void
SnapshotObject::Shutdown()
{
    RefDec();

    return;
}   // ItrObject::CloseRequest


/**
* Iterator management object
*/

ErlNifResourceType *ItrObject::m_Itr_RESOURCE(NULL);


void
ItrObject::CreateItrObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Itr_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_ItrObject",
                                             &ItrObject::ItrObjectResourceCleanup,
                                             flags, NULL);

    return;

}   // ItrObject::CreateItrObjectType


ItrObject *
ItrObject::CreateItrObject(
        DbObject *DbPtr,
        std::shared_ptr<erocksdb::ErlEnvCtr> Env,
        rocksdb::Iterator *Iterator) {
    ItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject));

    ret_ptr = new(alloc_ptr) ItrObject(DbPtr, Env, Iterator);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // ItrObject::CreateItrObject


ItrObject *
ItrObject::RetrieveItrObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &ItrTerm, bool ItrClosing) {
    ItrObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, ItrTerm, m_Itr_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested
            || (!ItrClosing && ret_ptr->m_DbPtr->m_CloseRequested)) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // ItrObject::RetrieveItrObject


void
ItrObject::ItrObjectResourceCleanup(
        ErlNifEnv * /*env*/,
        void *arg) {
    ItrObject *itr_ptr;

    itr_ptr = (ItrObject *) arg;

    // vtable for itr_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(itr_ptr);

    // YES this can be called after itr_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(itr_ptr);

    return;

}   // ItrObject::ItrObjectResourceCleanup


void
ItrObject::SetUpperBoundSlice(rocksdb::Slice *slice)
{
    upper_bound_slice = slice;
}

void
ItrObject::SetLowerBoundSlice(rocksdb::Slice *slice) {
    lower_bound_slice = slice;
}



ItrObject::ItrObject(
        DbObject *DbPtr,
        std::shared_ptr<erocksdb::ErlEnvCtr> Env,
        rocksdb::Iterator *Iterator)
        : m_Iterator(Iterator),
          env(Env),
          m_DbPtr(DbPtr),
          upper_bound_slice(nullptr),
          lower_bound_slice(nullptr)
{

    if (NULL != DbPtr)
        DbPtr->AddReference(this);

}   // ItrObject::ItrObject



ItrObject::~ItrObject() {
    // not likely to have active reuse item since it would
    //  block destruction


    if (nullptr != m_DbPtr.get())
        m_DbPtr->RemoveReference(this);

    if(upper_bound_slice != nullptr)
        delete upper_bound_slice;

    if(lower_bound_slice != nullptr)
        delete lower_bound_slice;


    delete m_Iterator;
    //m_Iterator = nullptr;

    return;

}   // ItrObject::~ItrObject


void
ItrObject::Shutdown() {
    // if there is an active move object, set it up to delete
    //  (reuse_move holds a counter to this object, which will
    //   release when move object destructs)
    RefDec();

    return;

}   // ItrObject::CloseRequest

/**
* transaction log object
*/

ErlNifResourceType *TLogItrObject::m_TLogItr_RESOURCE(NULL);


void
TLogItrObject::CreateTLogItrObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_TLogItr_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_TLogItrObject",
                                                 &TLogItrObject::TLogItrObjectResourceCleanup,
                                                 flags, NULL);

    return;

}   // SnapshotObject::CreateSnapshotObjectType


TLogItrObject *
TLogItrObject::CreateTLogItrObject(
        DbObject *DbPtr,
        rocksdb::TransactionLogIterator * Itr) {
    TLogItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_TLogItr_RESOURCE, sizeof(TLogItrObject));

    ret_ptr = new(alloc_ptr) TLogItrObject(DbPtr, Itr);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // TLogItrObject::CreateTLogItrObject


TLogItrObject *
TLogItrObject::RetrieveTLogItrObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &TLogItrTerm) {
    TLogItrObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, TLogItrTerm, m_TLogItr_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // TLogItrObject::RetrieveTLogItrObject


void
TLogItrObject::TLogItrObjectResourceCleanup(
        ErlNifEnv * /*env*/,
        void *arg) {
    TLogItrObject *tlog_ptr;

    tlog_ptr = (TLogItrObject *)arg;

    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(tlog_ptr);

    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(tlog_ptr);

    return;

}   // SnapshotObject::SnapshotObjectResourceCleanup


TLogItrObject::TLogItrObject(
        DbObject *DbPtr,
        rocksdb::TransactionLogIterator * Itr)
        : m_Iter(Itr), m_DbPtr(DbPtr) {

    if (NULL != DbPtr)
        DbPtr->AddTLogReference(this);

}   // TLogItrObject::TLogItrObject


TLogItrObject::~TLogItrObject() {

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveTLogReference(this);


    delete m_Iter;
    m_Iter = NULL;

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // TLogItrObject::~TLogItrObject


void
TLogItrObject::Shutdown() {
    RefDec();

    return;
}   // TLogItrObject::CloseRequest



/**
 * BackupEngineObject Functions
 */

ErlNifResourceType * BackupEngineObject::m_BackupEngine_RESOURCE(NULL);


void
BackupEngineObject::CreateBackupEngineObjectType(
    ErlNifEnv * Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_BackupEngine_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_BackupEngineObject",
                                            &BackupEngineObject::BackupEngineObjectResourceCleanup,
                                            flags, NULL);

    return;

}


BackupEngineObject *
BackupEngineObject::CreateBackupEngineObject(
    rocksdb::BackupEngine * BackupEngine)
{
    BackupEngineObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_BackupEngine_RESOURCE, sizeof(BackupEngineObject));
    ret_ptr=new (alloc_ptr) BackupEngineObject(BackupEngine);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc();

    // see OpenTask::operator() for release of reference count

    return(ret_ptr);

}

BackupEngineObject *
BackupEngineObject::RetrieveBackupEngineObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & BackupEngineTerm)
{
    BackupEngineObject * ret_ptr;
    ret_ptr=NULL;
    if (enif_get_resource(Env, BackupEngineTerm, m_BackupEngine_RESOURCE, (void **)&ret_ptr))
    {
        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
            ret_ptr=NULL;
        }   // else
    }   // if
    return(ret_ptr);
}


void
BackupEngineObject::BackupEngineObjectResourceCleanup(
    ErlNifEnv * /*env*/,
    void * arg)
{
    BackupEngineObject * engine_ptr;

    engine_ptr=(BackupEngineObject *)arg;

    // YES, the destructor may already have been called
    InitiateCloseRequest(engine_ptr);

    // YES, the destructor may already have been called
    AwaitCloseAndDestructor(engine_ptr);

    return;

}

BackupEngineObject::BackupEngineObject(
    rocksdb::BackupEngine * BackupEnginePtr)
    : m_BackupEngine(BackupEnginePtr)
    {}


BackupEngineObject::~BackupEngineObject()
{

    // close the db
    delete m_BackupEngine;
    m_BackupEngine=NULL;
    return;

}   // BackupEngineObject::~BackupEngineObject


void
BackupEngineObject::Shutdown()
{
    RefDec();
    return;

}   // BackupEngineObject::Shutdown

/**
 * TransactionObject Functions
 */

ErlNifResourceType * TransactionObject::m_Transaction_RESOURCE(NULL);


void
TransactionObject::CreateTransactionObjectType(
    ErlNifEnv * Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Transaction_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_TransactionObject",
                                            &TransactionObject::TransactionObjectResourceCleanup,
                                            flags, NULL);

    return;

}


TransactionObject *
TransactionObject::CreateTransactionObject(
    DbObject* DbPtr,
    rocksdb::Transaction* Tx)
{
    TransactionObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Transaction_RESOURCE, sizeof(TransactionObject));
    ret_ptr=new (alloc_ptr) TransactionObject(DbPtr, Tx);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc();

    // see OpenTask::operator() for release of reference count

    return(ret_ptr);

}

TransactionObject *
TransactionObject::RetrieveTransactionObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & TxTerm)
{
    TransactionObject * ret_ptr;
    ret_ptr=NULL;
    if (enif_get_resource(Env, TxTerm, m_Transaction_RESOURCE, (void **)&ret_ptr))
    {
        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
            ret_ptr=NULL;
        }   // else
    }   // if
    return(ret_ptr);
}


void
TransactionObject::TransactionObjectResourceCleanup(
    ErlNifEnv * /*env*/,
    void * arg)
{
    TransactionObject * tx_ptr;

    tx_ptr=(TransactionObject *)arg;

    // YES, the destructor may already have been called
    InitiateCloseRequest(tx_ptr);

    // YES, the destructor may already have been called
    AwaitCloseAndDestructor(tx_ptr);

    return;

}

TransactionObject::TransactionObject(
    DbObject* DbPtr,
    rocksdb::Transaction * TxPtr)
    : m_Tx(TxPtr), m_DbPtr(DbPtr)
    {

      if (NULL!=DbPtr)
        DbPtr->AddTransactionReference(this);

    }


TransactionObject::~TransactionObject()
{

  if(NULL!=m_DbPtr.get())
    m_DbPtr->RemoveTransactionReference(this);
  
  delete m_Tx;
  m_Tx=NULL;
  return;

}   // TransactionObject::~BackupEngineObject


void
TransactionObject::Shutdown()
{
    RefDec();
    return;

}   // TransactionObject::Shutdown

} // namespace erocksdb


