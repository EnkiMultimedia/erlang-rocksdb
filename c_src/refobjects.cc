// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016-2026 Benoit Chesneau/
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
}   // ErlRefObject::ErlRefObject


ErlRefObject::~ErlRefObject()
{
    // runs from AwaitCloseAndDestructor, after ReleaseNative
}   // ErlRefObject::~ErlRefObject


bool
ErlRefObject::InitiateCloseRequest(
    ErlRefObject * Object)
{
    bool ret_flag;

    ret_flag=false;

    if (NULL!=Object && 0==Object->m_CloseRequested)
    {
        // same lock as TryRefInc, see there
        std::lock_guard<std::mutex> lk(Object->m_CloseMutex);
        ret_flag=erocksdb::compare_and_swap(Object->m_CloseRequested, 0u, 1u);
    }

    // vtable is still good, this thread is initiating close
    //   ask object to clean-up
    if (ret_flag)
    {
        Object->Shutdown();
    }   // if

    return(ret_flag);

}   // ErlRefObject::InitiateCloseRequest


bool
ErlRefObject::TryRefInc()
{
    std::lock_guard<std::mutex> lk(m_CloseMutex);
    if (0!=m_CloseRequested)
        return false;
    RefInc();
    return true;
}   // ErlRefObject::TryRefInc


void
ErlRefObject::AwaitCloseAndDestructor(
    ErlRefObject * Object)
{
    if (NULL==Object)
        return;

    // Always take the lock, even if state 3 is already visible: the thread
    // that set it may still hold the mutex, which the destructor below frees.
    {
        std::unique_lock<std::mutex> lk(Object->m_CloseMutex);
        while (3!=Object->m_CloseRequested)
            Object->m_CloseCond.wait(lk);
    }

    // The native objects are released: run the C++ destructor. Erlang frees
    // the memory once this callback returns.
    Object->~ErlRefObject();

}   // ErlRefObject::AwaitCloseAndDestructor


void
ErlRefObject::AwaitRelease()
{
    std::unique_lock<std::mutex> lk(m_CloseMutex);
    while (3!=m_CloseRequested)
        m_CloseCond.wait(lk);
}   // ErlRefObject::AwaitRelease


uint32_t
ErlRefObject::RefDec()
{
    uint32_t cur_count;

    cur_count = --m_RefCount;
    // last reference after a close request: release the native objects, the
    // C++ object stays alive until its Erlang resource is destroyed
    if (0==cur_count && compare_and_swap(m_CloseRequested, 1u, 2u))
    {
        ReleaseNative();

        std::lock_guard<std::mutex> lk(m_CloseMutex);
        m_CloseRequested=3;
        m_CloseCond.notify_all();
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
DbObject::CreateDbObject(rocksdb::DB * Db, bool IsPessimistic, bool IsTTL)
{
    DbObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Db_RESOURCE, sizeof(DbObject));
    ret_ptr=new (alloc_ptr) DbObject(Db, IsPessimistic, IsTTL);

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


DbObject::DbObject(rocksdb::DB * DbPtr, bool IsPessimistic, bool IsTTL)
    : m_Db(DbPtr), m_IsPessimistic(IsPessimistic), m_IsTTL(IsTTL)
    {}   // DbObject::DbObject


void
DbObject::ReleaseNative()
{

    // close the db
    delete m_Db;
    m_Db=NULL;

    // the env must outlive the database
    m_Env.Reset();
}   // DbObject::ReleaseNative


DbObject::~DbObject()
{
    // everything was released by ReleaseNative
}   // DbObject::~DbObject


void
DbObject::Shutdown()
{
#if 1
    bool again;
    bool held = false;   // a child already closing is skipped
    ColumnFamilyObject *column_family_ptr;
    ItrObject * itr_ptr;
    SnapshotObject * snapshot_ptr;
    TLogItrObject *tlog_ptr;
    TransactionObject *tx_ptr;
    IngestionObject *ingestion_ptr;

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
                // a reference keeps the child alive once the list lock is released
                held = itr_ptr->TryRefInc();
            }   // if
        }

        // must be outside lock so ItrObject can attempt
        //  RemoveReference
        if (again && held)
        {
            ItrObject::InitiateCloseRequest(itr_ptr);
            itr_ptr->RefDec();
        }

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
                // a reference keeps the child alive once the list lock is released
                held = column_family_ptr->TryRefInc();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again && held)
        {
            ColumnFamilyObject::InitiateCloseRequest(column_family_ptr);
            column_family_ptr->RefDec();
        }

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
                // a reference keeps the child alive once the list lock is released
                held = snapshot_ptr->TryRefInc();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again && held)
        {
            SnapshotObject::InitiateCloseRequest(snapshot_ptr);
            snapshot_ptr->RefDec();
        }

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
                // a reference keeps the child alive once the list lock is released
                held = tlog_ptr->TryRefInc();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again && held)
        {
            TLogItrObject::InitiateCloseRequest(tlog_ptr);
            tlog_ptr->RefDec();
        }

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
                // a reference keeps the child alive once the list lock is released
                held = tx_ptr->TryRefInc();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again && held)
        {
            TransactionObject::InitiateCloseRequest(tx_ptr);
            tx_ptr->RefDec();
        }

    } while (again);

    // roll back ingestions prepared but never committed
    do {
        again = false;
        ingestion_ptr = NULL;

        {
            std::lock_guard<std::mutex> lock(m_IngestionMutex);

            if (!m_IngestionList.empty()) {
                again = true;
                ingestion_ptr = m_IngestionList.front();
                m_IngestionList.pop_front();
                // a reference keeps the child alive once the list lock is released
                held = ingestion_ptr->TryRefInc();
            }   // if
        }

        // outside lock so IngestionObject can attempt RemoveIngestionReference
        if (again && held)
        {
            IngestionObject::InitiateCloseRequest(ingestion_ptr);
            ingestion_ptr->RefDec();
        }

    } while (again);



#endif

    RefDec();

    return;

}   // DbObject::Shutdown

bool
DbObject::AddColumnFamilyReference(
        ColumnFamilyObject *ColumnFamilyPtr)
{
    // refused once closing started: Shutdown may already have drained the list
    std::lock_guard<std::mutex> lock(m_ColumnFamilyMutex);
    if (0 != m_CloseRequested)
        return false;
    m_ColumnFamilyList.push_back(ColumnFamilyPtr);
    return true;
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

bool
DbObject::AddSnapshotReference(
    SnapshotObject * SnapshotPtr)
{
    // refused once closing started: Shutdown may already have drained the list
    std::lock_guard<std::mutex> lock(m_SnapshotMutex);
    if (0 != m_CloseRequested)
        return false;
    m_SnapshotList.push_back(SnapshotPtr);
    return true;
}   // DbObject::AddSnapshotReference


void
DbObject::RemoveSnapshotReference(
    SnapshotObject * SnapshotPtr)
{
    std::lock_guard<std::mutex> lock(m_SnapshotMutex);

    m_SnapshotList.remove(SnapshotPtr);

    return;
}   // DbObject::RemoveSnapshotReference

bool
DbObject::AddIngestionReference(IngestionObject *IngestionPtr)
{
    // refused once closing started: Shutdown may already have drained the list
    std::lock_guard<std::mutex> lock(m_IngestionMutex);
    if (0 != m_CloseRequested)
        return false;
    m_IngestionList.push_back(IngestionPtr);
    return true;
}   // DbObject::AddIngestionReference


void
DbObject::RemoveIngestionReference(IngestionObject *IngestionPtr) {
    std::lock_guard<std::mutex> lock(m_IngestionMutex);
    m_IngestionList.remove(IngestionPtr);
}   // DbObject::RemoveIngestionReference


bool
DbObject::AddTLogReference(TLogItrObject *TLogItrPtr)
{
    // refused once closing started: Shutdown may already have drained the list
    std::lock_guard<std::mutex> lock(m_TLogItrMutex);
    if (0 != m_CloseRequested)
        return false;
    m_TLogItrList.push_back(TLogItrPtr);
    return true;
}   // DbObject::AddTLogReference


void
DbObject::RemoveTLogReference(TLogItrObject *TLogItrPtr) {
    std::lock_guard<std::mutex> lock(m_TLogItrMutex);

    m_TLogItrList.remove(TLogItrPtr);

    return;

}   // DbObject::RemoveTLogReference

bool
DbObject::AddTransactionReference(TransactionObject *TxPtr)
{
    // refused once closing started: Shutdown may already have drained the list
    std::lock_guard<std::mutex> lock(m_TransactionMutex);
    if (0 != m_CloseRequested)
        return false;
    m_TransactionList.push_back(TxPtr);
    return true;
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
    // creation reference already taken by the constructor

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


    // creation reference, released by Shutdown. Taken before the child is
    // attached: a concurrent close could otherwise find it at zero and
    // release it before it is even returned.
    RefInc();

    if (NULL != DbPtr)
        m_Attached = DbPtr->AddColumnFamilyReference(this);
}   // ColumnFamilyObject::ColumnFamilyObject


void
ColumnFamilyObject::ReleaseNative()
{
    if (NULL != m_ColumnFamily) {
        m_DbPtr->m_Db->DestroyColumnFamilyHandle(m_ColumnFamily);
        m_ColumnFamily = NULL;
    }
    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveColumnFamilyReference(this);

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // ColumnFamilyObject::ReleaseNative


ColumnFamilyObject::~ColumnFamilyObject()
{
    // everything was released by ReleaseNative
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
    // creation reference already taken by the constructor

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
    // creation reference, released by Shutdown. Taken before the child is
    // attached: a concurrent close could otherwise find it at zero and
    // release it before it is even returned.
    RefInc();

    if (NULL!=DbPtr)
        m_Attached = DbPtr->AddSnapshotReference(this);

}   // SnapshotObject::SnapshotObject


void
SnapshotObject::ReleaseNative()
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

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // SnapshotObject::ReleaseNative


SnapshotObject::~SnapshotObject()
{
    // everything was released by ReleaseNative
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
    // creation reference already taken by the constructor

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


void
ItrObject::KeepResource(void *Resource) {
    if (nullptr != Resource) {
        enif_keep_resource(Resource);
        m_KeptResources.push_back(Resource);
    }
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

    // creation reference, released by Shutdown
    RefInc();

    // not attached yet: the creating NIF calls Attach once the iterator is set
    // up, so a concurrent database close never sees a half built iterator
    m_Attached = false;

}   // ItrObject::ItrObject


bool
ItrObject::Attach()
{
    if (nullptr != m_DbPtr.get())
        m_Attached = m_DbPtr->AddReference(this);
    return m_Attached;
}   // ItrObject::Attach



void
ItrObject::ReleaseNative()
{
    // not likely to have active reuse item since it would
    //  block destruction


    if (nullptr != m_DbPtr.get())
        m_DbPtr->RemoveReference(this);

    delete upper_bound_slice;
    upper_bound_slice = nullptr;

    delete lower_bound_slice;
    lower_bound_slice = nullptr;

    // the bound keys live in this env, free it with them
    env.reset();


    delete m_Iterator;
    m_Iterator = nullptr;

    // the snapshot, transaction and column families are released only once
    // the iterator is gone
    SnapshotObject * snapshot = m_SnapshotPtr.get();
    m_SnapshotPtr.assign(NULL);
    if (snapshot != nullptr)
        enif_release_resource(snapshot);

    TransactionObject * tx = m_TxPtr.get();
    m_TxPtr.assign(NULL);
    if (tx != nullptr)
        enif_release_resource(tx);

    for (ColumnFamilyObject * cf : m_PinnedColumnFamilies) {
        cf->RefDec();
        enif_release_resource(cf);
    }
    m_PinnedColumnFamilies.clear();

    // release any resources (column family, transaction) pinned for this
    // iterator's lifetime. Done after deleting the iterator, which still
    // references them.
    for (void *resource : m_KeptResources)
        enif_release_resource(resource);
    m_KeptResources.clear();

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // ItrObject::ReleaseNative


ItrObject::~ItrObject()
{
    // everything was released by ReleaseNative
}   // ItrObject::~ItrObject


void
ItrObject::KeepSnapshot(SnapshotObject * Snapshot) {
    if (Snapshot == nullptr || m_SnapshotPtr.get() != nullptr)
        return;
    enif_keep_resource(Snapshot);
    m_SnapshotPtr.assign(Snapshot);
}   // ItrObject::KeepSnapshot


void
ItrObject::KeepTransaction(TransactionObject * Tx) {
    if (Tx == nullptr || m_TxPtr.get() != nullptr)
        return;
    enif_keep_resource(Tx);
    m_TxPtr.assign(Tx);
}   // ItrObject::KeepTransaction


void
ItrObject::KeepColumnFamily(ColumnFamilyObject * Cf) {
    // the caller holds a reference, so the object is live
    enif_keep_resource(Cf);
    Cf->RefInc();
    m_PinnedColumnFamilies.push_back(Cf);
}   // ItrObject::KeepColumnFamily


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
        rocksdb::WalIterator * Itr) {
    TLogItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_TLogItr_RESOURCE, sizeof(TLogItrObject));

    ret_ptr = new(alloc_ptr) TLogItrObject(DbPtr, Itr);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    // creation reference already taken by the constructor

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
        rocksdb::WalIterator * Itr)
        : m_Iter(Itr), m_DbPtr(DbPtr) {

    // creation reference, released by Shutdown. Taken before the child is
    // attached: a concurrent close could otherwise find it at zero and
    // release it before it is even returned.
    RefInc();

    if (NULL != DbPtr)
        m_Attached = DbPtr->AddTLogReference(this);

}   // TLogItrObject::TLogItrObject


void
TLogItrObject::ReleaseNative()
{

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveTLogReference(this);


    delete m_Iter;
    m_Iter = NULL;

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // TLogItrObject::ReleaseNative


TLogItrObject::~TLogItrObject()
{
    // everything was released by ReleaseNative
}   // TLogItrObject::~TLogItrObject


void
TLogItrObject::Shutdown() {
    RefDec();

    return;
}   // TLogItrObject::CloseRequest



/**
 * IngestionObject Functions
 */

ErlNifResourceType *IngestionObject::m_Ingestion_RESOURCE(NULL);


void
IngestionObject::CreateIngestionObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Ingestion_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_IngestionObject",
                                                   &IngestionObject::IngestionObjectResourceCleanup,
                                                   flags, NULL);
}   // IngestionObject::CreateIngestionObjectType


IngestionObject *
IngestionObject::CreateIngestionObject(
        DbObject *DbPtr,
        std::unique_ptr<rocksdb::FileIngestionHandle> Handle) {
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_Ingestion_RESOURCE, sizeof(IngestionObject));

    IngestionObject *ret_ptr = new(alloc_ptr) IngestionObject(DbPtr, std::move(Handle));

    // manual reference increase to keep active until "close" called
    // creation reference already taken by the constructor

    return ret_ptr;
}   // IngestionObject::CreateIngestionObject


IngestionObject *
IngestionObject::RetrieveIngestionObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &Term) {
    IngestionObject *ret_ptr = NULL;

    if (enif_get_resource(Env, Term, m_Ingestion_RESOURCE, (void **) &ret_ptr)) {
        // committed, aborted or its database is closed
        if (ret_ptr->m_CloseRequested)
            ret_ptr = NULL;
    }

    return ret_ptr;
}   // IngestionObject::RetrieveIngestionObject


void
IngestionObject::IngestionObjectResourceCleanup(
        ErlNifEnv * /*env*/,
        void *arg) {
    IngestionObject *ingestion_ptr = (IngestionObject *)arg;

    InitiateCloseRequest(ingestion_ptr);

    // can be called after the destructor ran, see ErlRefObject
    AwaitCloseAndDestructor(ingestion_ptr);
}   // IngestionObject::IngestionObjectResourceCleanup


IngestionObject::IngestionObject(
        DbObject *DbPtr,
        std::unique_ptr<rocksdb::FileIngestionHandle> Handle)
        : m_DbPtr(DbPtr), m_Handle(std::move(Handle)) {

    // creation reference, released by Shutdown. Taken before the child is
    // attached: a concurrent close could otherwise find it at zero and
    // release it before it is even returned.
    RefInc();

    if (NULL != DbPtr)
        m_Attached = DbPtr->AddIngestionReference(this);

}   // IngestionObject::IngestionObject


void
IngestionObject::ReleaseNative()
{

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveIngestionReference(this);

    // Destroying a handle that was not committed rolls the ingestion back.
    // This must happen while m_DbPtr still keeps the database open.
    m_Handle.reset();

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // IngestionObject::ReleaseNative


IngestionObject::~IngestionObject()
{
    // everything was released by ReleaseNative
}   // IngestionObject::~IngestionObject


void
IngestionObject::Shutdown() {
    RefDec();
}   // IngestionObject::Shutdown


std::unique_ptr<rocksdb::FileIngestionHandle>
IngestionObject::TakeHandle() {
    std::lock_guard<std::mutex> lock(m_HandleMutex);
    return std::move(m_Handle);
}   // IngestionObject::TakeHandle


void
IngestionObject::RestoreHandle(std::unique_ptr<rocksdb::FileIngestionHandle> Handle) {
    std::lock_guard<std::mutex> lock(m_HandleMutex);
    m_Handle = std::move(Handle);
}   // IngestionObject::RestoreHandle


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


void
BackupEngineObject::ReleaseNative()
{

    // close the db
    delete m_BackupEngine;
    m_BackupEngine=NULL;
}   // BackupEngineObject::ReleaseNative


BackupEngineObject::~BackupEngineObject()
{
    // everything was released by ReleaseNative
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
    // creation reference already taken by the constructor

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

      // creation reference, released by Shutdown. Taken before the child is
      // attached: a concurrent close could otherwise find it at zero and
      // release it before it is even returned.
      RefInc();

      if (NULL!=DbPtr)
        m_Attached = DbPtr->AddTransactionReference(this);

    }


void
TransactionObject::ReleaseNative()
{

  if(NULL!=m_DbPtr.get())
    m_DbPtr->RemoveTransactionReference(this);
  
  delete m_Tx;
  m_Tx=NULL;

    // release the parents now: closing the database must not wait for
    // this object's Erlang term to be garbage collected
    m_DbPtr.assign(NULL);
}   // TransactionObject::ReleaseNative


TransactionObject::~TransactionObject()
{
    // everything was released by ReleaseNative
}   // TransactionObject::~TransactionObject


void
TransactionObject::Shutdown()
{
    RefDec();
    return;

}   // TransactionObject::Shutdown

} // namespace erocksdb


