// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016-2026 Benoit Chesneau
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

#pragma once
#ifndef INCL_REFOBJECTS_H
#define INCL_REFOBJECTS_H

#include <memory>
#include <stdint.h>
#include <list>
#include <mutex>
#include <vector>
#include <atomic>
#include <condition_variable>

#include "erl_nif.h"

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"

#include "env.h"

namespace rocksdb {
    class DB;
    class ColumnFamilyHandle;
    class Snapshot;
    class Iterator;
    class BackupEngine;
    class Slice;
    class Transaction;
}

namespace erocksdb {

/**
 * Simple wrapper around an Erlang Environment that can
 * be stored in a shared pointer
 */
class ErlEnvCtr {
    public:

        ErlNifEnv *env;

        ErlEnvCtr() {
            env = enif_alloc_env();
        };

        ~ErlEnvCtr() {
            enif_free_env(env);
        }
};

/**
 * Base class for any object that offers RefInc / RefDec interface
 */

class RefObject
{
public:

protected:
    std::atomic<uint32_t> m_RefCount;     //!< simple count of reference, auto delete at zero

public:
    RefObject();

    RefObject(const RefObject&) = delete;              // nocopy
    RefObject& operator=(const RefObject&) = delete;   // nocopyassign

    virtual ~RefObject();

    virtual uint32_t RefInc();

    virtual uint32_t RefDec();
};  // class RefObject


/**
 * Base class for any object that is managed as an Erlang reference
 */

class ErlRefObject : public RefObject
{
public:
    // these member objects are public to simplify
    //  access by statics and external APIs
    //  (yes, wrapper functions would be welcome)
    // 0 open, 1 close requested, 2 releasing native objects, 3 released
    std::atomic<uint32_t> m_CloseRequested;

    std::mutex m_CloseMutex;                      //!< guards the close flag
    std::condition_variable m_CloseCond;          //!< signals state 3

    // false when the object was created after its database started closing:
    // the close will not reach it, see RefuseClosingChild
    bool m_Attached = true;

public:
    ErlRefObject();

    ErlRefObject(const ErlRefObject&) = delete;              // nocopy
    ErlRefObject& operator=(const ErlRefObject&) = delete;   // nocopyassign

    virtual ~ErlRefObject();

    virtual uint32_t RefDec();

    // allows for secondary close actions IF InitiateCloseRequest returns true
    virtual void Shutdown()=0;

    // Free the RocksDB objects and drop the references to parent objects.
    // Called once, when the last reference goes after a close request. The
    // C++ object itself stays alive until its Erlang resource is destroyed
    // (AwaitCloseAndDestructor), so the close state can always be read.
    virtual void ReleaseNative()=0;

    static bool InitiateCloseRequest(ErlRefObject * Object);

    // Take a reference unless a close was requested. The check and the
    // increment happen under m_CloseMutex, the same lock that sets the close
    // flag, so a concurrent close cannot destroy the object in between.
    bool TryRefInc();

    // From the Erlang resource destructor only: wait for ReleaseNative to
    // finish, then run the C++ destructor. Erlang frees the memory after.
    static void AwaitCloseAndDestructor(ErlRefObject * Object);

    // Wait until ReleaseNative has finished, without destroying anything.
    // The caller must keep the object alive (hold its Erlang term) and must
    // not hold a reference itself, or the release could never happen.
    void AwaitRelease();
};  // class RefObject

/**
 * A child (column family, snapshot, iterator, transaction, ...) created while
 * its database was closing is not attached to it, so closing the database
 * would never close it and RocksDB would stay open. Close it now and refuse
 * the call. Use before the child is turned into an Erlang term.
 */
inline bool
RefuseClosingChild(ErlRefObject * Child)
{
    if (Child->m_Attached)
        return false;
    ErlRefObject::InitiateCloseRequest(Child);
    enif_release_resource(Child);
    return true;
}


/**
 * Class to manage access and counting of references
 * to a reference object.
 */

template <class TargetT>
class ReferencePtr
{
    TargetT * t;

public:
    ReferencePtr()
        : t(NULL)
    {};

    ReferencePtr(TargetT *_t)
        : t(_t)
    {
        if (NULL!=t)
            t->RefInc();
    }

    ReferencePtr(const ReferencePtr &rhs)
    {t=rhs.t; if (NULL!=t) t->RefInc();};

    ReferencePtr & operator=(const ReferencePtr & rhs) = delete; // no assignment

    ~ReferencePtr()
    {
        if (NULL!=t)
            t->RefDec();
    }

    void assign(TargetT * _t)
    {
        if (_t!=t)
        {
            if (NULL!=t)
                t->RefDec();
            t=_t;
            if (NULL!=t)
                t->RefInc();
        }   // if
    };

    // Like assign() for objects retrieved from an Erlang term: only takes the
    // reference if the object is not closing. Returns false otherwise.
    bool try_assign(TargetT * _t)
    {
        if (NULL!=t)
            t->RefDec();
        t=NULL;
        if (NULL!=_t && _t->TryRefInc())
            t=_t;
        return NULL!=t;
    };

    TargetT * get() const { return t; }

    TargetT * operator->() const { return t; }
};  // ReferencePtr


/**
 * Per database object.  Created as erlang reference.
 *
 * Extra reference count created upon initialization, released on close.
 */
class DbObject : public ErlRefObject
{
public:
    rocksdb::DB* m_Db;                             // NULL or rocksdb database object
    bool m_IsPessimistic;                          //!< true if opened as PessimisticTransactionDB
    bool m_IsTTL;                                  //!< true if opened as DBWithTTL
    EnvRef m_Env;                                  //!< keeps the env alive until m_Db is deleted
    std::mutex m_ItrMutex;                         //!< mutex protecting m_ItrList
    std::mutex m_SnapshotMutex;                    //!< mutex protecting m_SnapshotList
    std::mutex m_ColumnFamilyMutex;                //!< mutex protecting m_ColumnFamily
    std::mutex m_TLogItrMutex;                     //!< mutex protecting m_TransactionLogList
    std::mutex m_TransactionMutex;
    std::mutex m_IngestionMutex;                   //!< mutex protecting m_IngestionList

    std::list<class ItrObject *> m_ItrList;        //!< ItrObjects holding ref count to this
    std::list<class SnapshotObject *> m_SnapshotList;
    std::list<class ColumnFamilyObject *> m_ColumnFamilyList;
    std::list<class TLogItrObject *> m_TLogItrList;
    std::list<class TransactionObject *> m_TransactionList;
    std::list<class IngestionObject *> m_IngestionList;

protected:
    static ErlNifResourceType* m_Db_RESOURCE;

public:
    DbObject(rocksdb::DB * DbPtr, bool IsPessimistic = false, bool IsTTL = false); // Open with default CF

    DbObject() = delete;                             // no default construct
    DbObject(const DbObject&) = delete;              // nocopy
    DbObject& operator=(const DbObject&) = delete;   // nocopyassign

    virtual ~DbObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    // manual back link to Snapshot ColumnFamilyObject holding reference to this
    bool AddColumnFamilyReference(class ColumnFamilyObject *);

    void RemoveColumnFamilyReference(class ColumnFamilyObject *);

    // manual back link to ItrObjects holding reference to this
    bool AddReference(class ItrObject *);

    void RemoveReference(class ItrObject *);

     // manual back link to Snapshot DbObjects holding reference to this
    bool AddSnapshotReference(class SnapshotObject *);

    void RemoveSnapshotReference(class SnapshotObject *);

    // manual back link to ItrObjects holding reference to this
    bool AddTLogReference(class TLogItrObject *);

    void RemoveTLogReference(class TLogItrObject *);

     // manual back link to  Transaction Objects holding reference to this
    bool AddTransactionReference(class TransactionObject *);

    void RemoveTransactionReference(class TransactionObject *);

    // manual back link to prepared ingestions holding reference to this
    bool AddIngestionReference(class IngestionObject *);

    void RemoveIngestionReference(class IngestionObject *);

    static void CreateDbObjectType(ErlNifEnv * Env);

    static DbObject * CreateDbObject(rocksdb::DB * Db, bool IsPessimistic = false, bool IsTTL = false);

    static DbObject * RetrieveDbObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void DbObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class DbObject

/**
 * Per ColumnFamilyObject object.  Created as erlang reference.
 */
class ColumnFamilyObject : public ErlRefObject
{
public:
    rocksdb::ColumnFamilyHandle* m_ColumnFamily;
    ReferencePtr<DbObject> m_DbPtr;

protected:
    static ErlNifResourceType* m_ColumnFamily_RESOURCE;

public:
    ColumnFamilyObject(DbObject * Db, rocksdb::ColumnFamilyHandle* Handle);
    ColumnFamilyObject() = delete;                                       // no default construct
    ColumnFamilyObject(const ColumnFamilyObject &) = delete;             // no copy
    ColumnFamilyObject & operator=(const ColumnFamilyObject &) = delete; // no assignment

    virtual ~ColumnFamilyObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateColumnFamilyObjectType(ErlNifEnv * Env);

    static ColumnFamilyObject * CreateColumnFamilyObject(DbObject * Db, rocksdb::ColumnFamilyHandle* m_ColumnFamily);

    static ColumnFamilyObject * RetrieveColumnFamilyObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void ColumnFamilyObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class ColumnFamilyObject

/**
 * Per Snapshot object.  Created as erlang reference.
 */
class SnapshotObject : public ErlRefObject
{
public:
    const rocksdb::Snapshot* m_Snapshot;

    ReferencePtr<DbObject> m_DbPtr;

    //Mutex m_ItrMutex;                       //!< mutex protecting m_ItrList -- NOT REALLY USED?
    std::list<class ItrObject *> m_ItrList;   //!< ItrObjects holding ref count to this

protected:
    static ErlNifResourceType* m_DbSnapshot_RESOURCE;

public:
    SnapshotObject(DbObject * Db, const rocksdb::Snapshot * Snapshot);

    SnapshotObject() = delete;                                   // no default construct
    SnapshotObject(const SnapshotObject &) = delete;             // no copy
    SnapshotObject & operator=(const SnapshotObject &) = delete; // no assignment

    virtual ~SnapshotObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateSnapshotObjectType(ErlNifEnv * Env);

    static SnapshotObject * CreateSnapshotObject(DbObject * Db, const rocksdb::Snapshot* Snapshot);

    static SnapshotObject * RetrieveSnapshotObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void SnapshotObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class SnapshotObject


class TransactionObject;

/**
 * Per Iterator object.  Created as erlang reference.
 */
class ItrObject : public ErlRefObject
{
public:
    ReferencePtr<SnapshotObject> m_SnapshotPtr; //!< read snapshot, alive as long as the iterator
    ReferencePtr<TransactionObject> m_TxPtr;    //!< transaction iterated over, if any
    std::vector<ColumnFamilyObject *> m_PinnedColumnFamilies; //!< one reference each, see KeepColumnFamily
    std::mutex m_Mutex;                         //!< RocksDB iterators need external synchronization
    rocksdb::Iterator * m_Iterator;
    std::shared_ptr<erocksdb::ErlEnvCtr> env;
    ReferencePtr<DbObject> m_DbPtr;

    rocksdb::Slice *upper_bound_slice;
    rocksdb::Slice *lower_bound_slice;

    //!< erlang resources (column family, transaction) kept alive with
    //!< enif_keep_resource so they cannot be garbage collected and freed
    //!< while this iterator still references them. Released in ~ItrObject.
    std::list<void *> m_KeptResources;

protected:
    static ErlNifResourceType* m_Itr_RESOURCE;

public:
    ItrObject(DbObject *, std::shared_ptr<erocksdb::ErlEnvCtr> Env, rocksdb::Iterator * Iterator);

    ItrObject() = delete;                              // no default construct
    ItrObject(const ItrObject &) = delete;             // no copy
    ItrObject & operator=(const ItrObject &) = delete; // no assignment

    virtual ~ItrObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateItrObjectType(ErlNifEnv * Env);

    static ItrObject * CreateItrObject(DbObject * Db, std::shared_ptr<erocksdb::ErlEnvCtr> Env, rocksdb::Iterator * Iterator);

    static ItrObject * RetrieveItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm,
                                         bool ItrClosing=false);

    static void ItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

    void SetUpperBoundSlice(rocksdb::Slice*);

    void SetLowerBoundSlice(rocksdb::Slice*);

    //!< Pin an erlang resource (e.g. column family, transaction) for this
    //!< iterator's lifetime so GC cannot free it while the iterator uses it.
    void KeepResource(void * Resource);

    //!< Keep the read snapshot alive for this iterator's lifetime: RocksDB
    //!< reads it again when the iterator refreshes. The reference stops an
    //!< explicit release from freeing it; enif_keep_resource stops garbage
    //!< collection, whose cleanup would otherwise block a scheduler waiting
    //!< for this reference to go away.
    void KeepSnapshot(SnapshotObject * Snapshot);

    //!< Same for the transaction an iterator reads: release_transaction must
    //!< not delete it while the iterator still uses it.
    void KeepTransaction(TransactionObject * Tx);

    //!< Same for column families whose handles RocksDB keeps in the iterator
    //!< (coalescing iterator): destroy_column_family must wait for it.
    void KeepColumnFamily(ColumnFamilyObject * Cf);
};  // class ItrObject

/**
 * Per Iterator object.  Created as erlang reference.
 */
class TLogItrObject : public ErlRefObject
{
public:
    rocksdb::WalIterator * m_Iter;
    ReferencePtr<DbObject> m_DbPtr;
    std::mutex m_Mutex;   //!< RocksDB iterators need external synchronization

protected:
    static ErlNifResourceType* m_TLogItr_RESOURCE;

public:
    TLogItrObject(DbObject *, rocksdb::WalIterator * Itr);

    TLogItrObject() = delete;                                  // no default construct
    TLogItrObject(const TLogItrObject &) = delete;             // no copy
    TLogItrObject & operator=(const TLogItrObject &) = delete; // no assignment

    virtual ~TLogItrObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateTLogItrObjectType(ErlNifEnv * Env);

    static TLogItrObject * CreateTLogItrObject(DbObject * Db, rocksdb::WalIterator * Itr);

    static TLogItrObject * RetrieveTLogItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void TLogItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class TLogItrObject


/**
 * A prepared, not yet committed, external file ingestion
 * (DB::PrepareFileIngestion). Created as erlang reference.
 *
 * RocksDB requires the handle to never outlive its DB: the object holds a
 * reference to the DbObject and is closed by DbObject::Shutdown, and dropping
 * a handle that was not committed rolls the ingestion back.
 */
class IngestionObject : public ErlRefObject
{
public:
    ReferencePtr<DbObject> m_DbPtr;

protected:
    std::mutex m_HandleMutex;                                   //!< protects m_Handle
    std::unique_ptr<rocksdb::FileIngestionHandle> m_Handle;     //!< NULL once committed or aborted

    static ErlNifResourceType* m_Ingestion_RESOURCE;

public:
    IngestionObject(DbObject *, std::unique_ptr<rocksdb::FileIngestionHandle> Handle);

    IngestionObject() = delete;
    IngestionObject(const IngestionObject &) = delete;
    IngestionObject & operator=(const IngestionObject &) = delete;

    virtual ~IngestionObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    // Take the prepared handle out of the object. Returns NULL if it was
    // already committed or aborted.
    std::unique_ptr<rocksdb::FileIngestionHandle> TakeHandle();

    // Put back a handle taken with TakeHandle (used when a batch commit is
    // rejected before reaching RocksDB).
    void RestoreHandle(std::unique_ptr<rocksdb::FileIngestionHandle> Handle);

    static void CreateIngestionObjectType(ErlNifEnv * Env);

    static IngestionObject * CreateIngestionObject(DbObject * Db, std::unique_ptr<rocksdb::FileIngestionHandle> Handle);

    static IngestionObject * RetrieveIngestionObject(ErlNifEnv * Env, const ERL_NIF_TERM & Term);

    static void IngestionObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class IngestionObject



/**
 * BackupEngine object.  Created as erlang reference.
 */
class BackupEngineObject : public ErlRefObject
{
public:
    rocksdb::BackupEngine* m_BackupEngine;                                   // NULL or rocksdb BackupEngine object


protected:
    static ErlNifResourceType* m_BackupEngine_RESOURCE;

public:
    BackupEngineObject(rocksdb::BackupEngine * BackupEnginePtr);

    BackupEngineObject() = delete;                                       // no default construct
    BackupEngineObject(const BackupEngineObject&) = delete;              // nocopy
    BackupEngineObject& operator=(const BackupEngineObject&) = delete;   // nocopyassign

    virtual ~BackupEngineObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateBackupEngineObjectType(ErlNifEnv * Env);

    static BackupEngineObject * CreateBackupEngineObject(rocksdb::BackupEngine * BackupEngine);

    static BackupEngineObject * RetrieveBackupEngineObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void BackupEngineObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class BackupEngineObject
    //

/**
 * Per Transaction object.  Created as erlang reference.
 */
class TransactionObject : public ErlRefObject
{
public:
    rocksdb::Transaction* m_Tx;
    ReferencePtr<DbObject> m_DbPtr;
    //!< RocksDB leaves it to the caller to synchronize access to a
    //!< transaction: every NIF using m_Tx, and iterators over it, hold this.
    std::mutex m_Mutex;

protected:
    static ErlNifResourceType* m_Transaction_RESOURCE;

public:
    TransactionObject(DbObject *, rocksdb::Transaction * Tx);

    TransactionObject() = delete;                                   // no default construct
    TransactionObject(const TransactionObject &) = delete;             // no copy
    TransactionObject & operator=(const TransactionObject &) = delete; // no assignment

    virtual ~TransactionObject();

    virtual void ReleaseNative() override;

    virtual void Shutdown();

    static void CreateTransactionObjectType(ErlNifEnv * Env);

    static TransactionObject * CreateTransactionObject(DbObject * Db, rocksdb::Transaction * Tx);

    static TransactionObject * RetrieveTransactionObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void TransactionObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class TransactionObject

} // namespace erocksdb


#endif  // INCL_REFOBJECTS_H
