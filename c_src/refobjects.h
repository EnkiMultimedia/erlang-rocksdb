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
#include <atomic>
#include <condition_variable>

#include "erl_nif.h"

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"

namespace rocksdb {
    class DB;
    class ColumnFamilyHandle;
    class Snapshot;
    class Iterator;
    class TransactionLogIterator;
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
    std::atomic<uint32_t> m_CloseRequested;        // 1 once api close called, 2 once thread starts destructor, 3 destructor done

    // DO NOT USE CONTAINER OBJECTS
    //  ... these must be live after destructor called
    std::mutex *m_CloseMutex;                     //!< for erlang forced close
    std::condition_variable *m_CloseCond;         //!< for erlang forced close

public:
    ErlRefObject();

    ErlRefObject(const ErlRefObject&) = delete;              // nocopy
    ErlRefObject& operator=(const ErlRefObject&) = delete;   // nocopyassign

    virtual ~ErlRefObject();

    virtual uint32_t RefDec();

    // allows for secondary close actions IF InitiateCloseRequest returns true
    virtual void Shutdown()=0;

    // the following will sometimes be called AFTER the
    //  destructor ... in which case the vtable is not valid
    static bool InitiateCloseRequest(ErlRefObject * Object);

    static void AwaitCloseAndDestructor(ErlRefObject * Object);
};  // class RefObject


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
    std::mutex m_ItrMutex;                         //!< mutex protecting m_ItrList
    std::mutex m_SnapshotMutex;                    //!< mutex protecting m_SnapshotList
    std::mutex m_ColumnFamilyMutex;                //!< mutex protecting m_ColumnFamily
    std::mutex m_TLogItrMutex;                     //!< mutex protecting m_TransactionLogList
    std::mutex m_TransactionMutex;

    std::list<class ItrObject *> m_ItrList;        //!< ItrObjects holding ref count to this
    std::list<class SnapshotObject *> m_SnapshotList;
    std::list<class ColumnFamilyObject *> m_ColumnFamilyList;
    std::list<class TLogItrObject *> m_TLogItrList;
    std::list<class TransactionObject *> m_TransactionList;

protected:
    static ErlNifResourceType* m_Db_RESOURCE;

public:
    DbObject(rocksdb::DB * DbPtr, bool IsPessimistic = false); // Open with default CF

    DbObject() = delete;                             // no default construct
    DbObject(const DbObject&) = delete;              // nocopy
    DbObject& operator=(const DbObject&) = delete;   // nocopyassign

    virtual ~DbObject();

    virtual void Shutdown();

    // manual back link to Snapshot ColumnFamilyObject holding reference to this
    void AddColumnFamilyReference(class ColumnFamilyObject *);

    void RemoveColumnFamilyReference(class ColumnFamilyObject *);

    // manual back link to ItrObjects holding reference to this
    bool AddReference(class ItrObject *);

    void RemoveReference(class ItrObject *);

     // manual back link to Snapshot DbObjects holding reference to this
    void AddSnapshotReference(class SnapshotObject *);

    void RemoveSnapshotReference(class SnapshotObject *);

    // manual back link to ItrObjects holding reference to this
    void AddTLogReference(class TLogItrObject *);

    void RemoveTLogReference(class TLogItrObject *);

     // manual back link to  Transaction Objects holding reference to this
    void AddTransactionReference(class TransactionObject *);

    void RemoveTransactionReference(class TransactionObject *);

    static void CreateDbObjectType(ErlNifEnv * Env);

    static DbObject * CreateDbObject(rocksdb::DB * Db, bool IsPessimistic = false);

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

    virtual ~ColumnFamilyObject(); // needs to perform free_itr

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

    virtual ~SnapshotObject(); // needs to perform free_itr

    virtual void Shutdown();

    static void CreateSnapshotObjectType(ErlNifEnv * Env);

    static SnapshotObject * CreateSnapshotObject(DbObject * Db, const rocksdb::Snapshot* Snapshot);

    static SnapshotObject * RetrieveSnapshotObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void SnapshotObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class SnapshotObject


/**
 * Per Iterator object.  Created as erlang reference.
 */
class ItrObject : public ErlRefObject
{
public:
    rocksdb::Iterator * m_Iterator;
    std::shared_ptr<erocksdb::ErlEnvCtr> env;
    ReferencePtr<DbObject> m_DbPtr;

    rocksdb::Slice *upper_bound_slice;
    rocksdb::Slice *lower_bound_slice;

protected:
    static ErlNifResourceType* m_Itr_RESOURCE;

public:
    ItrObject(DbObject *, std::shared_ptr<erocksdb::ErlEnvCtr> Env, rocksdb::Iterator * Iterator);

    ItrObject() = delete;                              // no default construct
    ItrObject(const ItrObject &) = delete;             // no copy
    ItrObject & operator=(const ItrObject &) = delete; // no assignment

    virtual ~ItrObject(); // needs to perform free_itr

    virtual void Shutdown();

    static void CreateItrObjectType(ErlNifEnv * Env);

    static ItrObject * CreateItrObject(DbObject * Db, std::shared_ptr<erocksdb::ErlEnvCtr> Env, rocksdb::Iterator * Iterator);

    static ItrObject * RetrieveItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm,
                                         bool ItrClosing=false);

    static void ItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

    void SetUpperBoundSlice(rocksdb::Slice*);

    void SetLowerBoundSlice(rocksdb::Slice*);
};  // class ItrObject

/**
 * Per Iterator object.  Created as erlang reference.
 */
class TLogItrObject : public ErlRefObject
{
public:
    rocksdb::TransactionLogIterator * m_Iter;
    ReferencePtr<DbObject> m_DbPtr;

protected:
    static ErlNifResourceType* m_TLogItr_RESOURCE;

public:
    TLogItrObject(DbObject *, rocksdb::TransactionLogIterator * Itr);

    TLogItrObject() = delete;                                  // no default construct
    TLogItrObject(const TLogItrObject &) = delete;             // no copy
    TLogItrObject & operator=(const TLogItrObject &) = delete; // no assignment

    virtual ~TLogItrObject(); // needs to perform free_itr

    virtual void Shutdown();

    static void CreateTLogItrObjectType(ErlNifEnv * Env);

    static TLogItrObject * CreateTLogItrObject(DbObject * Db, rocksdb::TransactionLogIterator * Itr);

    static TLogItrObject * RetrieveTLogItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void TLogItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class TLogItrObject



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

protected:
    static ErlNifResourceType* m_Transaction_RESOURCE;

public:
    TransactionObject(DbObject *, rocksdb::Transaction * Tx);

    TransactionObject() = delete;                                   // no default construct
    TransactionObject(const TransactionObject &) = delete;             // no copy
    TransactionObject & operator=(const TransactionObject &) = delete; // no assignment

    virtual ~TransactionObject(); // needs to perform free_itr

    virtual void Shutdown();

    static void CreateTransactionObjectType(ErlNifEnv * Env);

    static TransactionObject * CreateTransactionObject(DbObject * Db, rocksdb::Transaction * Tx);

    static TransactionObject * RetrieveTransactionObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void TransactionObjectResourceCleanup(ErlNifEnv *Env, void * Arg);
};  // class TransactionObject

} // namespace erocksdb


#endif  // INCL_REFOBJECTS_H
