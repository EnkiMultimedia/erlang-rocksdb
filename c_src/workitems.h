// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
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

#ifndef INCL_WORKITEMS_H
#define INCL_WORKITEMS_H

#include <stdint.h>

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/utilities/checkpoint.h"


#ifndef INCL_MUTEX_H
    #include "mutex.h"
#endif

#ifndef __WORK_RESULT_HPP
    #include "work_result.hpp"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif


namespace erocksdb {

/* Type returned from a work task: */
typedef leofs::async_nif::work_result   work_result;



/**
 * Virtual base class for async NIF work items:
 */
class WorkTask : public RefObject
{
public:

protected:
    ReferencePtr<DbObject> m_DbPtr;             //!< access to database, and holds reference
    ReferencePtr<ColumnFamilyObject> m_CfPtr;   

    ErlNifEnv      *local_env_;
    ERL_NIF_TERM   caller_ref_term;
    ERL_NIF_TERM   caller_pid_term;
    bool           terms_set;

    bool resubmit_work;           //!< true if this work item is loaded for prefetch

    ErlNifPid local_pid;   // maintain for task lifetime (JFW)

 public:

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref);

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr);

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr, ColumnFamilyObject * CfPtr);

    virtual ~WorkTask();

    virtual void prepare_recycle();
    virtual void recycle();

    virtual ErlNifEnv *local_env()         { return local_env_; }

    // call local_env() since the virtual creates the data in MoveTask
    const ERL_NIF_TERM& caller_ref()       { local_env(); return caller_ref_term; }
    const ERL_NIF_TERM& pid()              { local_env(); return caller_pid_term; }
    bool resubmit() const {return(resubmit_work);}

    virtual work_result operator()()     = 0;

private:
 WorkTask();
 WorkTask(const WorkTask &);
 WorkTask & operator=(const WorkTask &);

};  // class WorkTask


class DestroyTask : public WorkTask
{
protected:
    std::string         db_name;
    rocksdb::Options   options; 

public:
    DestroyTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                         const std::string& db_name_,
                         rocksdb::Options options_)
                  : WorkTask(_caller_env, _caller_ref), db_name(db_name_), options(options_)
    {};

    virtual ~DestroyTask() {};

    virtual work_result operator()()
    {
        rocksdb::Status status = rocksdb::DestroyDB(db_name, options);
        if(!status.ok())
            return work_result(local_env(), ATOM_ERROR_DB_DESTROY, status);
        return work_result(ATOM_OK);
    }   // operator()

};  // class DestroyTask

class RepairTask : public WorkTask
{
protected:
    std::string         db_name;
    rocksdb::Options   options;

public:
    RepairTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                         const std::string& db_name_,
                         rocksdb::Options options_)
                  : WorkTask(_caller_env, _caller_ref), db_name(db_name_), options(options_)
    {};

    virtual ~RepairTask() {};

    virtual work_result operator()()
    {
        rocksdb::Status status = rocksdb::RepairDB(db_name, options);
        if(!status.ok())
            return work_result(local_env(), ATOM_ERROR_DB_DESTROY, status);
        return work_result(ATOM_OK);
    }   // operator()

};  // class RepairTask


class IsEmptyTask : public WorkTask
{
protected:
    ReferencePtr<DbObject> m_DbPtr;

public:
    IsEmptyTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                        DbObject * Db)
                  : WorkTask(_caller_env, _caller_ref), m_DbPtr(Db)
    {};

    virtual ~IsEmptyTask() {};

    virtual work_result operator()()
    {
        DbObject* db_ptr = m_DbPtr.get();
        ERL_NIF_TERM result;
        rocksdb::ReadOptions opts;
        rocksdb::Iterator* itr = db_ptr->m_Db->NewIterator(opts);
        itr->SeekToFirst();
        if (itr->Valid())
        {
            result = erocksdb::ATOM_OK;
        }
        else
        {
            result = erocksdb::ATOM_TRUE;
        }
        delete itr;
        return work_result(result);
    }   // operator()
};  // class IsEmptyTask


/**
 * Background object for async checkpoint creation
 */

class CheckpointTask : public WorkTask
{
protected:
    std::string path;

public:
    CheckpointTask(ErlNifEnv *_caller_env,
                    ERL_NIF_TERM _caller_ref,
                    DbObject *_db_handle,
                    const std::string& path_)
                : WorkTask(_caller_env, _caller_ref, _db_handle),
                path(path_)
    {

    };

    virtual ~CheckpointTask() {};

    virtual work_result operator()()
    {
        rocksdb::Checkpoint* checkpoint;
        rocksdb::Status status;

        status = rocksdb::Checkpoint::Create(m_DbPtr->m_Db, &checkpoint);

        if (status.ok()) {
            status = checkpoint->CreateCheckpoint(path);
            if (status.ok())
            {
                return work_result(ATOM_OK);
            }
        }
        delete checkpoint;

        return work_result(local_env(), ATOM_ERROR, status);
    }   // operator()

};  // class CheckpointTask


} // namespace erocksdb


#endif  // INCL_WORKITEMS_H
