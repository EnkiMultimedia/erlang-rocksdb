// Copyright (c) 2018-2025 Benoit Chesneau
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

#include <string>
#include <memory>

#include "rocksdb/sst_file_reader.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"

#include "atoms.h"
#include "erocksdb_db.h"
#include "sst_file_reader.h"
#include "util.h"

namespace erocksdb {

ErlNifResourceType * SstFileReaderObject::m_SstFileReader_RESOURCE(NULL);

// Iterator for SstFileReader - separate resource type
class SstFileReaderItrObject {
protected:
    static ErlNifResourceType* m_SstFileReaderItr_RESOURCE;

public:
    rocksdb::Iterator* m_Iterator;
    SstFileReaderObject* m_ReaderRef;  // Keep reader alive

    SstFileReaderItrObject(rocksdb::Iterator* iter, SstFileReaderObject* reader)
        : m_Iterator(iter), m_ReaderRef(reader) {}

    ~SstFileReaderItrObject() {
        if (m_Iterator) {
            delete m_Iterator;
            m_Iterator = nullptr;
        }
    }

    static void CreateSstFileReaderItrType(ErlNifEnv* env) {
        ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
        m_SstFileReaderItr_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_SstFileReaderItr",
                                                &SstFileReaderItrResourceCleanup,
                                                flags, NULL);
    }

    static void SstFileReaderItrResourceCleanup(ErlNifEnv* /*env*/, void* arg) {
        SstFileReaderItrObject* itr_ptr = (SstFileReaderItrObject*)arg;
        itr_ptr->~SstFileReaderItrObject();
    }

    static SstFileReaderItrObject* CreateSstFileReaderItrResource(rocksdb::Iterator* iter, SstFileReaderObject* reader) {
        void* alloc_ptr = enif_alloc_resource(m_SstFileReaderItr_RESOURCE, sizeof(SstFileReaderItrObject));
        return new (alloc_ptr) SstFileReaderItrObject(iter, reader);
    }

    static SstFileReaderItrObject* RetrieveSstFileReaderItrResource(ErlNifEnv* env, const ERL_NIF_TERM& term) {
        SstFileReaderItrObject* ret_ptr;
        if (!enif_get_resource(env, term, m_SstFileReaderItr_RESOURCE, (void**)&ret_ptr))
            return NULL;
        return ret_ptr;
    }
};

ErlNifResourceType* SstFileReaderItrObject::m_SstFileReaderItr_RESOURCE(NULL);

void
SstFileReaderObject::CreateSstFileReaderType(ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_SstFileReader_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_SstFileReader",
                                            &SstFileReaderObject::SstFileReaderResourceCleanup,
                                            flags, NULL);

    // Also create the iterator resource type
    SstFileReaderItrObject::CreateSstFileReaderItrType(env);
    return;
}

void
SstFileReaderObject::SstFileReaderResourceCleanup(ErlNifEnv * /*env*/, void * arg)
{
    SstFileReaderObject* reader_ptr = (SstFileReaderObject *)arg;
    reader_ptr->~SstFileReaderObject();
    reader_ptr = nullptr;
    return;
}

SstFileReaderObject *
SstFileReaderObject::CreateSstFileReaderResource(std::unique_ptr<rocksdb::SstFileReader> reader)
{
    SstFileReaderObject * ret_ptr;
    void * alloc_ptr;

    alloc_ptr = enif_alloc_resource(m_SstFileReader_RESOURCE, sizeof(SstFileReaderObject));
    ret_ptr = new (alloc_ptr) SstFileReaderObject(std::move(reader));
    return ret_ptr;
}

SstFileReaderObject *
SstFileReaderObject::RetrieveSstFileReaderResource(ErlNifEnv * Env, const ERL_NIF_TERM & term)
{
    SstFileReaderObject * ret_ptr;
    if (!enif_get_resource(Env, term, m_SstFileReader_RESOURCE, (void **)&ret_ptr))
        return NULL;
    return ret_ptr;
}

SstFileReaderObject::SstFileReaderObject(std::unique_ptr<rocksdb::SstFileReader> reader)
    : reader_(std::move(reader)) {}

SstFileReaderObject::~SstFileReaderObject()
{
    if(reader_)
    {
        reader_.reset();
    }
    return;
}

rocksdb::SstFileReader* SstFileReaderObject::reader() {
    return reader_.get();
}

// Helper to get file path from Erlang term (string list or binary)
static bool get_file_path(ErlNifEnv* env, ERL_NIF_TERM term, std::string& path)
{
    // Try as binary first
    ErlNifBinary bin;
    if (enif_inspect_binary(env, term, &bin)) {
        path.assign((const char*)bin.data, bin.size);
        return true;
    }

    // Try as iolist (includes strings)
    if (enif_inspect_iolist_as_binary(env, term, &bin)) {
        path.assign((const char*)bin.data, bin.size);
        return true;
    }

    return false;
}

// Helper to make binary from std::string
static ERL_NIF_TERM make_binary_from_string(ErlNifEnv* env, const std::string& str)
{
    ERL_NIF_TERM result;
    unsigned char* buf = enif_make_new_binary(env, str.size(), &result);
    memcpy(buf, str.data(), str.size());
    return result;
}

ERL_NIF_TERM
SstFileReaderOpen(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: Options list
    // argv[1]: FilePath (binary/string)

    rocksdb::Options options;
    ERL_NIF_TERM head, tail;
    tail = argv[0];

    // Parse options
    while(enif_get_list_cell(env, tail, &head, &tail)) {
        ERL_NIF_TERM result = parse_db_option(env, head, options);
        if (result != ATOM_OK) {
            result = parse_cf_option(env, head, options);
        }
    }

    // Get file path
    std::string file_path;
    if (!get_file_path(env, argv[1], file_path)) {
        return enif_make_badarg(env);
    }

    // Create the SstFileReader
    auto sst_reader = std::make_unique<rocksdb::SstFileReader>(options);

    // Open the file for reading
    rocksdb::Status status = sst_reader->Open(file_path);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    // Wrap in resource
    auto reader_ptr = SstFileReaderObject::CreateSstFileReaderResource(std::move(sst_reader));
    ERL_NIF_TERM result = enif_make_resource(env, reader_ptr);
    enif_release_resource(reader_ptr);

    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
SstFileReaderIterator(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileReader resource
    // argv[1]: ReadOptions list

    SstFileReaderObject* reader_ptr = SstFileReaderObject::RetrieveSstFileReaderResource(env, argv[0]);
    if (nullptr == reader_ptr) {
        return enif_make_badarg(env);
    }

    // Parse read options
    rocksdb::ReadOptions read_options;
    fold(env, argv[1], parse_read_option, read_options);

    // Create iterator
    rocksdb::Iterator* iter = reader_ptr->reader()->NewIterator(read_options);
    if (iter == nullptr) {
        return error_tuple(env, ATOM_ERROR, "Failed to create iterator");
    }

    // Keep a reference to the reader to prevent GC while iterator is alive
    enif_keep_resource(reader_ptr);

    // Wrap in resource
    auto itr_ptr = SstFileReaderItrObject::CreateSstFileReaderItrResource(iter, reader_ptr);
    ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);
    enif_release_resource(itr_ptr);

    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
SstFileReaderGetTableProperties(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileReader resource

    SstFileReaderObject* reader_ptr = SstFileReaderObject::RetrieveSstFileReaderResource(env, argv[0]);
    if (nullptr == reader_ptr) {
        return enif_make_badarg(env);
    }

    std::shared_ptr<const rocksdb::TableProperties> props = reader_ptr->reader()->GetTableProperties();
    if (!props) {
        return error_tuple(env, ATOM_ERROR, "Failed to get table properties");
    }

    // Build table properties map with all available fields
    std::vector<ERL_NIF_TERM> keys;
    std::vector<ERL_NIF_TERM> values;

    // Basic counts
    keys.push_back(ATOM_NUM_ENTRIES);
    values.push_back(enif_make_uint64(env, props->num_entries));

    keys.push_back(ATOM_NUM_DELETIONS);
    values.push_back(enif_make_uint64(env, props->num_deletions));

    keys.push_back(ATOM_NUM_MERGE_OPERANDS);
    values.push_back(enif_make_uint64(env, props->num_merge_operands));

    keys.push_back(ATOM_NUM_RANGE_DELETIONS);
    values.push_back(enif_make_uint64(env, props->num_range_deletions));

    // Sizes
    keys.push_back(ATOM_DATA_SIZE);
    values.push_back(enif_make_uint64(env, props->data_size));

    keys.push_back(ATOM_INDEX_SIZE);
    values.push_back(enif_make_uint64(env, props->index_size));

    keys.push_back(ATOM_INDEX_PARTITIONS);
    values.push_back(enif_make_uint64(env, props->index_partitions));

    keys.push_back(ATOM_TOP_LEVEL_INDEX_SIZE);
    values.push_back(enif_make_uint64(env, props->top_level_index_size));

    keys.push_back(ATOM_FILTER_SIZE);
    values.push_back(enif_make_uint64(env, props->filter_size));

    keys.push_back(ATOM_RAW_KEY_SIZE);
    values.push_back(enif_make_uint64(env, props->raw_key_size));

    keys.push_back(ATOM_RAW_VALUE_SIZE);
    values.push_back(enif_make_uint64(env, props->raw_value_size));

    keys.push_back(ATOM_NUM_DATA_BLOCKS);
    values.push_back(enif_make_uint64(env, props->num_data_blocks));

    // Format info
    keys.push_back(ATOM_FORMAT_VERSION);
    values.push_back(enif_make_uint64(env, props->format_version));

    keys.push_back(ATOM_FIXED_KEY_LEN);
    values.push_back(enif_make_uint64(env, props->fixed_key_len));

    // Column family info
    keys.push_back(ATOM_COLUMN_FAMILY_ID);
    values.push_back(enif_make_uint64(env, props->column_family_id));

    keys.push_back(ATOM_COLUMN_FAMILY_NAME);
    values.push_back(make_binary_from_string(env, props->column_family_name));

    // Names/metadata
    keys.push_back(ATOM_FILTER_POLICY_NAME);
    values.push_back(make_binary_from_string(env, props->filter_policy_name));

    keys.push_back(ATOM_COMPARATOR_NAME);
    values.push_back(make_binary_from_string(env, props->comparator_name));

    keys.push_back(ATOM_MERGE_OPERATOR_NAME);
    values.push_back(make_binary_from_string(env, props->merge_operator_name));

    keys.push_back(ATOM_PREFIX_EXTRACTOR_NAME);
    values.push_back(make_binary_from_string(env, props->prefix_extractor_name));

    keys.push_back(ATOM_PROPERTY_COLLECTORS_NAMES);
    values.push_back(make_binary_from_string(env, props->property_collectors_names));

    keys.push_back(ATOM_COMPRESSION_NAME);
    values.push_back(make_binary_from_string(env, props->compression_name));

    keys.push_back(ATOM_COMPRESSION_OPTIONS);
    values.push_back(make_binary_from_string(env, props->compression_options));

    // Timestamps
    keys.push_back(ATOM_CREATION_TIME);
    values.push_back(enif_make_uint64(env, props->creation_time));

    keys.push_back(ATOM_OLDEST_KEY_TIME);
    values.push_back(enif_make_uint64(env, props->oldest_key_time));

    keys.push_back(ATOM_FILE_CREATION_TIME);
    values.push_back(enif_make_uint64(env, props->file_creation_time));

    // Compression estimates
    keys.push_back(ATOM_SLOW_COMPRESSION_ESTIMATED_DATA_SIZE);
    values.push_back(enif_make_uint64(env, props->slow_compression_estimated_data_size));

    keys.push_back(ATOM_FAST_COMPRESSION_ESTIMATED_DATA_SIZE);
    values.push_back(enif_make_uint64(env, props->fast_compression_estimated_data_size));

    keys.push_back(ATOM_EXTERNAL_SST_FILE_GLOBAL_SEQNO_OFFSET);
    values.push_back(enif_make_uint64(env, props->external_sst_file_global_seqno_offset));

    ERL_NIF_TERM props_map;
    enif_make_map_from_arrays(env, keys.data(), values.data(), keys.size(), &props_map);

    return enif_make_tuple2(env, ATOM_OK, props_map);
}

ERL_NIF_TERM
SstFileReaderVerifyChecksum(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileReader resource
    // argv[1]: (optional) ReadOptions list

    SstFileReaderObject* reader_ptr = SstFileReaderObject::RetrieveSstFileReaderResource(env, argv[0]);
    if (nullptr == reader_ptr) {
        return enif_make_badarg(env);
    }

    rocksdb::ReadOptions read_options;
    if (argc > 1) {
        fold(env, argv[1], parse_read_option, read_options);
    }

    rocksdb::Status status = reader_ptr->reader()->VerifyChecksum(read_options);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
ReleaseSstFileReader(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    SstFileReaderObject* reader_ptr = SstFileReaderObject::RetrieveSstFileReaderResource(env, argv[0]);
    if (nullptr == reader_ptr) {
        return ATOM_OK;
    }
    // Resource will be cleaned up by GC
    return ATOM_OK;
}

// Iterator operations for SstFileReader iterator
ERL_NIF_TERM
SstFileReaderIteratorMove(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileReaderItr resource
    // argv[1]: Action (first, last, next, prev, {seek, Key}, {seek_for_prev, Key})

    SstFileReaderItrObject* itr_ptr = SstFileReaderItrObject::RetrieveSstFileReaderItrResource(env, argv[0]);
    if (nullptr == itr_ptr || nullptr == itr_ptr->m_Iterator) {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* iter = itr_ptr->m_Iterator;

    if (argv[1] == ATOM_FIRST) {
        iter->SeekToFirst();
    } else if (argv[1] == ATOM_LAST) {
        iter->SeekToLast();
    } else if (argv[1] == ATOM_NEXT) {
        if (!iter->Valid()) {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }
        iter->Next();
    } else if (argv[1] == ATOM_PREV) {
        if (!iter->Valid()) {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }
        iter->Prev();
    } else {
        // Check for {seek, Key} or {seek_for_prev, Key}
        int arity;
        const ERL_NIF_TERM* tuple;
        if (enif_get_tuple(env, argv[1], &arity, &tuple) && arity == 2) {
            ErlNifBinary key;
            if (!enif_inspect_binary(env, tuple[1], &key)) {
                return enif_make_badarg(env);
            }
            rocksdb::Slice key_slice((const char*)key.data, key.size);

            if (tuple[0] == ATOM_SEEK) {
                iter->Seek(key_slice);
            } else if (tuple[0] == ATOM_SEEK_FOR_PREV) {
                iter->SeekForPrev(key_slice);
            } else {
                return enif_make_badarg(env);
            }
        } else {
            return enif_make_badarg(env);
        }
    }

    // Check status
    rocksdb::Status status = iter->status();
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    if (!iter->Valid()) {
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
    }

    // Return {ok, Key, Value}
    rocksdb::Slice key = iter->key();
    rocksdb::Slice value = iter->value();

    ERL_NIF_TERM key_term, value_term;
    unsigned char* key_buf = enif_make_new_binary(env, key.size(), &key_term);
    memcpy(key_buf, key.data(), key.size());

    unsigned char* value_buf = enif_make_new_binary(env, value.size(), &value_term);
    memcpy(value_buf, value.data(), value.size());

    return enif_make_tuple3(env, ATOM_OK, key_term, value_term);
}

ERL_NIF_TERM
SstFileReaderIteratorClose(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    SstFileReaderItrObject* itr_ptr = SstFileReaderItrObject::RetrieveSstFileReaderItrResource(env, argv[0]);
    if (nullptr == itr_ptr) {
        return ATOM_OK;
    }

    // Release the reference to the reader that we kept
    if (itr_ptr->m_ReaderRef != nullptr) {
        enif_release_resource(itr_ptr->m_ReaderRef);
        itr_ptr->m_ReaderRef = nullptr;
    }

    // Delete the iterator
    if (itr_ptr->m_Iterator != nullptr) {
        delete itr_ptr->m_Iterator;
        itr_ptr->m_Iterator = nullptr;
    }

    return ATOM_OK;
}

}  // namespace erocksdb
