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

#include "rocksdb/sst_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

#include "atoms.h"
#include "erocksdb_db.h"
#include "sst_file_writer.h"
#include "util.h"

namespace erocksdb {

ErlNifResourceType * SstFileWriterObject::m_SstFileWriter_RESOURCE(NULL);

void
SstFileWriterObject::CreateSstFileWriterType(ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_SstFileWriter_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_SstFileWriter",
                                            &SstFileWriterObject::SstFileWriterResourceCleanup,
                                            flags, NULL);
    return;
}

void
SstFileWriterObject::SstFileWriterResourceCleanup(ErlNifEnv * /*env*/, void * arg)
{
    SstFileWriterObject* writer_ptr = (SstFileWriterObject *)arg;
    writer_ptr->~SstFileWriterObject();
    writer_ptr = nullptr;
    return;
}

SstFileWriterObject *
SstFileWriterObject::CreateSstFileWriterResource(std::unique_ptr<rocksdb::SstFileWriter> writer)
{
    SstFileWriterObject * ret_ptr;
    void * alloc_ptr;

    alloc_ptr = enif_alloc_resource(m_SstFileWriter_RESOURCE, sizeof(SstFileWriterObject));
    ret_ptr = new (alloc_ptr) SstFileWriterObject(std::move(writer));
    return ret_ptr;
}

SstFileWriterObject *
SstFileWriterObject::RetrieveSstFileWriterResource(ErlNifEnv * Env, const ERL_NIF_TERM & term)
{
    SstFileWriterObject * ret_ptr;
    if (!enif_get_resource(Env, term, m_SstFileWriter_RESOURCE, (void **)&ret_ptr))
        return NULL;
    return ret_ptr;
}

SstFileWriterObject::SstFileWriterObject(std::unique_ptr<rocksdb::SstFileWriter> writer)
    : writer_(std::move(writer)) {}

SstFileWriterObject::~SstFileWriterObject()
{
    if(writer_)
    {
        writer_.reset();
    }
    return;
}

rocksdb::SstFileWriter* SstFileWriterObject::writer() {
    return writer_.get();
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
SstFileWriterOpen(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: Options list
    // argv[1]: FilePath (binary/string)

    rocksdb::Options options;
    ERL_NIF_TERM head, tail;
    tail = argv[0];

    // Parse options - each option is a tuple {atom, value}
    while(enif_get_list_cell(env, tail, &head, &tail)) {
        // parse_db_option and parse_cf_option handle the tuple internally
        ERL_NIF_TERM result = parse_db_option(env, head, options);
        if (result != ATOM_OK) {
            result = parse_cf_option(env, head, options);
        }
        // Ignore unknown options to be lenient
    }

    // Get file path
    std::string file_path;
    if (!get_file_path(env, argv[1], file_path)) {
        return enif_make_badarg(env);
    }

    // Create EnvOptions from Options
    rocksdb::EnvOptions env_options;

    // Create the SstFileWriter
    auto sst_writer = std::make_unique<rocksdb::SstFileWriter>(env_options, options);

    // Open the file for writing
    rocksdb::Status status = sst_writer->Open(file_path);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    // Wrap in resource
    auto writer_ptr = SstFileWriterObject::CreateSstFileWriterResource(std::move(sst_writer));
    ERL_NIF_TERM result = enif_make_resource(env, writer_ptr);
    enif_release_resource(writer_ptr);

    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
SstFileWriterPut(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: Key (binary)
    // argv[2]: Value (binary)

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    ErlNifBinary key, value;
    if (!enif_inspect_binary(env, argv[1], &key) ||
        !enif_inspect_binary(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    rocksdb::Slice key_slice((const char*)key.data, key.size);
    rocksdb::Slice value_slice((const char*)value.data, value.size);

    rocksdb::Status status = writer_ptr->writer()->Put(key_slice, value_slice);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
SstFileWriterPutEntity(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: Key (binary)
    // argv[2]: Columns (list of {Name, Value} tuples)

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    ErlNifBinary key;
    if (!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    rocksdb::Slice key_slice((const char*)key.data, key.size);

    // Parse columns list - we need to store the data in vectors to keep slices valid
    std::vector<std::string> col_names;
    std::vector<std::string> col_values;
    ERL_NIF_TERM head, tail;
    tail = argv[2];

    while(enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM* column;
        int arity;
        if (enif_get_tuple(env, head, &arity, &column) && 2 == arity) {
            ErlNifBinary col_name_bin, col_value_bin;
            if (!enif_inspect_binary(env, column[0], &col_name_bin) ||
                !enif_inspect_binary(env, column[1], &col_value_bin)) {
                return enif_make_badarg(env);
            }
            col_names.emplace_back((const char*)col_name_bin.data, col_name_bin.size);
            col_values.emplace_back((const char*)col_value_bin.data, col_value_bin.size);
        } else {
            return enif_make_badarg(env);
        }
    }

    // Build WideColumns from stored strings
    rocksdb::WideColumns columns;
    for (size_t i = 0; i < col_names.size(); ++i) {
        columns.push_back({
            rocksdb::Slice(col_names[i]),
            rocksdb::Slice(col_values[i])
        });
    }

    rocksdb::Status status = writer_ptr->writer()->PutEntity(key_slice, columns);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
SstFileWriterMerge(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: Key (binary)
    // argv[2]: Value (binary)

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    ErlNifBinary key, value;
    if (!enif_inspect_binary(env, argv[1], &key) ||
        !enif_inspect_binary(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    rocksdb::Slice key_slice((const char*)key.data, key.size);
    rocksdb::Slice value_slice((const char*)value.data, value.size);

    rocksdb::Status status = writer_ptr->writer()->Merge(key_slice, value_slice);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
SstFileWriterDelete(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: Key (binary)

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    ErlNifBinary key;
    if (!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    rocksdb::Slice key_slice((const char*)key.data, key.size);

    rocksdb::Status status = writer_ptr->writer()->Delete(key_slice);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
SstFileWriterDeleteRange(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: Begin key (binary)
    // argv[2]: End key (binary)

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    ErlNifBinary begin_key, end_key;
    if (!enif_inspect_binary(env, argv[1], &begin_key) ||
        !enif_inspect_binary(env, argv[2], &end_key)) {
        return enif_make_badarg(env);
    }

    rocksdb::Slice begin_slice((const char*)begin_key.data, begin_key.size);
    rocksdb::Slice end_slice((const char*)end_key.data, end_key.size);

    rocksdb::Status status = writer_ptr->writer()->DeleteRange(begin_slice, end_slice);
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
SstFileWriterFinish(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource
    // argv[1]: (optional) with_file_info atom

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    bool with_file_info = false;
    if (argc > 1 && argv[1] == ATOM_WITH_FILE_INFO) {
        with_file_info = true;
    }

    if (with_file_info) {
        rocksdb::ExternalSstFileInfo file_info;
        rocksdb::Status status = writer_ptr->writer()->Finish(&file_info);
        if (!status.ok()) {
            return error_tuple(env, ATOM_ERROR, status);
        }

        // Build file info map
        ERL_NIF_TERM keys[9];
        ERL_NIF_TERM values[9];
        int idx = 0;

        keys[idx] = ATOM_FILE_PATH;
        values[idx] = make_binary_from_string(env, file_info.file_path);
        idx++;

        keys[idx] = ATOM_SMALLEST_KEY;
        values[idx] = make_binary_from_string(env, file_info.smallest_key);
        idx++;

        keys[idx] = ATOM_LARGEST_KEY;
        values[idx] = make_binary_from_string(env, file_info.largest_key);
        idx++;

        keys[idx] = ATOM_SMALLEST_RANGE_DEL_KEY;
        values[idx] = make_binary_from_string(env, file_info.smallest_range_del_key);
        idx++;

        keys[idx] = ATOM_LARGEST_RANGE_DEL_KEY;
        values[idx] = make_binary_from_string(env, file_info.largest_range_del_key);
        idx++;

        keys[idx] = ATOM_FILE_SIZE;
        values[idx] = enif_make_uint64(env, file_info.file_size);
        idx++;

        keys[idx] = ATOM_NUM_ENTRIES;
        values[idx] = enif_make_uint64(env, file_info.num_entries);
        idx++;

        keys[idx] = ATOM_NUM_RANGE_DEL_ENTRIES;
        values[idx] = enif_make_uint64(env, file_info.num_range_del_entries);
        idx++;

        keys[idx] = ATOM_SEQUENCE_NUMBER;
        values[idx] = enif_make_uint64(env, file_info.sequence_number);
        idx++;

        ERL_NIF_TERM info_map;
        enif_make_map_from_arrays(env, keys, values, idx, &info_map);

        return enif_make_tuple2(env, ATOM_OK, info_map);
    } else {
        rocksdb::Status status = writer_ptr->writer()->Finish();
        if (!status.ok()) {
            return error_tuple(env, ATOM_ERROR, status);
        }
        return ATOM_OK;
    }
}

ERL_NIF_TERM
SstFileWriterFileSize(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    // argv[0]: SstFileWriter resource

    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return enif_make_badarg(env);
    }

    uint64_t file_size = writer_ptr->writer()->FileSize();
    return enif_make_uint64(env, file_size);
}

ERL_NIF_TERM
ReleaseSstFileWriter(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    SstFileWriterObject* writer_ptr = SstFileWriterObject::RetrieveSstFileWriterResource(env, argv[0]);
    if (nullptr == writer_ptr) {
        return ATOM_OK;
    }
    // Resource will be cleaned up by GC
    return ATOM_OK;
}

}  // namespace erocksdb
