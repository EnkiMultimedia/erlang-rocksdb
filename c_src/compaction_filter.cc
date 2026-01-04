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

#include "compaction_filter.h"
#include "atoms.h"
#include "util.h"

#include <cstring>
#include <chrono>

namespace erocksdb {

// Resource type for batch tracking
ErlNifResourceType* m_CompactionBatch_RESOURCE = nullptr;

static void compaction_batch_resource_cleanup(ErlNifEnv* /*env*/, void* arg)
{
    CompactionBatchResource* batch = static_cast<CompactionBatchResource*>(arg);
    batch->~CompactionBatchResource();
}

void CreateCompactionBatchResourceType(ErlNifEnv* env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_CompactionBatch_RESOURCE = enif_open_resource_type(
        env, nullptr, "erocksdb_CompactionBatch",
        compaction_batch_resource_cleanup, flags, nullptr);
}

//--------------------------------------------------------------------
// CompactionBatchResource Implementation
//--------------------------------------------------------------------

CompactionBatchResource::CompactionBatchResource()
    : m_HasResponse(false)
{
}

CompactionBatchResource::~CompactionBatchResource()
{
}

bool CompactionBatchResource::WaitForResponse(unsigned int timeout_ms)
{
    std::unique_lock<std::mutex> lock(m_Mutex);
    bool got_response = m_Cond.wait_for(
        lock,
        std::chrono::milliseconds(timeout_ms),
        [this] { return m_HasResponse; }
    );
    return got_response;
}

void CompactionBatchResource::SetResponse(const std::vector<FilterResult>& results)
{
    std::lock_guard<std::mutex> lock(m_Mutex);
    m_Results = results;
    m_HasResponse = true;
    m_Cond.notify_all();
}

bool CompactionBatchResource::HasResponse() const
{
    std::lock_guard<std::mutex> lock(m_Mutex);
    return m_HasResponse;
}

FilterResult CompactionBatchResource::GetResult(size_t index) const
{
    std::lock_guard<std::mutex> lock(m_Mutex);
    if (index < m_Results.size()) {
        return m_Results[index];
    }
    return FilterResult(FilterDecision::Keep);
}

void CompactionBatchResource::Reset()
{
    std::lock_guard<std::mutex> lock(m_Mutex);
    m_HasResponse = false;
    m_Results.clear();
}

//--------------------------------------------------------------------
// ErlangCompactionFilter Implementation
//--------------------------------------------------------------------

ErlangCompactionFilter::ErlangCompactionFilter(std::vector<FilterRule> rules)
    : m_UseErlangCallback(false)
    , m_Rules(std::move(rules))
    , m_BatchSize(0)
    , m_TimeoutMs(0)
    , m_HandlerDead(false)
{
    memset(&m_HandlerPid, 0, sizeof(m_HandlerPid));
}

ErlangCompactionFilter::ErlangCompactionFilter(
    ErlNifPid handler_pid,
    unsigned int batch_size,
    unsigned int timeout_ms)
    : m_UseErlangCallback(true)
    , m_HandlerPid(handler_pid)
    , m_BatchSize(batch_size)
    , m_TimeoutMs(timeout_ms)
    , m_HandlerDead(false)
{
}

ErlangCompactionFilter::~ErlangCompactionFilter()
{
}

bool ErlangCompactionFilter::Filter(
    int level,
    const rocksdb::Slice& key,
    const rocksdb::Slice& existing_value,
    std::string* new_value,
    bool* value_changed) const
{
    FilterResult result;

    if (m_UseErlangCallback) {
        // Check if handler is known to be dead - skip callback if so
        if (m_HandlerDead) {
            return false;  // Keep all keys when handler is dead
        }
        result = CallErlangHandler(level, key, existing_value);
    } else {
        result = ApplyRules(key, existing_value);
    }

    switch (result.decision) {
        case FilterDecision::Remove:
            return true;  // Delete the key

        case FilterDecision::ChangeValue:
            *new_value = std::move(result.new_value);
            *value_changed = true;
            return false; // Keep but with new value

        case FilterDecision::Keep:
        default:
            return false; // Keep as-is
    }
}

const char* ErlangCompactionFilter::Name() const
{
    return "ErlangCompactionFilter";
}

FilterResult ErlangCompactionFilter::ApplyRules(
    const rocksdb::Slice& key,
    const rocksdb::Slice& value) const
{
    for (const auto& rule : m_Rules) {
        switch (rule.type) {
            case RuleType::KeyPrefix:
                if (key.starts_with(rule.pattern)) {
                    return FilterResult(FilterDecision::Remove);
                }
                break;

            case RuleType::KeySuffix:
                if (key.size() >= rule.pattern.size()) {
                    std::string key_str = key.ToString();
                    if (key_str.substr(key_str.size() - rule.pattern.size()) == rule.pattern) {
                        return FilterResult(FilterDecision::Remove);
                    }
                }
                break;

            case RuleType::KeyContains:
                if (key.ToString().find(rule.pattern) != std::string::npos) {
                    return FilterResult(FilterDecision::Remove);
                }
                break;

            case RuleType::ValueEmpty:
                if (value.empty()) {
                    return FilterResult(FilterDecision::Remove);
                }
                break;

            case RuleType::ValuePrefix:
                if (value.starts_with(rule.pattern)) {
                    return FilterResult(FilterDecision::Remove);
                }
                break;

            case RuleType::TTLFromKey:
                if (CheckTTL(key, rule.offset, rule.length, rule.ttl_seconds)) {
                    return FilterResult(FilterDecision::Remove);
                }
                break;

            case RuleType::Always:
                return FilterResult(FilterDecision::Remove);
        }
    }

    return FilterResult(FilterDecision::Keep);
}

bool ErlangCompactionFilter::CheckTTL(
    const rocksdb::Slice& key,
    size_t offset,
    size_t length,
    uint64_t ttl_seconds) const
{
    if (key.size() < offset + length) {
        return false; // Key too short, keep it
    }

    // Extract timestamp bytes (big-endian)
    uint64_t timestamp = 0;
    const char* data = key.data() + offset;
    for (size_t i = 0; i < length && i < 8; ++i) {
        timestamp = (timestamp << 8) | static_cast<uint8_t>(data[i]);
    }

    // Get current time
    auto now = std::chrono::system_clock::now();
    auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();

    // Check if expired
    return (timestamp + ttl_seconds) < static_cast<uint64_t>(now_seconds);
}

FilterResult ErlangCompactionFilter::CallErlangHandler(
    int level,
    const rocksdb::Slice& key,
    const rocksdb::Slice& value) const
{
    // Create temporary environment for this call
    ErlNifEnv* env = enif_alloc_env();
    if (env == nullptr) {
        return FilterResult(FilterDecision::Keep);
    }

    // Check if handler process is still alive
    if (!enif_is_process_alive(env, const_cast<ErlNifPid*>(&m_HandlerPid))) {
        m_HandlerDead = true;
        enif_free_env(env);
        return FilterResult(FilterDecision::Keep);
    }

    // Allocate batch resource for synchronization
    void* batch_alloc = enif_alloc_resource(m_CompactionBatch_RESOURCE, sizeof(CompactionBatchResource));
    if (batch_alloc == nullptr) {
        enif_free_env(env);
        return FilterResult(FilterDecision::Keep);
    }

    CompactionBatchResource* batch = new (batch_alloc) CompactionBatchResource();
    ERL_NIF_TERM batch_ref = enif_make_resource(env, batch_alloc);
    // NOTE: We keep the native reference until after WaitForResponse completes.
    // This prevents the resource from being GC'd while we're still using it.
    // We must call enif_release_resource on ALL exit paths after this point.

    // Build key binary
    ERL_NIF_TERM key_bin;
    unsigned char* key_data = enif_make_new_binary(env, key.size(), &key_bin);
    if (key_data == nullptr) {
        enif_release_resource(batch_alloc);
        enif_free_env(env);
        return FilterResult(FilterDecision::Keep);
    }
    memcpy(key_data, key.data(), key.size());

    // Build value binary
    ERL_NIF_TERM value_bin;
    unsigned char* value_data = enif_make_new_binary(env, value.size(), &value_bin);
    if (value_data == nullptr) {
        enif_release_resource(batch_alloc);
        enif_free_env(env);
        return FilterResult(FilterDecision::Keep);
    }
    memcpy(value_data, value.data(), value.size());

    // Build message: {compaction_filter, BatchRef, [{Level, Key, Value}]}
    // For simplicity, send single key per message (batching can be added later)
    ERL_NIF_TERM key_tuple = enif_make_tuple3(
        env,
        enif_make_int(env, level),
        key_bin,
        value_bin
    );
    ERL_NIF_TERM keys_list = enif_make_list1(env, key_tuple);

    ERL_NIF_TERM msg = enif_make_tuple3(
        env,
        ATOM_COMPACTION_FILTER,
        batch_ref,
        keys_list
    );

    // Send message to handler process
    if (!enif_send(nullptr, const_cast<ErlNifPid*>(&m_HandlerPid), env, msg)) {
        // Send failed - handler might be dead
        m_HandlerDead = true;
        enif_release_resource(batch_alloc);
        enif_free_env(env);
        return FilterResult(FilterDecision::Keep);
    }

    // Wait for response with timeout
    bool got_response = batch->WaitForResponse(m_TimeoutMs);

    FilterResult result;
    if (got_response) {
        result = batch->GetResult(0);
    } else {
        // Timeout - default to keeping the key (safe fallback)
        result = FilterResult(FilterDecision::Keep);
    }

    // Now safe to release the native reference
    enif_release_resource(batch_alloc);
    enif_free_env(env);
    return result;
}

//--------------------------------------------------------------------
// ErlangCompactionFilterFactory Implementation
//--------------------------------------------------------------------

ErlangCompactionFilterFactory::ErlangCompactionFilterFactory(
    std::vector<FilterRule> rules)
    : m_UseErlangCallback(false)
    , m_Rules(std::move(rules))
    , m_BatchSize(0)
    , m_TimeoutMs(0)
{
    memset(&m_HandlerPid, 0, sizeof(m_HandlerPid));
}

ErlangCompactionFilterFactory::ErlangCompactionFilterFactory(
    ErlNifPid handler_pid,
    unsigned int batch_size,
    unsigned int timeout_ms)
    : m_UseErlangCallback(true)
    , m_HandlerPid(handler_pid)
    , m_BatchSize(batch_size)
    , m_TimeoutMs(timeout_ms)
{
}

ErlangCompactionFilterFactory::~ErlangCompactionFilterFactory()
{
}

std::unique_ptr<rocksdb::CompactionFilter>
ErlangCompactionFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& /*context*/)
{
    if (m_UseErlangCallback) {
        return std::make_unique<ErlangCompactionFilter>(
            m_HandlerPid, m_BatchSize, m_TimeoutMs);
    } else {
        return std::make_unique<ErlangCompactionFilter>(m_Rules);
    }
}

const char* ErlangCompactionFilterFactory::Name() const
{
    return "ErlangCompactionFilterFactory";
}

//--------------------------------------------------------------------
// Factory functions
//--------------------------------------------------------------------

std::shared_ptr<ErlangCompactionFilterFactory>
CreateCompactionFilterFactory(std::vector<FilterRule> rules)
{
    return std::make_shared<ErlangCompactionFilterFactory>(std::move(rules));
}

std::shared_ptr<ErlangCompactionFilterFactory>
CreateCompactionFilterFactory(ErlNifPid handler_pid,
                             unsigned int batch_size,
                             unsigned int timeout_ms)
{
    return std::make_shared<ErlangCompactionFilterFactory>(
        handler_pid, batch_size, timeout_ms);
}

//--------------------------------------------------------------------
// CompactionFilterReply NIF function
//--------------------------------------------------------------------

ERL_NIF_TERM CompactionFilterReply(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    // argv[0] = batch resource reference
    // argv[1] = list of decisions: [keep | remove | {change_value, binary()}]

    CompactionBatchResource* batch = nullptr;
    if (!enif_get_resource(env, argv[0], m_CompactionBatch_RESOURCE, (void**)&batch)) {
        return enif_make_badarg(env);
    }

    // Parse the results list
    std::vector<FilterResult> results;
    ERL_NIF_TERM head, tail = argv[1];

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        FilterResult result;

        if (head == ATOM_KEEP) {
            result.decision = FilterDecision::Keep;
        } else if (head == ATOM_REMOVE) {
            result.decision = FilterDecision::Remove;
        } else {
            // Check for {change_value, binary()}
            int arity;
            const ERL_NIF_TERM* tuple;
            if (enif_get_tuple(env, head, &arity, &tuple) && arity == 2) {
                if (tuple[0] == ATOM_CHANGE_VALUE) {
                    ErlNifBinary bin;
                    if (enif_inspect_binary(env, tuple[1], &bin)) {
                        result.decision = FilterDecision::ChangeValue;
                        result.new_value = std::string((const char*)bin.data, bin.size);
                    } else {
                        // Invalid binary, default to keep
                        result.decision = FilterDecision::Keep;
                    }
                } else {
                    // Unknown tuple, default to keep
                    result.decision = FilterDecision::Keep;
                }
            } else {
                // Unknown format, default to keep
                result.decision = FilterDecision::Keep;
            }
        }

        results.push_back(result);
    }

    // Set the response and signal waiting thread
    batch->SetResponse(results);

    return ATOM_OK;
}

} // namespace erocksdb
