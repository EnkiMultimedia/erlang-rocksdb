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
#ifndef INCL_COMPACTION_FILTER_H
#define INCL_COMPACTION_FILTER_H

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <unordered_set>
#include <unordered_map>

#include "rocksdb/compaction_filter.h"
#include "erl_nif.h"

namespace erocksdb {

//--------------------------------------------------------------------
// FilterDecision: Result from filter evaluation
//--------------------------------------------------------------------
enum class FilterDecision {
    Keep,           // Keep the key-value pair
    Remove,         // Delete the key
    ChangeValue     // Modify the value
};

struct FilterResult {
    FilterDecision decision;
    std::string new_value;  // Only used when decision == ChangeValue

    FilterResult() : decision(FilterDecision::Keep) {}
    FilterResult(FilterDecision d) : decision(d) {}
    FilterResult(FilterDecision d, std::string nv) : decision(d), new_value(std::move(nv)) {}
};

//--------------------------------------------------------------------
// Rule types for declarative mode
//--------------------------------------------------------------------
enum class RuleType {
    KeyPrefix,           // Delete if key has this prefix
    KeySuffix,           // Delete if key has this suffix
    KeyContains,         // Delete if key contains pattern
    ValueEmpty,          // Delete if value is empty
    ValuePrefix,         // Delete if value has prefix
    TTLFromKey,          // TTL check from key bytes (big-endian timestamp)
    Always               // Always delete
};

struct FilterRule {
    RuleType type;
    std::string pattern;
    size_t offset;        // For TTL: byte offset in key
    size_t length;        // For TTL: number of bytes for timestamp
    uint64_t ttl_seconds; // For TTL rules

    FilterRule() : type(RuleType::Always), offset(0), length(0), ttl_seconds(0) {}
};

//--------------------------------------------------------------------
// CompactionBatchResource: Manages synchronization for Erlang callbacks
//--------------------------------------------------------------------
class CompactionBatchResource {
public:
    CompactionBatchResource();
    ~CompactionBatchResource();

    // Wait for Erlang response with timeout
    // Returns true if response received, false on timeout
    bool WaitForResponse(unsigned int timeout_ms);

    // Called from NIF when Erlang sends response
    void SetResponse(const std::vector<FilterResult>& results);

    // Get result for a specific index
    FilterResult GetResult(size_t index) const;

    // Check if we have a response
    bool HasResponse() const;

    // Reset for reuse
    void Reset();

private:
    mutable std::mutex m_Mutex;
    std::condition_variable m_Cond;
    bool m_HasResponse;  // Protected by m_Mutex
    std::vector<FilterResult> m_Results;
};

//--------------------------------------------------------------------
// ErlangCompactionFilter: Main filter implementation
//--------------------------------------------------------------------
class ErlangCompactionFilter : public rocksdb::CompactionFilter {
public:
    // Constructor for declarative mode
    explicit ErlangCompactionFilter(std::vector<FilterRule> rules);

    // Constructor for Erlang callback mode
    ErlangCompactionFilter(ErlNifPid handler_pid,
                          unsigned int batch_size,
                          unsigned int timeout_ms);

    virtual ~ErlangCompactionFilter();

    // rocksdb::CompactionFilter interface - using FilterV2 for direct Decision control
    virtual Decision FilterV2(int level,
                             const rocksdb::Slice& key,
                             ValueType value_type,
                             const rocksdb::Slice& existing_value,
                             std::string* new_value,
                             std::string* skip_until) const override;

    virtual const char* Name() const override;

    // For debugging
    size_t GetRulesCount() const { return m_Rules.size(); }

private:
    // Mode indicator
    bool m_UseErlangCallback;

    // For declarative mode
    std::vector<FilterRule> m_Rules;

    // For Erlang callback mode
    ErlNifPid m_HandlerPid;
    [[maybe_unused]] unsigned int m_BatchSize;  // Reserved for future batching implementation
    unsigned int m_TimeoutMs;
    mutable bool m_HandlerDead;  // Simple flag, one-way transition to true

    // Helper methods
    FilterResult ApplyRules(const rocksdb::Slice& key,
                           const rocksdb::Slice& value) const;

    FilterResult CallErlangHandler(int level,
                                  const rocksdb::Slice& key,
                                  const rocksdb::Slice& value) const;

    bool CheckTTL(const rocksdb::Slice& key,
                 size_t offset,
                 size_t length,
                 uint64_t ttl_seconds) const;
};

//--------------------------------------------------------------------
// ErlangCompactionFilterFactory: Creates per-compaction filter instances
//--------------------------------------------------------------------
class ErlangCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    // Constructor for declarative mode
    explicit ErlangCompactionFilterFactory(std::vector<FilterRule> rules);

    // Constructor for Erlang callback mode
    ErlangCompactionFilterFactory(ErlNifPid handler_pid,
                                 unsigned int batch_size,
                                 unsigned int timeout_ms);

    virtual ~ErlangCompactionFilterFactory();

    virtual std::unique_ptr<rocksdb::CompactionFilter>
        CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override;

    virtual const char* Name() const override;

private:
    bool m_UseErlangCallback;
    std::vector<FilterRule> m_Rules;
    ErlNifPid m_HandlerPid;
    unsigned int m_BatchSize;
    unsigned int m_TimeoutMs;
};

//--------------------------------------------------------------------
// Factory functions
//--------------------------------------------------------------------
std::shared_ptr<ErlangCompactionFilterFactory>
CreateCompactionFilterFactory(std::vector<FilterRule> rules);

std::shared_ptr<ErlangCompactionFilterFactory>
CreateCompactionFilterFactory(ErlNifPid handler_pid,
                             unsigned int batch_size,
                             unsigned int timeout_ms);

//--------------------------------------------------------------------
// NIF resource type for batch tracking
//--------------------------------------------------------------------
extern ErlNifResourceType* m_CompactionBatch_RESOURCE;

void CreateCompactionBatchResourceType(ErlNifEnv* env);

// Helper to create binary from slice
ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, const rocksdb::Slice& slice);

} // namespace erocksdb

#endif // INCL_COMPACTION_FILTER_H
