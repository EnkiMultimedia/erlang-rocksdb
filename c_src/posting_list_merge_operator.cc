// Copyright (c) 2018-2026 Benoit Chesneau
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


#include <memory>
#include <deque>
#include <string>
#include <cstdint>
#include <unordered_map>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

#include "erl_nif.h"
#include "atoms.h"
#include "posting_list_merge_operator.h"


namespace erocksdb {

    PostingListMergeOperator::PostingListMergeOperator() {}

    // Parse an existing posting list binary into a map of key -> is_tombstone
    void PostingListMergeOperator::ParseExistingValue(
            const rocksdb::Slice& value,
            std::unordered_map<std::string, bool>& key_states) const {
        const char* ptr = value.data();
        const char* end = ptr + value.size();

        while (ptr + 5 <= end) {  // 4 bytes length + 1 byte flag minimum
            uint32_t len = (static_cast<uint8_t>(ptr[0]) << 24) |
                           (static_cast<uint8_t>(ptr[1]) << 16) |
                           (static_cast<uint8_t>(ptr[2]) << 8) |
                           static_cast<uint8_t>(ptr[3]);
            ptr += 4;

            uint8_t flag = static_cast<uint8_t>(*ptr);
            ptr += 1;

            bool is_tombstone = (flag != 0);

            if (ptr + len > end) break;

            std::string key(ptr, len);
            ptr += len;

            key_states[key] = is_tombstone;  // Last occurrence wins
        }
    }

    bool PostingListMergeOperator::FullMergeV2(
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {

        // Build a map of key -> is_tombstone (last occurrence wins)
        std::unordered_map<std::string, bool> key_states;

        // Parse existing value (if any) - it should be a posting list binary
        if (merge_in.existing_value != nullptr && !merge_in.existing_value->empty()) {
            ParseExistingValue(*merge_in.existing_value, key_states);
        }

        // Process each operand - they are in Erlang external term format
        for (const auto& operand : merge_in.operand_list) {
            std::string key;
            bool is_tombstone;

            // Try to parse as Erlang term format first
            if (ParseOperand(operand, key, is_tombstone)) {
                key_states[key] = is_tombstone;  // Last occurrence wins
            } else {
                // If parsing fails, the operand might be a raw posting list binary
                // (from PartialMergeMulti output or direct storage)
                // Parse it as a posting list and merge into key_states
                ParseExistingValue(operand, key_states);
            }
        }

        // Rebuild the value with only active keys (not tombstoned)
        merge_out->new_value.clear();
        for (const auto& [key, is_tombstone] : key_states) {
            if (!is_tombstone) {
                AppendEntry(&merge_out->new_value, key, false);
            }
        }

        return true;
    }

    bool PostingListMergeOperator::ParseOperand(
            const rocksdb::Slice& operand,
            std::string& key,
            bool& is_tombstone) const {

        // Operand format: Erlang term {posting_add, Binary} or {posting_delete, Binary}
        // Encoded using enif_term_to_binary on Erlang side

        ErlNifEnv* env = enif_alloc_env();
        if (!env) return false;

        ERL_NIF_TERM term;
        if (enif_binary_to_term(env, (unsigned char*)operand.data(),
                                operand.size(), &term, 0) == 0) {
            enif_free_env(env);
            return false;
        }

        int arity;
        const ERL_NIF_TERM* tuple;
        if (!enif_get_tuple(env, term, &arity, &tuple) || arity != 2) {
            enif_free_env(env);
            return false;
        }

        if (tuple[0] == ATOM_POSTING_ADD) {
            is_tombstone = false;
        } else if (tuple[0] == ATOM_POSTING_DELETE) {
            is_tombstone = true;
        } else {
            enif_free_env(env);
            return false;
        }

        ErlNifBinary bin;
        if (!enif_inspect_binary(env, tuple[1], &bin)) {
            enif_free_env(env);
            return false;
        }

        key.assign((char*)bin.data, bin.size);
        enif_free_env(env);
        return true;
    }

    void PostingListMergeOperator::AppendEntry(
            std::string* result,
            const std::string& key,
            bool is_tombstone) const {

        // Format: <Len:32/big><Flag:8><Key:Len>
        uint32_t len = static_cast<uint32_t>(key.size());

        // Append length (big-endian)
        result->push_back((len >> 24) & 0xFF);
        result->push_back((len >> 16) & 0xFF);
        result->push_back((len >> 8) & 0xFF);
        result->push_back(len & 0xFF);

        // Append flag byte
        result->push_back(is_tombstone ? 1 : 0);

        // Append key data
        result->append(key);
    }

    bool PostingListMergeOperator::PartialMergeMulti(
            const rocksdb::Slice& /*key*/,
            const std::deque<rocksdb::Slice>& operand_list,
            std::string* new_value,
            rocksdb::Logger* /*logger*/) const {
        // Combine multiple operands into a single consolidated posting list
        // This enables tombstone cleanup during compaction when there's no base value

        if (operand_list.size() < 2) {
            return false;  // Need at least 2 operands to merge
        }

        std::unordered_map<std::string, bool> key_states;

        // Process each operand - can be Erlang term format or raw posting list
        for (const auto& operand : operand_list) {
            std::string key;
            bool is_tombstone;

            // Try to parse as Erlang term format first
            if (ParseOperand(operand, key, is_tombstone)) {
                key_states[key] = is_tombstone;  // Last occurrence wins
            } else {
                // If parsing fails, the operand might be a raw posting list binary
                // (from a previous PartialMergeMulti output)
                ParseExistingValue(operand, key_states);
            }
        }

        // Build the consolidated posting list with only active keys
        new_value->clear();
        for (const auto& [key, is_tombstone] : key_states) {
            if (!is_tombstone) {
                AppendEntry(new_value, key, false);
            }
        }

        return true;
    }

    const char* PostingListMergeOperator::Name() const {
        return "PostingListMergeOperator";
    }

    std::shared_ptr<PostingListMergeOperator> CreatePostingListMergeOperator() {
        return std::make_shared<PostingListMergeOperator>();
    }

}
