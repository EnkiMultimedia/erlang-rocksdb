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
#include <cstring>
#include <map>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

#include "erl_nif.h"
#include "atoms.h"
#include "posting_list_merge_operator.h"


namespace erocksdb {

    PostingListMergeOperator::PostingListMergeOperator() {}

    // MurmurHash3 64-bit finalizer for mixing
    static inline uint64_t fmix64(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }

    uint64_t PostingListMergeOperator::HashKey(const std::string& key) const {
        uint64_t h = 0x9e3779b97f4a7c15ULL;  // seed
        const char* data = key.data();
        size_t len = key.size();

        // Process 8-byte blocks
        size_t nblocks = len / 8;
        for (size_t i = 0; i < nblocks; i++) {
            uint64_t k;
            memcpy(&k, data + i * 8, sizeof(k));
            h ^= fmix64(k);
            h = (h << 27) | (h >> 37);
            h = h * 5 + 0x52dce729;
        }

        // Handle remaining bytes
        const uint8_t* tail = (const uint8_t*)(data + nblocks * 8);
        uint64_t k = 0;
        switch (len & 7) {
            case 7: k ^= ((uint64_t)tail[6]) << 48; [[fallthrough]];
            case 6: k ^= ((uint64_t)tail[5]) << 40; [[fallthrough]];
            case 5: k ^= ((uint64_t)tail[4]) << 32; [[fallthrough]];
            case 4: k ^= ((uint64_t)tail[3]) << 24; [[fallthrough]];
            case 3: k ^= ((uint64_t)tail[2]) << 16; [[fallthrough]];
            case 2: k ^= ((uint64_t)tail[1]) << 8;  [[fallthrough]];
            case 1: k ^= ((uint64_t)tail[0]);
                    h ^= fmix64(k);
        }

        return fmix64(h ^ len);
    }

    uint8_t PostingListMergeOperator::DetectVersion(const rocksdb::Slice& data) const {
        if (data.empty()) {
            return POSTING_LIST_V1;
        }

        uint8_t first_byte = static_cast<uint8_t>(data.data()[0]);

        // V2 format starts with version byte 0x02
        if (first_byte == POSTING_LIST_V2) {
            return POSTING_LIST_V2;
        }

        // V1 format: first byte is MSB of 32-bit length
        // Lengths <= 16MB have MSB of 0x00, so version detection works
        // If first byte is 0x02, it would mean a key length of 33+ million bytes
        // which is unrealistic, so we can safely use 0x02 as version marker
        return POSTING_LIST_V1;
    }

    // Parse V1 posting list binary into a map of key -> is_tombstone
    void PostingListMergeOperator::ParseV1Value(
            const rocksdb::Slice& value,
            std::map<std::string, bool>& key_states) const {
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

    // Parse V2 posting list binary into a map of key -> is_tombstone (all active)
    void PostingListMergeOperator::ParseV2Value(
            const rocksdb::Slice& value,
            std::map<std::string, bool>& key_states) const {
        const char* ptr = value.data();
        const char* end = ptr + value.size();

        // Skip version byte
        if (ptr >= end) return;
        ptr += 1;

        // Read bitmap size
        if (ptr + 4 > end) return;
        uint32_t bitmap_size = (static_cast<uint8_t>(ptr[0]) << 24) |
                               (static_cast<uint8_t>(ptr[1]) << 16) |
                               (static_cast<uint8_t>(ptr[2]) << 8) |
                               static_cast<uint8_t>(ptr[3]);
        ptr += 4;

        // Skip bitmap data (we don't need it for parsing keys)
        if (ptr + bitmap_size > end) return;
        ptr += bitmap_size;

        // Read key count
        if (ptr + 4 > end) return;
        uint32_t key_count = (static_cast<uint8_t>(ptr[0]) << 24) |
                             (static_cast<uint8_t>(ptr[1]) << 16) |
                             (static_cast<uint8_t>(ptr[2]) << 8) |
                             static_cast<uint8_t>(ptr[3]);
        ptr += 4;

        // Read sorted keys (all active, no tombstones in V2)
        for (uint32_t i = 0; i < key_count && ptr + 4 <= end; i++) {
            uint32_t len = (static_cast<uint8_t>(ptr[0]) << 24) |
                           (static_cast<uint8_t>(ptr[1]) << 16) |
                           (static_cast<uint8_t>(ptr[2]) << 8) |
                           static_cast<uint8_t>(ptr[3]);
            ptr += 4;

            if (ptr + len > end) break;

            std::string key(ptr, len);
            ptr += len;

            key_states[key] = false;  // All keys in V2 are active
        }
    }

    void PostingListMergeOperator::ParseExistingValue(
            const rocksdb::Slice& value,
            std::map<std::string, bool>& key_states) const {
        uint8_t version = DetectVersion(value);
        if (version == POSTING_LIST_V2) {
            ParseV2Value(value, key_states);
        } else {
            ParseV1Value(value, key_states);
        }
    }

    void PostingListMergeOperator::SerializeV2(
            const std::map<std::string, bool>& key_states,
            std::string* output) const {

        // Collect active keys (std::map is already sorted)
        std::vector<std::string> active_keys;
        for (const auto& [key, is_tombstone] : key_states) {
            if (!is_tombstone) {
                active_keys.push_back(key);
            }
        }

        output->clear();

#ifdef WITH_CROARING
        // Build roaring bitmap
        roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
        for (const auto& key : active_keys) {
            uint64_t h = HashKey(key);
            roaring64_bitmap_add(bitmap, h);
        }

        // Serialize bitmap
        size_t bitmap_size = roaring64_bitmap_portable_size_in_bytes(bitmap);
        std::vector<char> bitmap_data(bitmap_size);
        roaring64_bitmap_portable_serialize(bitmap, bitmap_data.data());
        roaring64_bitmap_free(bitmap);

        // Version byte
        output->push_back(POSTING_LIST_V2);

        // Bitmap size (big-endian)
        uint32_t bs = static_cast<uint32_t>(bitmap_size);
        output->push_back((bs >> 24) & 0xFF);
        output->push_back((bs >> 16) & 0xFF);
        output->push_back((bs >> 8) & 0xFF);
        output->push_back(bs & 0xFF);

        // Bitmap data
        output->append(bitmap_data.data(), bitmap_size);
#else
        // Without CRoaring, write empty bitmap
        output->push_back(POSTING_LIST_V2);
        output->push_back(0);
        output->push_back(0);
        output->push_back(0);
        output->push_back(0);
#endif

        // Key count (big-endian)
        uint32_t kc = static_cast<uint32_t>(active_keys.size());
        output->push_back((kc >> 24) & 0xFF);
        output->push_back((kc >> 16) & 0xFF);
        output->push_back((kc >> 8) & 0xFF);
        output->push_back(kc & 0xFF);

        // Sorted keys (already sorted by std::map)
        for (const auto& key : active_keys) {
            uint32_t len = static_cast<uint32_t>(key.size());
            output->push_back((len >> 24) & 0xFF);
            output->push_back((len >> 16) & 0xFF);
            output->push_back((len >> 8) & 0xFF);
            output->push_back(len & 0xFF);
            output->append(key);
        }
    }

    void PostingListMergeOperator::AppendEntryV1(
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

    bool PostingListMergeOperator::FullMergeV2(
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {

        // Use std::map for automatic lexicographic sorting
        std::map<std::string, bool> key_states;

        // Parse existing value (if any) - handles both V1 and V2 formats
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
                ParseExistingValue(operand, key_states);
            }
        }

        // Serialize to V2 format (sorted, with bitmap)
        SerializeV2(key_states, &merge_out->new_value);

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

        if (enif_is_identical(tuple[0], ATOM_POSTING_ADD)) {
            is_tombstone = false;
        } else if (enif_is_identical(tuple[0], ATOM_POSTING_DELETE)) {
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

        std::map<std::string, bool> key_states;

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

        // Serialize to V2 format
        SerializeV2(key_states, new_value);

        return true;
    }

    const char* PostingListMergeOperator::Name() const {
        return "PostingListMergeOperator";
    }

    std::shared_ptr<PostingListMergeOperator> CreatePostingListMergeOperator() {
        return std::make_shared<PostingListMergeOperator>();
    }

}
