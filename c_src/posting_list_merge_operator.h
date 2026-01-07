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


#pragma once

#include <deque>
#include <string>
#include <memory>
#include <map>
#include <unordered_map>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

#ifdef WITH_CROARING
#include <roaring/roaring64.h>
#endif

// forward declaration
namespace rocksdb {
    class MergeOperator;
    class Slice;
    class Logger;
}

namespace erocksdb {

    // Format version constants
    constexpr uint8_t POSTING_LIST_V1 = 0x01;
    constexpr uint8_t POSTING_LIST_V2 = 0x02;

    /**
     * PostingListMergeOperator - A merge operator for managing posting lists.
     *
     * Format V1 (legacy, unordered):
     *   <KeyLength:32/big><Flag:8><KeyData:KeyLength/binary>...
     *
     * Format V2 (new, sorted with roaring bitmap):
     *   <Version:8><BitmapSize:32/big><BitmapData:BitmapSize><KeyCount:32/big>
     *   <SortedKeys: <Len:32/big><Key:Len>...>
     *
     * Where:
     *   - Version: 0x02 for V2 format
     *   - BitmapSize: 4-byte big-endian size of serialized roaring64 bitmap
     *   - BitmapData: serialized roaring64 bitmap for fast lookups
     *   - KeyCount: 4-byte big-endian count of keys
     *   - SortedKeys: lexicographically sorted keys (no flag byte, tombstones filtered)
     *
     * Merge operations:
     *   - {posting_add, Binary} - Add a key to the posting list
     *   - {posting_delete, Binary} - Remove a key (tombstone until compaction)
     *
     * Tombstones are automatically cleaned up during merge operations.
     * V1 format is automatically upgraded to V2 on merge.
     */
    class PostingListMergeOperator : public rocksdb::MergeOperator {
        public:
            explicit PostingListMergeOperator();

            virtual bool FullMergeV2(
                    const MergeOperationInput& merge_in,
                    MergeOperationOutput* merge_out) const override;

            virtual bool PartialMergeMulti(
                    const rocksdb::Slice& key,
                    const std::deque<rocksdb::Slice>& operand_list,
                    std::string* new_value,
                    rocksdb::Logger* logger) const override;

            virtual const char* Name() const override;

        private:
            // Detect format version from data
            uint8_t DetectVersion(const rocksdb::Slice& data) const;

            // Hash a key to 64-bit value for roaring bitmap
            uint64_t HashKey(const std::string& key) const;

            // Parse {posting_add, Binary} or {posting_delete, Binary} from Erlang term
            bool ParseOperand(const rocksdb::Slice& operand,
                              std::string& key,
                              bool& is_tombstone) const;

            // Parse V1 posting list binary into a map of key -> is_tombstone
            void ParseV1Value(const rocksdb::Slice& value,
                              std::map<std::string, bool>& key_states) const;

            // Parse V2 posting list binary into a map of key -> is_tombstone
            void ParseV2Value(const rocksdb::Slice& value,
                              std::map<std::string, bool>& key_states) const;

            // Parse any format (auto-detect version)
            void ParseExistingValue(const rocksdb::Slice& value,
                                    std::map<std::string, bool>& key_states) const;

            // Serialize to V2 format
            void SerializeV2(const std::map<std::string, bool>& key_states,
                            std::string* output) const;

            // Legacy: Append an entry in V1 format (for debugging/compatibility)
            void AppendEntryV1(std::string* result,
                               const std::string& key,
                               bool is_tombstone) const;
    };

    std::shared_ptr<PostingListMergeOperator> CreatePostingListMergeOperator();

}
