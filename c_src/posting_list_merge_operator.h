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
#include <unordered_map>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

// forward declaration
namespace rocksdb {
    class MergeOperator;
    class Slice;
    class Logger;
}

namespace erocksdb {

    /**
     * PostingListMergeOperator - A merge operator for managing posting lists.
     *
     * A posting list is a binary value containing a sequence of entries:
     *   <KeyLength:32/big><Flag:8><KeyData:KeyLength/binary>...
     *
     * Where:
     *   - KeyLength: 4-byte big-endian length of the key data
     *   - Flag: 1 byte, 0 = normal entry, non-zero = tombstone
     *   - KeyData: the actual key bytes
     *
     * Merge operations:
     *   - {posting_add, Binary} - Append a key to the posting list
     *   - {posting_delete, Binary} - Append a tombstone for the key
     *
     * Tombstones are automatically cleaned up:
     *   - During reads (FullMergeV2 is called when reading a key with operands)
     *   - During compaction (PartialMergeMulti consolidates operands)
     *
     * The returned posting list only contains active (non-tombstoned) keys.
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
            // Parse {posting_add, Binary} or {posting_delete, Binary} from Erlang term
            bool ParseOperand(const rocksdb::Slice& operand,
                              std::string& key,
                              bool& is_tombstone) const;

            // Append an entry to the result buffer
            void AppendEntry(std::string* result,
                             const std::string& key,
                             bool is_tombstone) const;

            // Parse an existing posting list binary into a map of key -> is_tombstone
            void ParseExistingValue(const rocksdb::Slice& value,
                                    std::unordered_map<std::string, bool>& key_states) const;
    };

    std::shared_ptr<PostingListMergeOperator> CreatePostingListMergeOperator();

}
