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

#include <string>
#include <memory>
#include <list>
#include <algorithm>
#include <cstdint>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

#include "erl_nif.h"
#include "util.h"
#include "bitset_merge_operator.h"

static const std::size_t bits_per_char = 0x08;    // 8 bits in 1 char(unsigned)
static const unsigned char bit_mask[bits_per_char] = {
                                                       0x80,  //10000000
                                                       0x40,  //01000000
                                                       0x20,  //00100000
                                                       0x10,  //00010000
                                                       0x08,  //00001000
                                                       0x04,  //00000100
                                                       0x02,  //00000010
                                                       0x01   //00000001
                                                     };

namespace erocksdb {

    BitsetMergeOperator::BitsetMergeOperator(unsigned int cap)
        :cap_(cap) {
    }

   bool BitsetMergeOperator::FullMergeV2(
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {

    const size_t max_size = cap_ / bits_per_char;

    // A bad operand fails the reads of this key only. With the default scope
    // a flush or compaction meeting it would make the DB read-only.
    merge_out->op_failure_scope = OpFailureScope::kMustMerge;

    // Work on our own copy: existing_value points into RocksDB memory
    // (memtable, block cache) that other readers see and must not change.
    std::string& data = merge_out->new_value;
    if (!merge_in.existing_value) {
        data.assign(max_size, '\0');
    } else {
        if (merge_in.existing_value->size() < max_size)
            return false;
        data.assign(merge_in.existing_value->data(),
                    merge_in.existing_value->size());
    }
    const size_t size = data.size();

    for (const rocksdb::Slice& operand : merge_in.operand_list) {
        if (operand.empty()) {
            std::fill(data.begin(), data.end(), '\0');
            continue;
        }

        const char op = operand[0];
        if (op != '+' && op != '-')
            return false;

        // bit position: decimal digits only
        if (operand.size() < 2 || operand.size() > 21)
            return false;
        uint64_t pos = 0;
        for (size_t i = 1; i < operand.size(); i++) {
            const char c = operand[i];
            if (c < '0' || c > '9')
                return false;
            pos = pos * 10 + static_cast<uint64_t>(c - '0');
            if (pos >= static_cast<uint64_t>(size) * bits_per_char)
                return false;
        }

        const size_t ofs = static_cast<size_t>(pos / bits_per_char);
        const unsigned char mask = bit_mask[pos % bits_per_char];
        if (op == '+')
            data[ofs] = static_cast<char>(static_cast<unsigned char>(data[ofs]) | mask);
        else
            data[ofs] = static_cast<char>(static_cast<unsigned char>(data[ofs]) & ~mask);
    }

    return true;
   }

   bool BitsetMergeOperator::PartialMergeMulti(
            const rocksdb::Slice& /*key*/,
            const std::deque<rocksdb::Slice>& /*operand_list*/,
            std::string* /*new_value*/,
            rocksdb::Logger* /*logger*/) const {
        return false;
    }

    const char* BitsetMergeOperator::Name() const  {
        return "BitsetMergeOperator";
    }

    std::shared_ptr<BitsetMergeOperator> CreateBitsetMergeOperator(unsigned int cap) {
        return std::make_shared<BitsetMergeOperator>(cap);
    }
}
