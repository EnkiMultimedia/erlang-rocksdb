// Copyright (c) 2018 Benoit Chesneau
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
    };

   bool BitsetMergeOperator::FullMergeV2(
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {

    size_t size;
    char* data = nullptr;
    std::string s;
    int pos;

    size_t max_size = cap_ / bits_per_char;

    if (!merge_in.existing_value) {
        size = max_size;
        merge_out->new_value.reserve(size);
        data = new cell_type[size];
        std::fill_n(data, size, 0x00);
    } else {
        size = merge_in.existing_value->size();
        if (size < max_size)
            return false;
        data = const_cast<char*>(merge_in.existing_value->data());
    }

    auto it = merge_in.operand_list.begin();
    while (it != merge_in.operand_list.end()) {
        s = it->ToString();
        if (s.empty()) {
            std::fill_n(data, size, 0x00);
            ++it;
            continue;
        }

        try {
            pos = std::stoi(s.substr(1));
        } catch(...) {
            delete[] data;
            return false;
        }

        size_t ofs = pos >> 3;

        if (ofs > size) {
            delete[] data;
            return false;
        }

        //char bytemask = (1 << ((1 << 3)) - (pos & ((1 << 3)))));
        if (s[0] == '+') {
            //data[(pos >> 3)] |= bytemask;
            data[ofs] |= bit_mask[pos % bits_per_char];
        } else if (s[0] == '-') {
            //data[(pos >> 3)] &= ~bytemask;
            data[ofs] &= ~bit_mask[pos % bits_per_char];
        } else {
            delete[] data;
            return false;
        }

        ++it;
    }

    //clear the new value for writing
    merge_out->new_value.clear();
    merge_out->new_value.append(reinterpret_cast<char*>(data), size);
    return true;

   }

   bool BitsetMergeOperator::PartialMergeMulti(
            const rocksdb::Slice& key,
            const std::deque<rocksdb::Slice>& operand_list,
            std::string* new_value,
            rocksdb::Logger* logger) const {
        return false;
    }

    const char* BitsetMergeOperator::Name() const  {
        return "BitsetMergeOperator";
    }

    std::shared_ptr<BitsetMergeOperator> CreateBitsetMergeOperator(unsigned int cap) {
        return std::make_shared<BitsetMergeOperator>(cap);
    }
}
