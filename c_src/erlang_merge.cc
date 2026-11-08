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


#include <iostream>
#include <memory>
#include <list>
#include <deque>
#include <string>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

#include <cstdint>

#include "erl_nif.h"
#include "atoms.h"
#include "util.h"
#include "erlang_merge.h"


bool on_error(ErlNifEnv* env) {
    enif_free_env(env);
    return false;
}


// op {add, Value}, {set, Pos, Value}, remove_last, clear
namespace erocksdb {

    ErlangMergeOperator::ErlangMergeOperator() {}

    bool ErlangMergeOperator::FullMergeV2(
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {


        ERL_NIF_TERM existing_term;
        ErlNifEnv* env = enif_alloc_env();
        int arity;
        const ERL_NIF_TERM* op;

        //clear the new value for writing
        merge_out->new_value.clear();

        // A bad operand fails the reads of this key only. With the default
        // scope a flush or compaction meeting it would make the DB read-only.
        merge_out->op_failure_scope = OpFailureScope::kMustMerge;

        if (merge_in.existing_value) {
            if(!enif_binary_to_term(
                        env, (const unsigned char *)merge_in.existing_value->data(),
                        merge_in.existing_value->size(), &existing_term, 0)) {
                return on_error(env);
            }

            if (enif_is_number(env, existing_term)) {
                ErlNifSInt64 old_val;
                if (!enif_get_int64(env, existing_term, &old_val))
                    return on_error(env);
                return mergeErlangInt(env, old_val, false, merge_in, merge_out);
            } else if (enif_is_list(env, existing_term)) {
                ERL_NIF_TERM head, tail;
                std::list<ERL_NIF_TERM> l;
                tail = existing_term;
                while(enif_get_list_cell(env, tail, &head, &tail)) {
                    l.push_back(std::move(head));
                }
                return mergeErlangList(env, l, false, merge_in, merge_out);
            } else if (enif_is_binary(env, existing_term)) {
                ErlNifBinary bin;
                if(!enif_inspect_binary(env, existing_term, &bin))
                    return on_error(env);
                std::string s = std::string((const char *)bin.data, bin.size);
                return mergeErlangBinary(env, s, false, merge_in, merge_out);
            }
        } else {
            ERL_NIF_TERM term;
            // take first element and check if we can continue with it
            auto first = merge_in.operand_list.front();

            if (!enif_binary_to_term(
                        env, (const unsigned char *)first.data(),
                        first.size(), &term, 0)) {
               return on_error(env);
            }

            if (!enif_get_tuple(env, term, &arity, &op) || arity != 2)
                return on_error(env);

            if (op[0] == ATOM_MERGE_INT_ADD) {
                ErlNifSInt64 val;
                if (!enif_get_int64(env, op[1], &val))
                    return on_error(env);
                return mergeErlangInt(env, val, true, merge_in, merge_out);
            } else if ((op[0] == ATOM_MERGE_LIST_APPEND)  && enif_is_list(env, op[1])) {
                std::list<ERL_NIF_TERM> l;
                return mergeErlangList(env, l, false, merge_in, merge_out);
            } else if ((op[0] == ATOM_MERGE_LIST_SUBSTRACT)  && enif_is_list(env, op[1])) {
                std::list<ERL_NIF_TERM> l;
                return mergeErlangList(env, l, true, merge_in, merge_out);
            } else if ((op[0] == ATOM_MERGE_BINARY_APPEND) && enif_is_binary(env, op[1])) {
                ErlNifBinary bin;
                if(!enif_inspect_binary(env, op[1], &bin))
                    return on_error(env);
                std::string s = std::string((const char *)bin.data, bin.size);
                return mergeErlangBinary(env, s, true, merge_in, merge_out);
            }
        }

        return on_error(env);
    }

    bool ErlangMergeOperator::mergeErlangInt(
                    ErlNifEnv* env,
                    ErlNifSInt64 new_val,
                    bool next,
                    const MergeOperationInput& merge_in,
                    MergeOperationOutput* merge_out) const {

        ErlNifSInt64 val;
        ERL_NIF_TERM new_term, term;
        ErlNifBinary bin;
        int arity;
        const ERL_NIF_TERM* op;

        auto it = merge_in.operand_list.begin();
        if (next)
            std::advance(it, 1);

        while (it != merge_in.operand_list.end()) {

            if (!enif_binary_to_term(
                        env, (const unsigned char *)it->data(),
                        it->size(), &term, 0)) {
                return on_error(env);
            }

            if (!enif_get_tuple(env, term, &arity, &op) || arity != 2)
                return on_error(env);

            if ((op[0] != ATOM_MERGE_INT_ADD)
                    || !enif_get_int64(env, op[1], &val))
                return on_error(env);

            // the sum must fit in a 64-bit integer
            if ((val > 0 && new_val > INT64_MAX - val)
                    || (val < 0 && new_val < INT64_MIN - val))
                return on_error(env);

            new_val += val;

            ++it;
        }

        new_term = enif_make_int64(env, new_val);
        if (!enif_term_to_binary(env, new_term, &bin))
                return on_error(env);

        merge_out->new_value.reserve(bin.size);
        merge_out->new_value.assign((const char*)bin.data, bin.size);
        enif_free_env(env);
        return true;
    }

    bool ErlangMergeOperator::mergeErlangList(
                    ErlNifEnv* env,
                    std::list<ERL_NIF_TERM> list_in,
                    bool next,
                    const MergeOperationInput& merge_in,
                    MergeOperationOutput* merge_out) const {

        ERL_NIF_TERM head, tail, new_term, term;
        ErlNifBinary bin;
        int arity;
        const ERL_NIF_TERM* op;
        unsigned int pos, len;

        auto it = merge_in.operand_list.begin();
        if (next)
            std::advance(it, 1);

        while (it != merge_in.operand_list.end()) {

            if (!enif_binary_to_term(
                        env, (const unsigned char *)it->data(),
                        it->size(), &term, 0)) {
                return on_error(env);
            }

            if (!enif_get_tuple(env, term, &arity, &op))
                return on_error(env);

            if (arity == 2) {
                if (op[0] == ATOM_MERGE_LIST_APPEND) {
                    if (!enif_is_list(env, op[1]))
                        return on_error(env);

                    tail = op[1];
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        list_in.push_back(std::move(head));
                    }
                } else if (op[0] == ATOM_MERGE_LIST_SUBSTRACT) {
                    if(!enif_get_list_length(env, op[1], &len))
                        return on_error(env);

                    if (len > 0) {
                        tail = op[1];
                        while(enif_get_list_cell(env, tail, &head, &tail)) {
                            for(auto list_it = list_in.begin(); list_it!=list_in.end();) {
                                if(enif_compare(*list_it, head) == 0) {
                                    list_in.erase(list_it);
                                    break;
                                } else {
                                    ++list_it;
                                }
                            }
                        }
                    }
                } else if (op[0] == ATOM_MERGE_LIST_DELETE) {
                    if(!enif_get_uint(env, op[1], &pos))
                        return on_error(env);

                    if (pos < list_in.size()) {
                        auto lit = list_in.begin();
                        std::advance(lit, pos);
                        list_in.erase(lit);
                    }
                } else {
                    return on_error(env);
                }
            } else if (arity == 3) {
                if (op[0] == ATOM_MERGE_LIST_SET) {
                    if(!enif_get_uint(env, op[1], &pos))
                        return on_error(env);

                    if (pos < list_in.size()) {
                        auto lit = list_in.begin();
                        std::advance(lit, pos);
                        *lit = std::move(op[2]);
                    }
                } else if (op[0] == ATOM_MERGE_LIST_DELETE) {

                    unsigned int start, end;
                    if(!enif_get_uint(env, op[1], &start)
                            || !enif_get_uint(env, op[2], &end))
                        return on_error(env);

                    if (start >= end)
                        return on_error(env);

                    if ((start < end) && (end < list_in.size())) {
                        auto lit = list_in.begin();
                        std::advance(lit, start);
                        for(unsigned int i=start; i <= end; i++) {
                            lit = list_in.erase(lit);
                        }
                    }
                } else if (op[0] == ATOM_MERGE_LIST_INSERT) {
                    if(!enif_get_uint(env, op[1], &pos)
                            || !enif_is_list(env, op[2]))
                        return on_error(env);

                    if (pos < list_in.size()) {
                        tail = op[2];
                        auto lit = list_in.begin();
                        std::advance(lit, pos);
                        while(enif_get_list_cell(env, tail, &head, &tail)) {
                            list_in.insert(lit, std::move(head));
                        }
                    }
                } else {
                    return on_error(env);
                }
            } else {
                return on_error(env);
            }
            ++it;
        }

        new_term = enif_make_list(env, 0);
        for(std::list<ERL_NIF_TERM>::reverse_iterator rq = list_in.rbegin();
                rq!=list_in.rend(); ++rq) {
            new_term = enif_make_list_cell(env, *rq, new_term);
        }

        if (!enif_term_to_binary(env, new_term, &bin))
            return on_error(env);

        merge_out->new_value.reserve(bin.size);
        merge_out->new_value.assign((const char*)bin.data, bin.size);
        enif_free_env(env);
        return true;
    }


    bool ErlangMergeOperator::mergeErlangBinary(
            ErlNifEnv* env,
            std::string s,
            bool next,
            const MergeOperationInput& merge_in,
            MergeOperationOutput* merge_out) const {


        ERL_NIF_TERM new_term, term;
        ErlNifBinary bin;
        int arity;
        const ERL_NIF_TERM* op;
        unsigned int pos, count, pos2;

        auto it = merge_in.operand_list.begin();
        if (next)
            std::advance(it, 1);


        while (it != merge_in.operand_list.end()) {

            if (!enif_binary_to_term(
                        env, (const unsigned char *)it->data(),
                        it->size(), &term, 0)) {
                return on_error(env);
            }

            if (!enif_get_tuple(env, term, &arity, &op))
                return on_error(env);


            if ((arity == 2) && (op[0] == ATOM_MERGE_BINARY_APPEND)) {
                if(!enif_inspect_binary(env, op[1], &bin))
                    return on_error(env);

                s.append((const char *)bin.data, bin.size);

            } else if ((arity == 3)  && (op[0] == ATOM_MERGE_BINARY_ERASE)) {
                if(!enif_get_uint(env, op[1], &pos)
                        || !enif_get_uint(env, op[2], &count))
                    return on_error(env);
                if (pos < s.size())
                    s.erase(pos, count);
            } else if ((arity == 3) && (op[0] == ATOM_MERGE_BINARY_INSERT)) {
                if(!enif_get_uint(env, op[1], &pos)
                        || !enif_inspect_binary(env, op[2], &bin))
                    return on_error(env);

                if (pos <= s.size()) {
                    std::string chunk = std::string((const char*)bin.data, bin.size);
                    s.insert(pos, chunk);
                }
            } else if ((arity == 4) && (op[0] == ATOM_MERGE_BINARY_REPLACE)) {
                if(!enif_get_uint(env, op[1], &pos)
                        || !enif_get_uint(env, op[2], &count)
                        || !enif_inspect_binary(env, op[3], &bin)) {
                    return on_error(env);
                }
                pos2 = pos + count;
                if (pos2 < s.size())
                    s.replace(pos, count,(const char *)bin.data, bin.size);
            } else {
                return on_error(env);
            }
            ++it;
        }

        memcpy(enif_make_new_binary(env, s.size(), &new_term), s.data(), s.size());
        if (!enif_term_to_binary(env, new_term, &bin))
            return on_error(env);

        merge_out->new_value.reserve(bin.size);
        merge_out->new_value.assign((const char*)bin.data, bin.size);
        enif_free_env(env);
        return true;

    }

    bool ErlangMergeOperator::PartialMergeMulti(
            const rocksdb::Slice& /*key*/,
            const std::deque<rocksdb::Slice>& /*operand_list*/,
            std::string* /*new_value*/,
            rocksdb::Logger* /*logger*/) const {
        return false;
    }


    const char* ErlangMergeOperator::Name() const  {
        return "ErlangMergeOperator";
    }

    std::shared_ptr<ErlangMergeOperator> CreateErlangMergeOperator() {
        return std::make_shared<ErlangMergeOperator>();
    }


}



