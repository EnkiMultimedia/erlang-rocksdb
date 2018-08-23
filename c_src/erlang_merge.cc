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


#include <iostream>
#include <memory>
#include <list>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

#include "erl_nif.h"
#include "atoms.h"
#include "util.h"
#include "erlang_merge.h"


// op {add, Value}, {set, Pos, Value}, remove_last, clear
namespace erocksdb {

    ErlangMergeOperator::ErlangMergeOperator() {};

    bool ErlangMergeOperator::Merge(
            const rocksdb::Slice& key,
            const rocksdb::Slice* existing_value,
            const rocksdb::Slice& value,
            std::string* new_value,
            rocksdb::Logger* logger) const {

        ERL_NIF_TERM existing_term;
        ERL_NIF_TERM term;
        ErlNifEnv* env = enif_alloc_env();
        int arity;
        const ERL_NIF_TERM* op;

        bool should_encode = true;

        //clear the new value for writing
        assert(new_value);
        new_value->clear();

        if (!enif_binary_to_term(env, (const unsigned char *)value.data(), value.size(), &term, 0)) {
            enif_free_env(env);
            return false;
        }


        ERL_NIF_TERM new_term = 0;
        ErlNifBinary bin, bin_term;


        if(enif_get_tuple(env, term, &arity, &op)) {

            if ((existing_value != nullptr) &&
                    !enif_binary_to_term(env, (const unsigned char *)existing_value->data(), existing_value->size(), &existing_term, 0))
            {
                if((op[0] != ATOM_MERGE_BINARY_APPEND)
                        && (op[0] != ATOM_MERGE_BINARY_REPLACE)
                        && (op[0] != ATOM_MERGE_BINARY_INSERT)
                        && (op[0] != ATOM_MERGE_BINARY_ERASE)) {
                    enif_free_env(env);
                    return false;
                }

                should_encode = false;
            }

            if (arity == 2) {
                if (op[0] == ATOM_MERGE_INT_ADD) {
                    ErlNifSInt64 old_val;
                    ErlNifSInt64 val;

                    if (existing_value == nullptr) {
                        old_val = 0;
                    } else if (!enif_get_int64(env, existing_term, &old_val)) {
                        enif_free_env(env);
                        return false;
                    }


                    if (!enif_get_int64(env, op[1], &val)) {
                        enif_free_env(env);
                        return false;
                    }

                    new_term = enif_make_int64(env, old_val + val);
                }

                else if (op[0] == ATOM_MERGE_LIST_APPEND) {
                    ERL_NIF_TERM head, tail;
                    std::list<ERL_NIF_TERM> q;
                    new_term = enif_make_list(env, 0);
                    if (existing_value) {
                        if(!enif_is_list(env, existing_term)) {
                            enif_free_env(env);
                            return false;
                        } else {
                            tail  = existing_term;
                            while(enif_get_list_cell(env, tail, &head, &tail)) {
                                q.push_back(std::move(head));
                            }
                        }
                    }
                    if (!enif_is_list(env, op[1])) {
                        enif_free_env(env);
                        return false;
                    }

                    tail = op[1];
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        q.push_back(std::move(head));
                    }

                    for(std::list<ERL_NIF_TERM>::reverse_iterator rq = q.rbegin();  rq!=q.rend(); ++rq) {
                        new_term = enif_make_list_cell(env, *rq, new_term);
                    }
                }

                else if(op[0] == ATOM_MERGE_LIST_SUBSTRACT) {
                    ERL_NIF_TERM head, tail;
                    new_term = enif_make_list(env, 0);
                    std::list<ERL_NIF_TERM> q;
                    unsigned int len;
                    if (existing_value) {
                        if (!enif_is_list(env, existing_term) ||
                                !enif_get_list_length(env, op[1], &len)) {
                            enif_free_env(env);
                            return false;
                        }

                        if(len == 0) {
                            new_term = existing_term;
                        } else {
                            tail = existing_term;
                            while(enif_get_list_cell(env, tail, &head, &tail)) {
                                q.push_back(std::move(head));
                            }

                            tail = op[1];
                            while(enif_get_list_cell(env, tail, &head, &tail)) {
                                for(auto it = q.begin(); it!=q.end();) {
                                    if(enif_compare(*it, head) == 0) {
                                        it = q.erase(it);
                                    } else {
                                        ++it;
                                    }
                                }
                            }
                            for(std::list<ERL_NIF_TERM>::reverse_iterator rq = q.rbegin(); rq!=q.rend(); ++rq) {
                                new_term = enif_make_list_cell(env, *rq, new_term);
                            }
                        }
                    }
                }
                else if (op[0] == ATOM_MERGE_LIST_DELETE) {
                    ERL_NIF_TERM head, tail, list_in;
                    unsigned int pos, i, len;
                    if(!enif_get_uint(env, op[1], &pos)) {
                        enif_free_env(env);
                        return false;
                    }
                    if (!enif_get_list_length(env, existing_term, &len)) {
                        enif_free_env(env);
                        return false;
                    } else if (pos >= len) {
                         enif_free_env(env);
                        return false;
                    }
                    i = 0;
                    tail = existing_term;
                    list_in = enif_make_list(env, 0);
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        if (pos != i) {
                            list_in = enif_make_list_cell(env, head, list_in);
                    }
                        i++;
                    }
                    enif_make_reverse_list(env, list_in, &new_term);
                }
                else if (op[0] == ATOM_MERGE_BINARY_APPEND) {

                    if(!enif_inspect_binary(env, op[1], &bin)) {
                        enif_free_env(env);
                        return false;
                    }

                    if(!should_encode || !existing_value) {
                        if (!existing_value) {
                            new_value->assign((const char *)bin.data, bin.size);
                        } else {
                            new_value->reserve(existing_value->size() + bin.size);
                            new_value->assign(existing_value->data(), existing_value->size());
                            new_value->append((const char *)bin.data, bin.size);
                        }
                    } else {
                        if(!enif_inspect_binary(env, existing_term, &bin_term)) {
                            enif_free_env(env);
                            return false;
                        }
                        std::string s = std::string((const char*)bin_term.data, bin_term.size);
                        s.append((const char *)bin.data, bin.size);
                        memcpy(enif_make_new_binary(env, s.size(), &new_term), s.data(), s.size());
                    }
                }
            }
            else if (arity == 3) {
                if (op[0] == ATOM_MERGE_LIST_SET) {
                    ERL_NIF_TERM head, tail, list_in;
                    unsigned int pos, i, len;
                    if(!enif_get_uint(env, op[1], &pos)) {
                        enif_free_env(env);
                        return false;
                    }

                    if (!enif_get_list_length(env, existing_term, &len)) {
                        enif_free_env(env);
                        return false;
                    } else if (pos >= len) {
                        enif_free_env(env);
                        return false;
                    }

                    i = 0;
                    tail = existing_term;
                    list_in = enif_make_list(env, 0);
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        if (pos == i) {
                            list_in = enif_make_list_cell(env, op[2], list_in);
                        } else {
                            list_in = enif_make_list_cell(env, head, list_in);
                        }
                        i++;
                    }
                    enif_make_reverse_list(env, list_in, &new_term);
                }
                else if (op[0] == ATOM_MERGE_LIST_DELETE) {
                    ERL_NIF_TERM head, tail, list_in;
                    unsigned int start, end, i, len;
                    if(!enif_get_uint(env, op[1], &start) || !enif_get_uint(env, op[2], &end)) {
                        enif_free_env(env);
                        return false;
                    }
                    if (!enif_get_list_length(env, existing_term, &len)) {
                        enif_free_env(env);
                        return false;
                    }

                    if ((start >= len) || (start >= end) || (end >= len))  {
                         enif_free_env(env);
                        return false;
                    }

                    i = 0;
                    tail = existing_term;
                    list_in = enif_make_list(env, 0);
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        if ((i < start) || (i > end)) {
                            list_in = enif_make_list_cell(env, head, list_in);
                        }
                        i++;
                    }
                    enif_make_reverse_list(env, list_in, &new_term);
                }
                else if (op[0] == ATOM_MERGE_LIST_INSERT) {
                    ERL_NIF_TERM head, tail, itail, ihead, list_in;
                    unsigned int pos, i, len;

                    if(!enif_get_uint(env, op[1], &pos) || !enif_is_list(env, op[2])) {
                        enif_free_env(env);
                        return false;
                    }

                    if (!enif_get_list_length(env, existing_term, &len)) {
                        enif_free_env(env);
                        return false;
                    }

                    if (pos >= len) {
                        enif_free_env(env);
                        return false;
                    }

                    i = 0;
                    tail = existing_term;
                    itail = op[2];
                    list_in = enif_make_list(env, 0);
                    while(enif_get_list_cell(env, tail, &head, &tail)) {
                        if (pos == i) {
                            while(enif_get_list_cell(env, itail, &ihead, &itail)) {
                                list_in = enif_make_list_cell(env, ihead, list_in);
                            }
                        }
                        list_in = enif_make_list_cell(env, head, list_in);
                        i++;
                    }
                    enif_make_reverse_list(env, list_in, &new_term);
                } else if (op[0] == ATOM_MERGE_BINARY_ERASE) {
                    unsigned int pos, count, pos2;

                    if(!enif_get_uint(env, op[1], &pos)
                            || !enif_get_uint(env, op[2], &count)
                            || !existing_value) {
                        enif_free_env(env);
                        return false;
                    }

                    pos2 = pos + count;
                    if(!should_encode) {
                        if(pos2 > existing_value->size()) {
                            enif_free_env(env);
                            return false;
                        }
                        new_value->assign(existing_value->data(), existing_value->size());
                        new_value->erase(pos, count);
                    } else {
                        if(!enif_inspect_binary(env, existing_term, &bin_term)) {
                            enif_free_env(env);
                            return false;
                        }
                        if(pos2 > bin_term.size) {
                            enif_free_env(env);
                            return false;
                        }
                        std::string s = std::string((const char*)bin_term.data, bin_term.size);
                        s.erase(pos, count);
                        memcpy(enif_make_new_binary(env, s.size(), &new_term), s.data(), s.size());
                    }
                }
                else if (op[0] == ATOM_MERGE_BINARY_INSERT) {
                    unsigned int pos;

                    if(!enif_get_uint(env, op[1], &pos)
                            || !enif_inspect_binary(env, op[2], &bin)
                            || !existing_value) {
                        enif_free_env(env);
                        return false;
                    }

                    std::string chunk = std::string((const char*)bin.data, bin.size);
                    if(!should_encode) {
                        if(pos > existing_value->size()) {
                            enif_free_env(env);
                            return false;
                        }
                        new_value->assign(existing_value->data(), existing_value->size());
                        new_value->insert(pos, chunk);
                    } else {
                        if(!enif_inspect_binary(env, existing_term, &bin_term)) {
                            enif_free_env(env);
                            return false;
                        }

                        std::string s = std::string((const char*)bin_term.data, bin_term.size);

                        if(pos > s.size()) {
                            enif_free_env(env);
                            return false;
                        }
                        s.insert(pos, chunk);
                        memcpy(enif_make_new_binary(env, s.size(), &new_term), s.data(), s.size());
                    }
                }
            }
            else if (arity == 4) {
                if (op[0] == ATOM_MERGE_BINARY_REPLACE) {
                    unsigned int pos, pos2, count;

                    if(!enif_get_uint(env, op[1], &pos)
                            || !enif_get_uint(env, op[2], &count)
                            || !existing_value) {
                        enif_free_env(env);
                        return false;
                    }

                    if(!enif_inspect_binary(env, op[3], &bin)) {
                        enif_free_env(env);
                        return false;
                    }

                    pos2 = pos + count;
                    if(!should_encode) {
                        if (pos2 > existing_value->size()) {
                            enif_free_env(env);
                            return false;
                        }
                        std::string s = std::string((const char*)existing_value->data(),  existing_value->size());
                        s.replace(pos, count,  (const char *)bin.data, bin.size);
                        new_value->assign(s.data(), s.size());
                    } else {
                        if(!enif_inspect_binary(env, existing_term, &bin_term)) {
                            enif_free_env(env);
                            return false;
                        }

                        if (pos2 > bin_term.size) {
                            enif_free_env(env);
                            return false;
                        }
                        std::string s = std::string((const char*)bin_term.data, bin_term.size);
                        s.replace(pos, count,(const char *)bin.data, bin.size);
                        memcpy(enif_make_new_binary(env, s.size(), &new_term), s.data(), s.size());
                    }
                }
            }
        }

        if (new_term) {

            if (!enif_term_to_binary(env, new_term, &bin)) {
                enif_free_env(env);
                return false;
            }
            rocksdb::Slice term_slice((const char*)bin.data, bin.size);
            new_value->reserve(term_slice.size());
            new_value->assign(term_slice.data(), term_slice.size());

        }

        enif_free_env(env);
        return true;
    };

    const char* ErlangMergeOperator::Name() const  {
        return "ErlangMergeOperator";
    }

    std::shared_ptr<ErlangMergeOperator> CreateErlangMergeOperator() {
        return std::make_shared<ErlangMergeOperator>();
    }
}
