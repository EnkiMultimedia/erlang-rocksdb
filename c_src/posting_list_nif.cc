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

#include <string>
#include <unordered_map>
#include <cstdint>
#include <cstring>

#include "erl_nif.h"
#include "atoms.h"
#include "erocksdb.h"

namespace erocksdb {

// Parse posting list binary: <Len:32><Flag:8><Key:Len>...
// Returns map of key -> is_tombstone (last occurrence wins)
static std::unordered_map<std::string, bool> parse_posting_list(
    const unsigned char* data, size_t size)
{
    std::unordered_map<std::string, bool> result;
    const unsigned char* ptr = data;
    const unsigned char* end = data + size;

    while (ptr + 5 <= end) {
        uint32_t len = (static_cast<uint32_t>(ptr[0]) << 24) |
                       (static_cast<uint32_t>(ptr[1]) << 16) |
                       (static_cast<uint32_t>(ptr[2]) << 8) |
                       static_cast<uint32_t>(ptr[3]);
        ptr += 4;

        bool is_tombstone = (*ptr != 0);
        ptr += 1;

        if (ptr + len > end) break;

        std::string key(reinterpret_cast<const char*>(ptr), len);
        ptr += len;

        result[key] = is_tombstone;  // Last occurrence wins
    }
    return result;
}

// posting_list_keys(Binary) -> [binary()]
ERL_NIF_TERM PostingListKeys(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    auto entries = parse_posting_list(bin.data, bin.size);

    // Build list of active keys
    ERL_NIF_TERM list = enif_make_list(env, 0);
    for (const auto& [key, is_tombstone] : entries) {
        if (!is_tombstone) {
            ERL_NIF_TERM key_bin;
            unsigned char* buf = enif_make_new_binary(env, key.size(), &key_bin);
            if (buf == nullptr) {
                return enif_make_badarg(env);
            }
            memcpy(buf, key.data(), key.size());
            list = enif_make_list_cell(env, key_bin, list);
        }
    }
    return list;
}

// posting_list_contains(Binary, Key) -> boolean()
ERL_NIF_TERM PostingListContains(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin, search_key;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin) ||
        !enif_inspect_binary(env, argv[1], &search_key)) {
        return enif_make_badarg(env);
    }

    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
    auto entries = parse_posting_list(bin.data, bin.size);

    auto it = entries.find(key_str);
    if (it != entries.end() && !it->second) {
        return ATOM_TRUE;
    }
    return ATOM_FALSE;
}

// posting_list_find(Binary, Key) -> {ok, boolean()} | not_found
ERL_NIF_TERM PostingListFind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin, search_key;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin) ||
        !enif_inspect_binary(env, argv[1], &search_key)) {
        return enif_make_badarg(env);
    }

    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
    auto entries = parse_posting_list(bin.data, bin.size);

    auto it = entries.find(key_str);
    if (it == entries.end()) {
        return ATOM_NOT_FOUND;
    }
    return enif_make_tuple2(env, ATOM_OK,
        it->second ? ATOM_TRUE : ATOM_FALSE);
}

// posting_list_count(Binary) -> non_neg_integer()
ERL_NIF_TERM PostingListCount(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    auto entries = parse_posting_list(bin.data, bin.size);

    size_t count = 0;
    for (const auto& [key, is_tombstone] : entries) {
        if (!is_tombstone) count++;
    }
    return enif_make_uint64(env, count);
}

// posting_list_to_map(Binary) -> #{binary() => active | tombstone}
ERL_NIF_TERM PostingListToMap(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    auto entries = parse_posting_list(bin.data, bin.size);

    ERL_NIF_TERM map = enif_make_new_map(env);
    for (const auto& [key, is_tombstone] : entries) {
        ERL_NIF_TERM key_bin;
        unsigned char* buf = enif_make_new_binary(env, key.size(), &key_bin);
        if (buf == nullptr) {
            return enif_make_badarg(env);
        }
        memcpy(buf, key.data(), key.size());

        ERL_NIF_TERM value = is_tombstone ? ATOM_TOMBSTONE : ATOM_ACTIVE;
        enif_make_map_put(env, map, key_bin, value, &map);
    }
    return map;
}

} // namespace erocksdb
