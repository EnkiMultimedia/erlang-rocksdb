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
#include <map>
#include <set>
#include <vector>
#include <cstdint>
#include <cstring>

#include "erl_nif.h"
#include "atoms.h"
#include "erocksdb.h"
#include "posting_list_merge_operator.h"

#ifdef WITH_CROARING
#include <roaring/roaring64.h>
#endif

namespace erocksdb {

// Forward declaration
struct PostingListResource;

// Resource type for parsed posting lists
static ErlNifResourceType* posting_list_resource_type = nullptr;

struct PostingListResource {
    std::set<std::string> keys;                     // All keys (for exact lookup)
    std::vector<std::string> sorted_keys;           // For indexed access
#ifdef WITH_CROARING
    roaring64_bitmap_t* bitmap;                     // For fast hash lookup
#endif

    PostingListResource() {
#ifdef WITH_CROARING
        bitmap = nullptr;
#endif
    }

    ~PostingListResource() {
#ifdef WITH_CROARING
        if (bitmap) {
            roaring64_bitmap_free(bitmap);
        }
#endif
    }
};

static void posting_list_resource_dtor(ErlNifEnv* env, void* obj) {
    auto* res = static_cast<PostingListResource*>(obj);
    res->~PostingListResource();
}

// Initialize resource type (call from nif_load)
bool init_posting_list_resource(ErlNifEnv* env) {
    posting_list_resource_type = enif_open_resource_type(
        env, nullptr, "posting_list",
        posting_list_resource_dtor,
        ERL_NIF_RT_CREATE, nullptr);
    return posting_list_resource_type != nullptr;
}

// MurmurHash3 64-bit finalizer for mixing (same as in merge operator)
static inline uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

static uint64_t hash_key(const std::string& key) {
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

// Detect format version from data
static uint8_t detect_version(const unsigned char* data, size_t size) {
    if (size == 0) {
        return POSTING_LIST_V1;
    }
    if (data[0] == POSTING_LIST_V2) {
        return POSTING_LIST_V2;
    }
    return POSTING_LIST_V1;
}

// Parse V1 posting list binary: <Len:32><Flag:8><Key:Len>...
static std::set<std::string> parse_posting_list_v1(
    const unsigned char* data, size_t size)
{
    std::set<std::string> result;
    std::map<std::string, bool> states;
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

        states[key] = is_tombstone;  // Last occurrence wins
    }

    for (const auto& [key, is_tombstone] : states) {
        if (!is_tombstone) {
            result.insert(key);
        }
    }
    return result;
}

// Parse V2 posting list binary
// Returns sorted set of active keys
static std::set<std::string> parse_posting_list_v2(
    const unsigned char* data, size_t size)
{
    std::set<std::string> result;
    const unsigned char* ptr = data;
    const unsigned char* end = data + size;

    // Skip version byte
    if (ptr >= end) return result;
    ptr += 1;

    // Read bitmap size
    if (ptr + 4 > end) return result;
    uint32_t bitmap_size = (static_cast<uint32_t>(ptr[0]) << 24) |
                           (static_cast<uint32_t>(ptr[1]) << 16) |
                           (static_cast<uint32_t>(ptr[2]) << 8) |
                           static_cast<uint32_t>(ptr[3]);
    ptr += 4;

    // Skip bitmap data
    if (ptr + bitmap_size > end) return result;
    ptr += bitmap_size;

    // Read key count
    if (ptr + 4 > end) return result;
    uint32_t key_count = (static_cast<uint32_t>(ptr[0]) << 24) |
                         (static_cast<uint32_t>(ptr[1]) << 16) |
                         (static_cast<uint32_t>(ptr[2]) << 8) |
                         static_cast<uint32_t>(ptr[3]);
    ptr += 4;

    // Read sorted keys
    for (uint32_t i = 0; i < key_count && ptr + 4 <= end; i++) {
        uint32_t len = (static_cast<uint32_t>(ptr[0]) << 24) |
                       (static_cast<uint32_t>(ptr[1]) << 16) |
                       (static_cast<uint32_t>(ptr[2]) << 8) |
                       static_cast<uint32_t>(ptr[3]);
        ptr += 4;

        if (ptr + len > end) break;

        std::string key(reinterpret_cast<const char*>(ptr), len);
        ptr += len;

        result.insert(key);
    }
    return result;
}

// Parse any format (auto-detect)
static std::set<std::string> parse_posting_list(
    const unsigned char* data, size_t size)
{
    uint8_t version = detect_version(data, size);
    if (version == POSTING_LIST_V2) {
        return parse_posting_list_v2(data, size);
    }
    return parse_posting_list_v1(data, size);
}

#ifdef WITH_CROARING
// Extract bitmap from V2 posting list (returns nullptr for V1)
static roaring64_bitmap_t* extract_bitmap_v2(
    const unsigned char* data, size_t size)
{
    if (detect_version(data, size) != POSTING_LIST_V2) {
        return nullptr;
    }

    const unsigned char* ptr = data;
    const unsigned char* end = data + size;

    // Skip version byte
    ptr += 1;

    // Read bitmap size
    if (ptr + 4 > end) return nullptr;
    uint32_t bitmap_size = (static_cast<uint32_t>(ptr[0]) << 24) |
                           (static_cast<uint32_t>(ptr[1]) << 16) |
                           (static_cast<uint32_t>(ptr[2]) << 8) |
                           static_cast<uint32_t>(ptr[3]);
    ptr += 4;

    if (bitmap_size == 0) return nullptr;
    if (ptr + bitmap_size > end) return nullptr;

    return roaring64_bitmap_portable_deserialize_safe(
        reinterpret_cast<const char*>(ptr), bitmap_size);
}

// Build bitmap from keys (for V1 or building new)
static roaring64_bitmap_t* build_bitmap_from_keys(const std::set<std::string>& keys) {
    roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
    for (const auto& key : keys) {
        roaring64_bitmap_add(bitmap, hash_key(key));
    }
    return bitmap;
}
#endif

// Serialize set of keys to V2 format
static void serialize_v2(const std::set<std::string>& keys,
                         std::string& output) {
    output.clear();

#ifdef WITH_CROARING
    // Build roaring bitmap
    roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
    for (const auto& key : keys) {
        roaring64_bitmap_add(bitmap, hash_key(key));
    }

    // Serialize bitmap
    size_t bitmap_size = roaring64_bitmap_portable_size_in_bytes(bitmap);
    std::vector<char> bitmap_data(bitmap_size);
    roaring64_bitmap_portable_serialize(bitmap, bitmap_data.data());
    roaring64_bitmap_free(bitmap);

    // Version byte
    output.push_back(POSTING_LIST_V2);

    // Bitmap size (big-endian)
    uint32_t bs = static_cast<uint32_t>(bitmap_size);
    output.push_back((bs >> 24) & 0xFF);
    output.push_back((bs >> 16) & 0xFF);
    output.push_back((bs >> 8) & 0xFF);
    output.push_back(bs & 0xFF);

    // Bitmap data
    output.append(bitmap_data.data(), bitmap_size);
#else
    // Without CRoaring, write empty bitmap
    output.push_back(POSTING_LIST_V2);
    output.push_back(0);
    output.push_back(0);
    output.push_back(0);
    output.push_back(0);
#endif

    // Key count (big-endian)
    uint32_t kc = static_cast<uint32_t>(keys.size());
    output.push_back((kc >> 24) & 0xFF);
    output.push_back((kc >> 16) & 0xFF);
    output.push_back((kc >> 8) & 0xFF);
    output.push_back(kc & 0xFF);

    // Sorted keys (std::set is already sorted)
    for (const auto& key : keys) {
        uint32_t len = static_cast<uint32_t>(key.size());
        output.push_back((len >> 24) & 0xFF);
        output.push_back((len >> 16) & 0xFF);
        output.push_back((len >> 8) & 0xFF);
        output.push_back(len & 0xFF);
        output.append(key);
    }
}

// posting_list_keys(Binary) -> [binary()]
ERL_NIF_TERM PostingListKeys(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    auto keys = parse_posting_list(bin.data, bin.size);

    // Build list of keys (sorted)
    ERL_NIF_TERM list = enif_make_list(env, 0);
    // Iterate in reverse to build list in correct order
    for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
        ERL_NIF_TERM key_bin;
        unsigned char* buf = enif_make_new_binary(env, it->size(), &key_bin);
        if (buf == nullptr) {
            return enif_make_badarg(env);
        }
        memcpy(buf, it->data(), it->size());
        list = enif_make_list_cell(env, key_bin, list);
    }
    return list;
}

// Fast binary search in V2 sorted keys section (no allocation)
static bool binary_search_v2_keys(const unsigned char* data, size_t size,
                                   const unsigned char* search_key, size_t search_len) {
    const unsigned char* ptr = data;
    const unsigned char* end = data + size;

    // Skip version byte
    if (ptr >= end || *ptr != POSTING_LIST_V2) return false;
    ptr += 1;

    // Skip bitmap
    if (ptr + 4 > end) return false;
    uint32_t bitmap_size = (static_cast<uint32_t>(ptr[0]) << 24) |
                           (static_cast<uint32_t>(ptr[1]) << 16) |
                           (static_cast<uint32_t>(ptr[2]) << 8) |
                           static_cast<uint32_t>(ptr[3]);
    ptr += 4 + bitmap_size;

    // Read key count
    if (ptr + 4 > end) return false;
    uint32_t key_count = (static_cast<uint32_t>(ptr[0]) << 24) |
                         (static_cast<uint32_t>(ptr[1]) << 16) |
                         (static_cast<uint32_t>(ptr[2]) << 8) |
                         static_cast<uint32_t>(ptr[3]);
    ptr += 4;

    if (key_count == 0) return false;

    // Build index of key offsets for binary search
    std::vector<std::pair<const unsigned char*, uint32_t>> key_offsets;
    key_offsets.reserve(key_count);

    const unsigned char* scan = ptr;
    for (uint32_t i = 0; i < key_count && scan + 4 <= end; i++) {
        uint32_t len = (static_cast<uint32_t>(scan[0]) << 24) |
                       (static_cast<uint32_t>(scan[1]) << 16) |
                       (static_cast<uint32_t>(scan[2]) << 8) |
                       static_cast<uint32_t>(scan[3]);
        scan += 4;
        if (scan + len > end) break;
        key_offsets.push_back({scan, len});
        scan += len;
    }

    // Binary search
    size_t lo = 0, hi = key_offsets.size();
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const unsigned char* key_ptr = key_offsets[mid].first;
        uint32_t key_len = key_offsets[mid].second;

        // Compare
        size_t cmp_len = (key_len < search_len) ? key_len : search_len;
        int cmp = memcmp(key_ptr, search_key, cmp_len);
        if (cmp == 0) {
            if (key_len < search_len) cmp = -1;
            else if (key_len > search_len) cmp = 1;
        }

        if (cmp == 0) return true;
        if (cmp < 0) lo = mid + 1;
        else hi = mid;
    }
    return false;
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

    // Fast path for V2: binary search without allocation
    if (detect_version(bin.data, bin.size) == POSTING_LIST_V2) {
        if (binary_search_v2_keys(bin.data, bin.size, search_key.data, search_key.size)) {
            return ATOM_TRUE;
        }
        return ATOM_FALSE;
    }

    // V1 fallback: need to parse (has tombstones)
    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
    auto keys = parse_posting_list_v1(bin.data, bin.size);

    if (keys.count(key_str) > 0) {
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

    // For V2, all keys present are active (no tombstones stored)
    uint8_t version = detect_version(bin.data, bin.size);
    if (version == POSTING_LIST_V2) {
        auto keys = parse_posting_list_v2(bin.data, bin.size);
        if (keys.count(key_str) > 0) {
            return enif_make_tuple2(env, ATOM_OK, ATOM_FALSE);  // active (not tombstone)
        }
        return ATOM_NOT_FOUND;
    }

    // For V1, we need to check tombstone status
    std::map<std::string, bool> states;
    const unsigned char* ptr = bin.data;
    const unsigned char* end = bin.data + bin.size;

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

        states[key] = is_tombstone;
    }

    auto it = states.find(key_str);
    if (it == states.end()) {
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

    auto keys = parse_posting_list(bin.data, bin.size);
    return enif_make_uint64(env, keys.size());
}

// posting_list_to_map(Binary) -> #{binary() => active | tombstone}
ERL_NIF_TERM PostingListToMap(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM map = enif_make_new_map(env);

    uint8_t version = detect_version(bin.data, bin.size);
    if (version == POSTING_LIST_V2) {
        // V2: all keys are active
        auto keys = parse_posting_list_v2(bin.data, bin.size);
        for (const auto& key : keys) {
            ERL_NIF_TERM key_bin;
            unsigned char* buf = enif_make_new_binary(env, key.size(), &key_bin);
            if (buf == nullptr) {
                return enif_make_badarg(env);
            }
            memcpy(buf, key.data(), key.size());
            enif_make_map_put(env, map, key_bin, ATOM_ACTIVE, &map);
        }
    } else {
        // V1: parse with tombstone status
        std::map<std::string, bool> states;
        const unsigned char* ptr = bin.data;
        const unsigned char* end = bin.data + bin.size;

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

            states[key] = is_tombstone;
        }

        for (const auto& [key, is_tombstone] : states) {
            ERL_NIF_TERM key_bin;
            unsigned char* buf = enif_make_new_binary(env, key.size(), &key_bin);
            if (buf == nullptr) {
                return enif_make_badarg(env);
            }
            memcpy(buf, key.data(), key.size());
            ERL_NIF_TERM value = is_tombstone ? ATOM_TOMBSTONE : ATOM_ACTIVE;
            enif_make_map_put(env, map, key_bin, value, &map);
        }
    }
    return map;
}

// posting_list_version(Binary) -> 1 | 2
ERL_NIF_TERM PostingListVersion(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    uint8_t version = detect_version(bin.data, bin.size);
    return enif_make_uint(env, version);
}

// posting_list_intersection(Binary1, Binary2) -> Binary
ERL_NIF_TERM PostingListIntersection(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin1, bin2;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    auto keys1 = parse_posting_list(bin1.data, bin1.size);
    auto keys2 = parse_posting_list(bin2.data, bin2.size);

    // Compute intersection
    std::set<std::string> result;
    for (const auto& key : keys1) {
        if (keys2.count(key) > 0) {
            result.insert(key);
        }
    }

    // Serialize to V2 format
    std::string output;
    serialize_v2(result, output);

    // Create Erlang binary
    ERL_NIF_TERM result_bin;
    unsigned char* buf = enif_make_new_binary(env, output.size(), &result_bin);
    if (buf == nullptr) {
        return enif_make_badarg(env);
    }
    memcpy(buf, output.data(), output.size());
    return result_bin;
}

// posting_list_union(Binary1, Binary2) -> Binary
ERL_NIF_TERM PostingListUnion(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin1, bin2;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    auto keys1 = parse_posting_list(bin1.data, bin1.size);
    auto keys2 = parse_posting_list(bin2.data, bin2.size);

    // Compute union (std::set automatically handles duplicates)
    std::set<std::string> result = keys1;
    result.insert(keys2.begin(), keys2.end());

    // Serialize to V2 format
    std::string output;
    serialize_v2(result, output);

    // Create Erlang binary
    ERL_NIF_TERM result_bin;
    unsigned char* buf = enif_make_new_binary(env, output.size(), &result_bin);
    if (buf == nullptr) {
        return enif_make_badarg(env);
    }
    memcpy(buf, output.data(), output.size());
    return result_bin;
}

// posting_list_difference(Binary1, Binary2) -> Binary
// Returns keys in Binary1 that are not in Binary2
ERL_NIF_TERM PostingListDifference(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin1, bin2;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    auto keys1 = parse_posting_list(bin1.data, bin1.size);
    auto keys2 = parse_posting_list(bin2.data, bin2.size);

    // Compute difference (keys1 - keys2)
    std::set<std::string> result;
    for (const auto& key : keys1) {
        if (keys2.count(key) == 0) {
            result.insert(key);
        }
    }

    // Serialize to V2 format
    std::string output;
    serialize_v2(result, output);

    // Create Erlang binary
    ERL_NIF_TERM result_bin;
    unsigned char* buf = enif_make_new_binary(env, output.size(), &result_bin);
    if (buf == nullptr) {
        return enif_make_badarg(env);
    }
    memcpy(buf, output.data(), output.size());
    return result_bin;
}

// posting_list_intersection_count(Binary1, Binary2) -> non_neg_integer()
// Fast cardinality using bitmap when available
ERL_NIF_TERM PostingListIntersectionCount(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin1, bin2;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

#ifdef WITH_CROARING
    // Try fast path with bitmaps if both are V2
    uint8_t v1 = detect_version(bin1.data, bin1.size);
    uint8_t v2 = detect_version(bin2.data, bin2.size);

    if (v1 == POSTING_LIST_V2 && v2 == POSTING_LIST_V2) {
        roaring64_bitmap_t* bitmap1 = extract_bitmap_v2(bin1.data, bin1.size);
        roaring64_bitmap_t* bitmap2 = extract_bitmap_v2(bin2.data, bin2.size);

        if (bitmap1 && bitmap2) {
            uint64_t count = roaring64_bitmap_and_cardinality(bitmap1, bitmap2);
            roaring64_bitmap_free(bitmap1);
            roaring64_bitmap_free(bitmap2);
            return enif_make_uint64(env, count);
        }

        if (bitmap1) roaring64_bitmap_free(bitmap1);
        if (bitmap2) roaring64_bitmap_free(bitmap2);
    }
#endif

    // Fallback: compute exact intersection
    auto keys1 = parse_posting_list(bin1.data, bin1.size);
    auto keys2 = parse_posting_list(bin2.data, bin2.size);

    size_t count = 0;
    for (const auto& key : keys1) {
        if (keys2.count(key) > 0) {
            count++;
        }
    }
    return enif_make_uint64(env, count);
}

// posting_list_bitmap_contains(Binary, Key) -> boolean()
// Fast bitmap-based lookup (may have false positives in rare hash collision cases)
ERL_NIF_TERM PostingListBitmapContains(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin, search_key;
    if (argc != 2 ||
        !enif_inspect_binary(env, argv[0], &bin) ||
        !enif_inspect_binary(env, argv[1], &search_key)) {
        return enif_make_badarg(env);
    }

#ifdef WITH_CROARING
    if (detect_version(bin.data, bin.size) == POSTING_LIST_V2) {
        roaring64_bitmap_t* bitmap = extract_bitmap_v2(bin.data, bin.size);
        if (bitmap) {
            std::string key_str(reinterpret_cast<const char*>(search_key.data),
                               search_key.size);
            uint64_t h = hash_key(key_str);
            bool contains = roaring64_bitmap_contains(bitmap, h);
            roaring64_bitmap_free(bitmap);
            return contains ? ATOM_TRUE : ATOM_FALSE;
        }
    }
#endif

    // Fallback to exact lookup
    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
    auto keys = parse_posting_list(bin.data, bin.size);

    if (keys.count(key_str) > 0) {
        return ATOM_TRUE;
    }
    return ATOM_FALSE;
}

// postings_open(Binary) -> {ok, Resource} | {error, Reason}
// Parse posting list into a resource for fast repeated lookups
ERL_NIF_TERM PostingsOpen(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    // Allocate resource
    void* mem = enif_alloc_resource(posting_list_resource_type, sizeof(PostingListResource));
    if (!mem) {
        return enif_make_tuple2(env, ATOM_ERROR,
            enif_make_atom(env, "alloc_failed"));
    }

    // Construct in place
    auto* res = new (mem) PostingListResource();

    // Parse keys
    res->keys = parse_posting_list(bin.data, bin.size);
    res->sorted_keys.reserve(res->keys.size());
    for (const auto& key : res->keys) {
        res->sorted_keys.push_back(key);
    }

#ifdef WITH_CROARING
    // Extract or build bitmap
    if (detect_version(bin.data, bin.size) == POSTING_LIST_V2) {
        res->bitmap = extract_bitmap_v2(bin.data, bin.size);
    }
    if (!res->bitmap) {
        res->bitmap = build_bitmap_from_keys(res->keys);
    }
#endif

    ERL_NIF_TERM result = enif_make_resource(env, res);
    enif_release_resource(res);

    return enif_make_tuple2(env, ATOM_OK, result);
}

// postings_contains(Resource, Key) -> boolean()
// Fast lookup using parsed resource
ERL_NIF_TERM PostingsContains(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PostingListResource* res;
    ErlNifBinary search_key;

    if (argc != 2 ||
        !enif_get_resource(env, argv[0], posting_list_resource_type, (void**)&res) ||
        !enif_inspect_binary(env, argv[1], &search_key)) {
        return enif_make_badarg(env);
    }

    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);

    if (res->keys.count(key_str) > 0) {
        return ATOM_TRUE;
    }
    return ATOM_FALSE;
}

// postings_bitmap_contains(Resource, Key) -> boolean()
// Fast hash-based lookup using bitmap
ERL_NIF_TERM PostingsBitmapContains(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PostingListResource* res;
    ErlNifBinary search_key;

    if (argc != 2 ||
        !enif_get_resource(env, argv[0], posting_list_resource_type, (void**)&res) ||
        !enif_inspect_binary(env, argv[1], &search_key)) {
        return enif_make_badarg(env);
    }

#ifdef WITH_CROARING
    if (res->bitmap) {
        std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
        uint64_t h = hash_key(key_str);
        if (roaring64_bitmap_contains(res->bitmap, h)) {
            return ATOM_TRUE;
        }
        return ATOM_FALSE;
    }
#endif

    // Fallback to exact lookup
    std::string key_str(reinterpret_cast<const char*>(search_key.data), search_key.size);
    if (res->keys.count(key_str) > 0) {
        return ATOM_TRUE;
    }
    return ATOM_FALSE;
}

// postings_count(Resource) -> non_neg_integer()
ERL_NIF_TERM PostingsCount(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PostingListResource* res;

    if (argc != 1 ||
        !enif_get_resource(env, argv[0], posting_list_resource_type, (void**)&res)) {
        return enif_make_badarg(env);
    }

    return enif_make_uint64(env, res->keys.size());
}

// postings_keys(Resource) -> [binary()]
ERL_NIF_TERM PostingsKeys(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PostingListResource* res;

    if (argc != 1 ||
        !enif_get_resource(env, argv[0], posting_list_resource_type, (void**)&res)) {
        return enif_make_badarg(env);
    }

    // Build list of keys (already sorted in std::set)
    ERL_NIF_TERM list = enif_make_list(env, 0);
    for (auto it = res->keys.rbegin(); it != res->keys.rend(); ++it) {
        ERL_NIF_TERM key_bin;
        unsigned char* buf = enif_make_new_binary(env, it->size(), &key_bin);
        if (buf == nullptr) {
            return enif_make_badarg(env);
        }
        memcpy(buf, it->data(), it->size());
        list = enif_make_list_cell(env, key_bin, list);
    }
    return list;
}

// postings_to_binary(Resource) -> binary()
// Convert postings resource back to V2 binary format
ERL_NIF_TERM PostingsToBinary(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PostingListResource* res;

    if (argc != 1 ||
        !enif_get_resource(env, argv[0], posting_list_resource_type, (void**)&res)) {
        return enif_make_badarg(env);
    }

    // Serialize to V2 format
    std::string output;
    serialize_v2(res->keys, output);

    // Create Erlang binary
    ERL_NIF_TERM result_bin;
    unsigned char* buf = enif_make_new_binary(env, output.size(), &result_bin);
    if (buf == nullptr) {
        return enif_make_badarg(env);
    }
    memcpy(buf, output.data(), output.size());
    return result_bin;
}

} // namespace erocksdb
