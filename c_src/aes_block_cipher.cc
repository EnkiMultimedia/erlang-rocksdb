// Copyright (c) 2016-2026 Benoit Chesneau
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

#include "aes_block_cipher.h"

#include <cstring>

#include <openssl/evp.h>

namespace erocksdb {

AesBlockCipher::AesBlockCipher(
    const std::array<unsigned char, kKeyBytes>& key)
    : key_(key) {}

AesBlockCipher::AesBlockCipher(const std::string& key) {
  // Caller validates length; copy up to kKeyBytes, zero-pad the rest.
  key_.fill(0);
  size_t n = key.size() < kKeyBytes ? key.size() : kKeyBytes;
  std::memcpy(key_.data(), key.data(), n);
}

// A single raw AES-256-ECB transform of one 16-byte block, in place.
// CTR mode only ever encrypts the counter block, so Encrypt and Decrypt are
// the same forward operation.
rocksdb::Status AesBlockCipher::Transform(char* data) {
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    return rocksdb::Status::IOError("AesBlockCipher: EVP_CIPHER_CTX_new failed");
  }

  unsigned char out[kBlockBytes];
  int out_len = 0;
  rocksdb::Status status = rocksdb::Status::OK();

  // No IV: the CTREncryptionProvider owns the per-file counter/IV, and we are
  // only transforming the counter block here. Padding is disabled because we
  // transform exactly one full block.
  if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ecb(), nullptr, key_.data(),
                         nullptr) != 1) {
    status = rocksdb::Status::IOError("AesBlockCipher: EVP_EncryptInit_ex failed");
  } else if (EVP_CIPHER_CTX_set_padding(ctx, 0) != 1) {
    status = rocksdb::Status::IOError("AesBlockCipher: set_padding failed");
  } else if (EVP_EncryptUpdate(ctx, out, &out_len,
                               reinterpret_cast<const unsigned char*>(data),
                               static_cast<int>(kBlockBytes)) != 1) {
    status = rocksdb::Status::IOError("AesBlockCipher: EVP_EncryptUpdate failed");
  } else if (out_len != static_cast<int>(kBlockBytes)) {
    status = rocksdb::Status::IOError("AesBlockCipher: unexpected block length");
  }

  if (status.ok()) {
    std::memcpy(data, out, kBlockBytes);
  }

  EVP_CIPHER_CTX_free(ctx);
  return status;
}

rocksdb::Status AesBlockCipher::Encrypt(char* data) { return Transform(data); }

rocksdb::Status AesBlockCipher::Decrypt(char* data) { return Transform(data); }

}  // namespace erocksdb
