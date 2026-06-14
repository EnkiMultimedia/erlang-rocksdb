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

#pragma once
#ifndef INCL_AES_BLOCK_CIPHER_H
#define INCL_AES_BLOCK_CIPHER_H

#include <array>
#include <string>

#include "rocksdb/env_encryption.h"
#include "rocksdb/status.h"

namespace erocksdb {

// AesBlockCipher implements rocksdb::BlockCipher using AES-256 in raw ECB
// block mode (a single forward AES block transform).
//
// It is meant to be wrapped by rocksdb's CTREncryptionProvider. In CTR mode the
// provider XORs the plaintext with the AES-encrypted counter block, so both
// Encrypt() and Decrypt() must perform the *same* forward AES block operation
// on the 16-byte counter block. There is therefore no separate decrypt path.
//
// The key is a fixed 32-byte (256-bit) AES key. No IV is held here: the
// CTREncryptionProvider owns the per-file counter/IV.
class AesBlockCipher : public rocksdb::BlockCipher {
 public:
  static constexpr size_t kKeyBytes = 32;   // AES-256
  static constexpr size_t kBlockBytes = 16; // AES block size

  // key must be exactly kKeyBytes (32) bytes long.
  explicit AesBlockCipher(const std::array<unsigned char, kKeyBytes>& key);
  explicit AesBlockCipher(const std::string& key);

  ~AesBlockCipher() override = default;

  static const char* kClassName() { return "AES256CTR"; }
  const char* Name() const override { return kClassName(); }

  // BlockSize returns the AES block size (16 bytes).
  size_t BlockSize() override { return kBlockBytes; }

  // Encrypt performs a single forward AES-256 block transform of the 16-byte
  // block at data, in place.
  rocksdb::Status Encrypt(char* data) override;

  // Decrypt performs the same forward AES-256 block transform as Encrypt:
  // in CTR mode decryption of the counter block is identical to encryption.
  rocksdb::Status Decrypt(char* data) override;

 private:
  rocksdb::Status Transform(char* data);

  std::array<unsigned char, kKeyBytes> key_;
};

}  // namespace erocksdb

#endif  // INCL_AES_BLOCK_CIPHER_H
