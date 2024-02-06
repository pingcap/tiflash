// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/nocopyable.h>
#include <IO/Endian.h>
#include <Poco/Crypto/DigestEngine.h>
#include <Poco/HMACEngine.h>
#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <common/types.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>


namespace DB
{

// class EncryptionKeyCore is only used in EncryptionKey, so put it in anonymous namespace
namespace
{
// HMAC SHA256 DigestEngine
class SHA256Engine : public Poco::Crypto::DigestEngine
{
public:
    enum
    {
        BLOCK_SIZE = 64,
        DIGEST_SIZE = 32
    };

    SHA256Engine()
        : DigestEngine("SHA256")
    {}
};


// EncryptionKeyCore stores information of a encryption key.
// Different version will use differe key to encrypt data, but the plain text is the same.
// And the data encrypted by differe version can also be decrypted.
class EncryptionKeyCore
{
public:
    EncryptionKeyCore(String cipher_text_, String plain_text_, String current_key_, UInt32 current_ver_)
        : cipher_text(std::move(cipher_text_))
        , plain_text(std::move(plain_text_))
        , current_key(std::move(current_key_))
        , current_ver(current_ver_)
    {}

    DISALLOW_COPY_AND_MOVE(EncryptionKeyCore);

    FileEncryptionInfo generateEncryptionInfo(String && iv) const
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Ok,
            EncryptionMethod::Aes256Ctr,
            RawCppString::New(current_key),
            RawCppString::New(iv),
            nullptr,
        };
    }

    static String newCurrentKey(String & plain_text, UInt32 version)
    {
        Poco::HMACEngine<SHA256Engine> hmac{plain_text};
        hmac.update(version);
        const auto & digest = hmac.digest();
        return String(reinterpret_cast<const char *>(digest.data()), digest.size());
    }

    // return a new EncryptionKeyCore with the given version
    std::shared_ptr<EncryptionKeyCore> setVersion(UInt32 new_ver)
    {
        const auto current_key = newCurrentKey(plain_text, new_ver);
        return std::make_shared<EncryptionKeyCore>(cipher_text, plain_text, current_key, new_ver);
    }

    // get the given version key
    String getKey(UInt32 version)
    {
        if (current_ver != version)
        {
            return newCurrentKey(plain_text, version);
        }
        else
        {
            return current_key;
        }
    }

    String exportString() const
    {
        std::vector<char> buffer;
        buffer.reserve(1 + sizeof(current_ver) + cipher_text.size());
        buffer.push_back(static_cast<char>(EncryptionMethod::Aes256Ctr));
        auto ver = toBigEndian(current_ver);
        buffer.insert(
            buffer.end(),
            reinterpret_cast<const char *>(&ver),
            reinterpret_cast<const char *>(&ver) + sizeof(current_ver));
        buffer.insert(buffer.end(), cipher_text.begin(), cipher_text.end());
        return String(buffer.data(), buffer.size());
    }

private:
    // encrypted<plain_text>, encrypted by master key
    // The text stored in disk.
    String cipher_text; // 64 bytes
    // the plain text of the key
    String plain_text; // 64 bytes
    // The real key used to encrypt data
    String current_key; // 32 bytes
    // The version of current key
    UInt32 current_ver = 0;
};
} // namespace

class EncryptionKey
{
private:
    explicit EncryptionKey(const std::shared_ptr<EncryptionKeyCore> & core_)
        : core(core_)
    {}

public:
    static constexpr UInt32 KEY_LENGTH = 64;
    static constexpr UInt32 EXPORT_KEY_LENGTH = 1 + 4 + KEY_LENGTH; // encryption_method + version + key

    EncryptionKey(String & cipher_text_, String & plain_text_, UInt32 current_ver_)
    {
        const auto current_key = EncryptionKeyCore::newCurrentKey(plain_text_, current_ver_);
        core = std::make_shared<EncryptionKeyCore>(
            std::move(cipher_text_),
            std::move(plain_text_),
            current_key,
            current_ver_);
    }

    FileEncryptionInfo generateEncryptionInfo(String && iv) const
    {
        return core->generateEncryptionInfo(std::move(iv));
    }

    ~EncryptionKey() = default;

    // return the key of the given version
    EncryptionKey setVersion(UInt32 new_ver) const { return EncryptionKey(core->setVersion(new_ver)); }

    String exportString() const { return core->exportString(); }

private:
    // EncryptionKey holds a shared_ptr to EncryptionKeyCore
    // Several EncryptionKey can share the same EncryptionKeyCore instance
    std::shared_ptr<EncryptionKeyCore> core;
};

using EncryptionKeyPtr = std::shared_ptr<EncryptionKey>;

} // namespace DB
