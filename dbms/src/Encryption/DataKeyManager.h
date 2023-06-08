// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Encryption/KeyManager.h>
#include <Poco/Path.h>


namespace DB
{
struct EngineStoreServerWrap;
class DataKeyManager : public KeyManager
{
public:
    DataKeyManager(EngineStoreServerWrap * tiflash_instance_wrap_);

    ~DataKeyManager() = default;

    FileEncryptionInfo getFile(const String & fname) override;

    FileEncryptionInfo newFile(const String & fname) override;

    void deleteFile(const String & fname, bool throw_on_error) override;

    void linkFile(const String & src_fname, const String & dst_fname) override;

private:
    EngineStoreServerWrap * tiflash_instance_wrap;
};
} // namespace DB
