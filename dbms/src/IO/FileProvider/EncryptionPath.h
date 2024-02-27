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

#include <common/types.h>
#include <pingcap/pd/Types.h>


namespace DB
{

using KeyspaceID = pingcap::pd::KeyspaceID;

/// 1. In OP, we use tikv proxy, all encryption info are stored in one file called file.dict.
/// Every update of file.dict will sync the whole file.
/// So when the file is too large, the update cost increases.
/// To keep the file size as small as possible, we reuse the encryption info among a group of related files.(e.g. the files of a DMFile)
/// For security reason, the same `iv` is not allowed to encrypt two different files,
/// so we combine the `iv` fetched from file.dict with the hash value of the file name to calculate the real `iv` for every file.
/// 2. In cloud, we use cse proxy, all encryption info are stored in page storage.
/// Each keyspace has only one encryption info. We will use full_path and file_name to generate `iv`.
/// Note: If keyspace_id = pingcap::pd::NullspaceID, the key manager will return an empty FileEncryptionInfo which will generate an empty AESCTRCipherStream.
struct EncryptionPath
{
    EncryptionPath(const String & full_path_, const String & file_name_)
        : full_path{full_path_}
        , file_name{file_name_}
    {}

    EncryptionPath(const String & full_path_, const String & file_name_, KeyspaceID keyspace_id_)
        : full_path{full_path_}
        , file_name{file_name_}
        , keyspace_id{keyspace_id_}
    {}

    // If it is a file, full_path is the full path of the file.
    // If it is a directory, full_path is the full path of the directory.
    const String full_path;
    const String file_name;
    // The id of the keyspace which the file belongs to.
    const KeyspaceID keyspace_id = pingcap::pd::NullspaceID;
};

} // namespace DB
