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

#include <Poco/File.h>

#include <ext/scope_guard.h>

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace FileHashCheck
{

using uint128 = CityHash_v1_0_2::uint128;

char * readFileFully(const std::string & path, size_t file_size);
void checkObjectHashInFile(const std::string & path, const std::vector<size_t> & object_bytes, const BoolVec & use,
    const std::vector<uint128> & expected_hash_codes, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE);


} // namespace FileHashCheck

} // namespace DB
