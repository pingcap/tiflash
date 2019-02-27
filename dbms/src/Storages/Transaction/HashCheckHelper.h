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
void checkObjectHashInFile(const std::string & path, const std::vector<size_t> & object_bytes, const std::vector<bool> & use,
    const std::vector<uint128> & expected_hash_codes, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE);


} // namespace FileHashCheck

} // namespace DB