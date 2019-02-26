#pragma once

#include <Poco/File.h>

#include <ext/scope_guard.h>

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SIZE_CHECK_FAILED;
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int UNEXPECTED_END_OF_FILE;
extern const int CHECKSUM_DOESNT_MATCH;
} // namespace ErrorCodes

namespace FileHashCheck
{

using uint128 = CityHash_v1_0_2::uint128;

char * readFileFully(const std::string & path, size_t file_size);
void checkObjectHashInFile(const std::string & path, std::vector<size_t> object_bytes, std::vector<bool> use,
    std::vector<uint128> expected_hash_codes, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE);


} // namespace FileHashCheck

} // namespace DB