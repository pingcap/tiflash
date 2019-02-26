#include <fcntl.h>

#include <Storages/Transaction/HashCheckHelper.h>

namespace DB
{
namespace FileHashCheck
{
char * readFileFully(const std::string & path, size_t file_size)
{
    char * data = (char *)malloc(file_size);
    char * pos = data;

    auto fd = open(path.c_str(), O_RDONLY);
    if (-1 == fd)
        throwFromErrno("Cannot open file " + path, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    SCOPE_EXIT({ ::close(fd); });

    size_t remain = file_size;
    while (remain)
    {
        auto res = ::read(fd, pos, remain);

        if (-1 == res && errno != EINTR)
            throwFromErrno("Cannot read from file " + path, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        if (!res && errno != EINTR)
            throw Exception("End of file", ErrorCodes::UNEXPECTED_END_OF_FILE);

        remain -= res;
        pos += res;
    }

    return data;
}

void checkObjectHashInFile(const std::string & path, std::vector<size_t> object_bytes, std::vector<bool> use,
    std::vector<uint128> expected_hash_codes, size_t block_size)
{
    Poco::File file(path);
    size_t file_size = file.getSize();
    size_t total_size = 0;
    for (auto b : object_bytes)
        total_size += b;
    if (total_size != file_size)
        throw Exception("File size not match! Expected: " + DB::toString(total_size) + ", got: " + DB::toString(file_size),
            ErrorCodes::SIZE_CHECK_FAILED);

    char * file_data = readFileFully(path, file_size);
    SCOPE_EXIT({ free(file_data); });

    char * pos = file_data;
    for (size_t index = 0; index < object_bytes.size(); ++index)
    {
        if (use[index])
        {
            uint128 hashcode{0, 0};
            size_t bytes = object_bytes[index];
            while (bytes)
            {
                auto to_cal_bytes = std::min(bytes, block_size);
                hashcode = CityHash_v1_0_2::CityHash128WithSeed(pos, to_cal_bytes, hashcode);
                pos += to_cal_bytes;
                bytes -= to_cal_bytes;
            }

            if (hashcode != expected_hash_codes[index])
                throw Exception(
                    "File " + path + " hash code not match at object index: " + DB::toString(index), ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }
        else
        {
            pos += object_bytes[index];
        }
    }
}
} // namespace FileHashCheck

} // namespace DB