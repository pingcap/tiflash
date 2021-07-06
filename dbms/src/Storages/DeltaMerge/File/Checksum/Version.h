//
// Created by schrodinger on 7/6/21.
//

#ifndef CLICKHOUSE_VERSION_H
#define CLICKHOUSE_VERSION_H

#include <Encryption/FileProvider.h>

#include "Checksum.h"

namespace DB::DM
{

static inline constexpr uint32_t DEFAULT_ENDIAN_VALUE = 0x04030201u;

enum class FileVersion : uint64_t
{
    Legacy = 0x0000'0000'0000'0000,
    FileV1 = 0x0000'0000'0000'0001
};

static inline constexpr uint64_t CURRENT_FILE_VERSION = 0x00000001u;

static inline size_t getChecksumCountByVersion(FileVersion version)
{
    switch (version)
    {
    case FileVersion::Legacy:
        return 0;
    case FileVersion::FileV1:
        return 2;
    }
    __builtin_unreachable();
}


struct ExtraInfoFrame
{
    size_t  nameSize;
    size_t  contentSize;
    uint8_t data[0];
};

struct VersionInfo
{
    // version header
    union
    {
        uint32_t endianValue; // Set to 0x04030201;
        uint8_t  endianArray[4];
    };
    FileVersion version;

    // checksum header
    ChecksumAlgo checksumAlgo;
    size_t       checksumBlockSize;
    uint8_t      checksumArea[512]; // Currently, this area stores checksum for PackProperty and Meta.
                                    // extra space are left for future.

    // extra info for file diagnostic
    size_t         extraCount;
    ExtraInfoFrame extraData[0];
};

class SerializedVersionInfo
{
public:
    [[nodiscard]] FileVersion  getVersion() const { return version; }
    [[nodiscard]] ChecksumAlgo getChecksumAlgo() const { return checksumAlgo; }
    [[nodiscard]] size_t       getChecksumBlockSize() const { return checksumBlockSize; }

    template <class T>
    [[nodiscard]] auto checksumAt(size_t index) const
    {
        auto             frame = checksums.at(index);
        ChecksumFrame<T> translated_frame;
        std::memcpy(&translated_frame, &frame, sizeof(translated_frame));
        return translated_frame;
    }

    [[nodiscard]] const std::map<std::string, std::string> & getInfoMap() const { return extraInfo; }

    SerializedVersionInfo(FileVersion                        _version,
                          ChecksumAlgo                       _checksumAlgo,
                          size_t                             _checksumBlockSize,
                          std::vector<FixedChecksumFrame>    _checksums,
                          std::map<std::string, std::string> _extraInfo)
        : version(_version),
          checksumAlgo(_checksumAlgo),
          checksumBlockSize(_checksumBlockSize),
          checksums(std::move(_checksums)),
          extraInfo(std::move(_extraInfo))
    {
    }

    explicit SerializedVersionInfo(const ReadBufferPtr & filePtr);

    void writeToBuffer(const WriteBufferPtr & filePtr);

private:
    FileVersion                        version{};
    ChecksumAlgo                       checksumAlgo;
    size_t                             checksumBlockSize{};
    std::vector<FixedChecksumFrame>    checksums;
    std::map<std::string, std::string> extraInfo;
};


} // namespace DB::DM

#endif //CLICKHOUSE_VERSION_H
