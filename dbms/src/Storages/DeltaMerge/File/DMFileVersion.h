//
// Created by schrodinger on 7/6/21.
//

#ifndef CLICKHOUSE_VERSION_H
#define CLICKHOUSE_VERSION_H

#include <Common/config_version.h>
#include <Encryption/FileProvider.h>
#include <IO/CompressionSettings.h>

#include "Checksum/Checksum.h"
/*!
 * On the filesystem, the `Version File` takes the following form:
 *
 * -------------------------------------------------------------------------
 * | - Endian                 <val = 0x04030201, size = 4>                 |
 * | - FileVersion            <size = 8>                                   |
 * -------------------------------------------------------------------------
 * | - ChecksumAlgo           <size = 8>                                   |
 * | - ChecksumBlockSize      <size = 8>                                   |
 * -------------------------------------------------------------------------
 * | - Embedded ChecksumArea  <size = 512>                                 |
 * | --------------------------------------------------------------------- |
 * | | > Fixed Checksum Frame                                            | |
 * | | - Bytes                 <size = 8>                                | |
 * | | - Data                  <size = 64>                               | |
 * | --------------------------------------------------------------------- |
 * |                 .....................................                 |
 * |                < up to 7 frames, padded to 512 bytes >                |
 * -------------------------------------------------------------------------
 * | - Extra Count             <size = 8>                                  |
 * -------------------------------------------------------------------------
 * | - Extra Information Area                                              |
 * | --------------------------------------------------------------------- |
 * | | > Extra Information Header                                        | |
 * | | - Length of Name        <size = 8>                                | |
 * | | - Length of Body        <size = 8>                                | |
 * | --------------------------------------------------------------------- |
 * | | > Extra Information Data Area                                     | |
 * | |               .....................................               | |
 * | --------------------------------------------------------------------- |
 * |                 .....................................                 |
 * |                     < Up to Extra Count Blocks >                      |
 * -------------------------------------------------------------------------
 *
 * The version storage file can be deserialized into `DeserializedVersionInfo` and vice versa.
 *
 * How many embedded checksum frames are used is determined by the file version; currently
 * (in FileV1), only two fields are used: one for meta file, one for pack property file.
 *
 */
namespace DB::DM
{

enum class FileVersion : uint64_t
{
    Legacy = 0x0000'0000'0000'0000,
    FileV1 = 0x0000'0000'0000'0001
};

static inline constexpr FileVersion CURRENT_FILE_VERSION = FileVersion::FileV1;
static inline constexpr uint32_t    DEFAULT_ENDIAN_VALUE = 0x04030201u;

static inline constexpr size_t getChecksumCountByVersion(FileVersion version)
{
    switch (version)
    {
    case FileVersion::Legacy:
        return 0ull;
    case FileVersion::FileV1:
        return 2ull;
    }
    return 0ull;
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

class DeserializedVersionInfo
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

    DeserializedVersionInfo(FileVersion                        _version,
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

    DeserializedVersionInfo(const ReadBufferPtr & filePtr, bool readExtra);

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
