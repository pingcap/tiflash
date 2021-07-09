//
// Created by schrodinger on 7/6/21.
//

#include "DMFileVersion.h"

//DB::DM::SerializedVersionInfo::SerializedVersionInfo(DB::DM::VersionInfo * ptr, Poco::File) {
//    union
//    {
//        uint32_t endianValue = DEFAULT_ENDIAN_VALUE;
//        uint8_t  endianArray[4];
//    };
//
//}

static inline void requiredRead(void * buffer, size_t size, const DB::ReadBufferPtr & filePtr)
{
    auto read = filePtr->read(reinterpret_cast<char *>(buffer), size);
    if (read != size)
    {
        throw Poco::ReadFileException("target has wrong size");
    }
}

#define CHECK_READ_FRAME(ALGO)                                                                                                   \
    case ChecksumAlgo::ALGO:                                                                                                     \
        std::memcpy(&frame, header.checksumArea + i * sizeof(ChecksumFrame<Digest::ALGO>), sizeof(ChecksumFrame<Digest::ALGO>)); \
        break;

#define CHECK_WRITE_FRAME(ALGO)                                                             \
    case ChecksumAlgo::ALGO:                                                                \
        std::memcpy(header.checksumArea + offset, &i, sizeof(ChecksumFrame<Digest::ALGO>)); \
        offset += sizeof(ChecksumFrame<Digest::ALGO>);                                      \
        break;

DB::DM::DeserializedVersionInfo::DeserializedVersionInfo(const DB::ReadBufferPtr & filePtr, bool readExtra) : checksums(), extraInfo()
{
    VersionInfo header{};
    requiredRead(&header, sizeof(header), filePtr);
    if (header.endianValue != DEFAULT_ENDIAN_VALUE)
    {
        throw Poco::ReadFileException("target file has wrong endianness");
    }
    version           = header.version;
    checksumAlgo      = header.checksumAlgo;
    checksumBlockSize = header.checksumBlockSize;

    auto count = getChecksumCountByVersion(version);
    for (auto i = 0ull; i < count; ++i)
    {
        FixedChecksumFrame frame{};
        switch (checksumAlgo)
        {
            CHECK_READ_FRAME(None)
            CHECK_READ_FRAME(CRC32)
            CHECK_READ_FRAME(CRC64)
            CHECK_READ_FRAME(City128)
            CHECK_READ_FRAME(XXH3)
        }
        checksums.push_back(frame);
    }

    if (readExtra)
    {
        for (auto i = 0ull; i < header.extraCount; ++i)
        {
            ExtraInfoFrame frame{};
            requiredRead(&frame, sizeof(frame), filePtr);
            std::string name(frame.nameSize, ' ');
            std::string content(frame.contentSize, ' ');
            requiredRead(name.data(), frame.nameSize, filePtr);
            requiredRead(content.data(), frame.contentSize, filePtr);
            extraInfo.emplace(std::move(name), std::move(content));
        }
    }
}

void DB::DM::DeserializedVersionInfo::writeToBuffer(const DB::WriteBufferPtr & filePtr)
{
    VersionInfo header{};
    header.endianValue       = DEFAULT_ENDIAN_VALUE;
    header.version           = version;
    header.checksumAlgo      = checksumAlgo;
    header.checksumBlockSize = checksumBlockSize;
    header.extraCount        = extraInfo.size();

    size_t offset = 0;
    for (const auto & i : checksums)
    {
        switch (checksumAlgo)
        {
            CHECK_WRITE_FRAME(None)
            CHECK_WRITE_FRAME(CRC32)
            CHECK_WRITE_FRAME(CRC64)
            CHECK_WRITE_FRAME(City128)
            CHECK_WRITE_FRAME(XXH3)
        }
    }
    filePtr->write(reinterpret_cast<char *>(&header), sizeof(header));

    for (const auto & i : extraInfo)
    {
        ExtraInfoFrame frame{};
        frame.nameSize    = i.first.size();
        frame.contentSize = i.second.size();
        filePtr->write(reinterpret_cast<char *>(&frame), sizeof(frame));
        filePtr->write(i.first.c_str(), i.first.size());
        filePtr->write(i.second.c_str(), i.second.size());
    }

    filePtr->next();
}
