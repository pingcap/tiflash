#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>
#include <Storages/DeltaMerge/File/DMFileVersion.h>

namespace DB
{
namespace DM
{
namespace tests
{

namespace
{
template <class T>
FixedChecksumFrame fakeFrame(std::string_view data)
{
    FixedChecksumFrame frame{};
    frame.bytes = data.size();
    T digest{};
    digest.update(data.data(), data.size());
    auto result = digest.checksum();
    std::memcpy(frame.checksum, &result, sizeof(result));
    return frame;
}
} // namespace

TEST(DMFileVersion, Serialization)
{
    auto                    data = std::vector<char>{};
    DeserializedVersionInfo info{DM::FileVersion::FileV1,
                                 DM::ChecksumAlgo::CRC64,
                                 TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
                                 {
                                     fakeFrame<Digest::CRC64>("abc"),
                                     fakeFrame<Digest::CRC64>("def"),
                                 },
                                 {
                                     {"field 0", "00000"},
                                 }

    };
    {
        auto writeBuffer = std::make_shared<WriteBufferFromVector<std::vector<char>>>(data);
        info.writeToBuffer(writeBuffer);
    }
    auto                    readBuffer = std::make_shared<ReadBufferFromMemory>(data.data(), data.size());
    DeserializedVersionInfo info2(readBuffer, true);
    ASSERT_EQ(info2.getVersion(), info.getVersion());
    ASSERT_EQ(info2.getChecksumAlgo(), info.getChecksumAlgo());
    ASSERT_EQ(info2.getChecksumBlockSize(), info.getChecksumBlockSize());
    ASSERT_EQ(info2.getInfoMap(), info.getInfoMap());
    {
        auto f1 = info.checksumAt<Digest::CRC64>(0);
        auto f2 = info2.checksumAt<Digest::CRC64>(0);
        ASSERT_EQ(f1.bytes, f2.bytes);
        ASSERT_EQ(f1.checksum, f2.checksum);
    }

    {
        auto f1 = info.checksumAt<Digest::CRC64>(1);
        auto f2 = info2.checksumAt<Digest::CRC64>(1);
        ASSERT_EQ(f1.bytes, f2.bytes);
        ASSERT_EQ(f1.checksum, f2.checksum);
    }
}
} // namespace tests
} // namespace DM
} // namespace DB