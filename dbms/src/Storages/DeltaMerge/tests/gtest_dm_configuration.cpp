#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>
#include <Storages/DeltaMerge/File/DMConfigFile.h>
namespace DB::DM
{

TEST(DMConfigFile, Serialization)
{
    DMConfiguration original{
        TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, ChecksumAlgo::XXH3, {{"abc", "abc"}, {"123", "123"}}, {{"abc", "abc"}, {"123", "123"}}};

    std::stringstream ss;
    ss << original;

    DMConfiguration deserialized(ss);

    ASSERT_EQ(original.getChecksumAlgorithm(), deserialized.getChecksumAlgorithm());
    ASSERT_EQ(original.getChecksumFrameLength(), deserialized.getChecksumFrameLength());
    ASSERT_EQ(original.getDebugInfo(), deserialized.getDebugInfo());
    ASSERT_EQ(original.getEmbeddedChecksum(), deserialized.getEmbeddedChecksum());
}

} // namespace DB::DM
