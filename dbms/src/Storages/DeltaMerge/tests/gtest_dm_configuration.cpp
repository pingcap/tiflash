#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#include <Common/Checksum.h>
#include <Storages/DeltaMerge/File/DMConfiguration.h>
namespace DB::DM
{

template <ChecksumAlgo algo>
void runSerializationTest()
{
    DMConfiguration original{{{"abc", "abc"}, {"123", "123"}}, TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, algo};

    std::stringstream ss;
    ss << original;

    DMConfiguration deserialized(ss);

    ASSERT_EQ(original.getChecksumAlgorithm(), deserialized.getChecksumAlgorithm());
    ASSERT_EQ(original.getChecksumFrameLength(), deserialized.getChecksumFrameLength());
    ASSERT_EQ(original.getDebugInfo(), deserialized.getDebugInfo());
    ASSERT_EQ(original.getEmbeddedChecksum(), deserialized.getEmbeddedChecksum());
};

#define TEST_SERIALIZATION(ALGO) \
    TEST(DMConfiguration, ALGO##Serialization) { runSerializationTest<DB::ChecksumAlgo::ALGO>(); }

TEST_SERIALIZATION(None)
TEST_SERIALIZATION(CRC32)
TEST_SERIALIZATION(CRC64)
TEST_SERIALIZATION(City128)
TEST_SERIALIZATION(XXH3)

} // namespace DB::DM
