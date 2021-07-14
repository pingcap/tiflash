#pragma once
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace base
{
class Tmp_path_base : public ::testing::Test
{
public:
    inline void dropDataOnDisk()
    {
        if (Poco::File file(DB::tests::TiFlashTestEnv::getTemporaryPath()); file.exists())
            file.remove(true);
    }

    void SetUp() override { dropDataOnDisk(); }

    void TearDown() override
    {
        dropDataOnDisk();
    }
};
} // namespace base
} // namespace DB