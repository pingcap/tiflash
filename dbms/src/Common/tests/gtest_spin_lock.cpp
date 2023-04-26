// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/SpinLock.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <mutex>

namespace DB
{
namespace tests
{
class SpinLockTest : public testing::Test
{
};

TEST_F(SpinLockTest, base)
{
    std::mutex mu;
    mu.lock();
    {
        SpinLock spin_lock{mu};
        ASSERT_FALSE(spin_lock);
    }
    mu.unlock();
    {
        SpinLock spin_lock{mu};
        ASSERT_TRUE(spin_lock);
        SpinLock spin_lock2{mu};
        ASSERT_FALSE(spin_lock2);
    }
    {
        SpinLock spin_lock{mu};
        ASSERT_TRUE(spin_lock);
    }
}

} // namespace tests
} // namespace DB
