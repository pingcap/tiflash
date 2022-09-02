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

#include <Common/MemoryTracker.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
namespace
{
class MemTrackerTest : public ::testing::Test
{
};

TEST_F(MemTrackerTest, testBasic)
try
{
    MemoryTracker mem_tracker;
    mem_tracker.alloc(1024);
    ASSERT_EQ(1024, mem_tracker.get());
    mem_tracker.free(1024);
    ASSERT_EQ(0, mem_tracker.get());
}
CATCH

TEST_F(MemTrackerTest, testRootAndChild)
try
{
    MemoryTracker root_mem_tracker;
    MemoryTracker child_mem_tracker(512);
    child_mem_tracker.setNext(&root_mem_tracker);
    // alloc 500
    child_mem_tracker.alloc(500);
    ASSERT_EQ(500, child_mem_tracker.get());
    ASSERT_EQ(500, root_mem_tracker.get());

    // alloc 256 base on 500
    bool has_err = false;
    try
    {
        child_mem_tracker.alloc(256); //500 + 256 > limit(512)
    }
    catch (...)
    {
        has_err = true;
    }
    ASSERT_TRUE(has_err);
    ASSERT_EQ(500, child_mem_tracker.get());
    ASSERT_EQ(500, root_mem_tracker.get());

    //free 500
    child_mem_tracker.free(500);
    ASSERT_EQ(0, child_mem_tracker.get());
    ASSERT_EQ(0, root_mem_tracker.get());
}
CATCH

TEST_F(MemTrackerTest, testRootAndMultipleChild)
try
{
    MemoryTracker root(512); // limit 512
    MemoryTracker child1(512); // limit 512
    MemoryTracker child2(512); // limit 512
    child1.setNext(&root);
    child2.setNext(&root);
    // alloc 500 on child1
    child1.alloc(500);
    ASSERT_EQ(500, child1.get());
    ASSERT_EQ(0, child2.get());
    ASSERT_EQ(500, root.get());


    // alloc 500 on child2, should fail
    bool has_err = false;
    try
    {
        child2.alloc(500); // root will throw error because of "out of quota"
    }
    catch (...)
    {
        has_err = true;
    }
    ASSERT_TRUE(has_err);
    ASSERT_EQ(500, child1.get());
    ASSERT_EQ(0, child2.get());
    ASSERT_EQ(500, root.get());

    // alloc 10 on child2
    child2.alloc(10);
    ASSERT_EQ(500, child1.get());
    ASSERT_EQ(10, child2.get());
    ASSERT_EQ(510, root.get());

    // free 500 on child1
    child1.free(500);
    ASSERT_EQ(0, child1.get());
    ASSERT_EQ(10, child2.get());
    ASSERT_EQ(10, root.get());

    // free 10 on child2
    child2.free(10);
    ASSERT_EQ(0, child1.get());
    ASSERT_EQ(0, child2.get());
    ASSERT_EQ(0, root.get());
}
CATCH


} // namespace
} // namespace DB::tests
