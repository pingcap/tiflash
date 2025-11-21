// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/tests/gtest_segment_bitmap.h>

using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{
class VersionChainTest : public SegmentBitmapFilterTest
{
protected:
    struct TestOptions
    {
        PageIdU64 seg_id = SEG_ID;
        std::string_view seg_data;
        std::optional<std::tuple<Int64, Int64, bool>> rowkey_range;
        std::vector<RowID> expected_base_versions;
        int caller_line;
    };

    void runVersionChainTest(const TestOptions & opts)
    {
        if (!opts.seg_data.empty())
            writeSegmentGeneric(opts.seg_data, opts.rowkey_range);

        auto [seg, snap] = getSegmentForRead(opts.seg_id);
        auto actual_base_versions = std::visit(
            [&](auto & version_chain) { return version_chain.replaySnapshot(*dm_context, *snap); },
            *(seg->version_chain));
        ASSERT_EQ(opts.expected_base_versions, *actual_base_versions) << "caller_line=" << opts.caller_line;
    }

    void checkHandleIndex(size_t expected_new_handle_count, size_t expected_dmfile_or_delete_range_count)
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        auto [actutal_new_handle_count, actual_dmfile_or_delete_range_count] = std::visit(
            [&](auto & version_chain) {
                return std::pair{
                    version_chain.new_handle_to_row_ids.handle_to_row_id.size(),
                    version_chain.dmfile_or_delete_range_list.size(),
                };
            },
            *(seg->version_chain));
        ASSERT_EQ(actutal_new_handle_count, expected_new_handle_count);
        ASSERT_EQ(actual_dmfile_or_delete_range_count, expected_dmfile_or_delete_range_count);
    }
};

INSTANTIATE_TEST_CASE_P(VersionChain, VersionChainTest, /* is_common_handle */ ::testing::Bool());

TEST_P(VersionChainTest, InMemory1)
try
{
    std::vector<RowID> excepted_base_versions(1000, NotExistRowID);
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory2)
try
{
    std::vector<RowID> excepted_base_versions(2000);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 0); // d_mem:[0, 1000)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem:[0, 1000)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory3)
try
{
    std::vector<RowID> excepted_base_versions(1100);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 100); // d_mem:[100, 200)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem:[100, 200)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory4)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(
        excepted_base_versions.begin(),
        excepted_base_versions.begin() + 1100,
        NotExistRowID); // d_mem:[0, 1000) + d_mem:[-100, 0)
    std::iota(excepted_base_versions.begin() + 1100, excepted_base_versions.end(), 0); // d_mem:[0, 100)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem:[-100, 100)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory5)
try
{
    std::vector<RowID> excepted_base_versions(2000);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 0); // d_mem_del:[0, 1000)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem_del:[0, 1000)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory6)
try
{
    std::vector<RowID> excepted_base_versions(1100);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 100); // d_mem_del:[100, 200)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem_del:[100, 200)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, InMemory7)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(
        excepted_base_versions.begin(),
        excepted_base_versions.begin() + 1100,
        NotExistRowID); // d_mem:[0, 1000) + d_mem:[-100, 0)
    std::iota(excepted_base_versions.begin() + 1100, excepted_base_versions.end(), 0); // d_mem_del:[0, 100)
    runVersionChainTest(TestOptions{
        .seg_data = "d_mem:[0, 1000)|d_mem_del:[-100, 100)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Tiny1)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 300,
        100); // d_mem:[200, 500)
    std::fill(
        excepted_base_versions.begin() + 400 + 300,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[500, 1000)
    runVersionChainTest(TestOptions{
        .seg_data = "d_tiny:[100, 500)|d_mem:[200, 1000)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, TinyDel1)
try
{
    std::vector<RowID> excepted_base_versions(600);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 100,
        100); // d_tiny_del:[200, 300)
    std::fill(
        excepted_base_versions.begin() + 400 + 100,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[0, 100)
    runVersionChainTest(TestOptions{
        .seg_data = "d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, DeleteRange)
try
{
    std::vector<RowID> excepted_base_versions(450);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(excepted_base_versions.begin() + 400, excepted_base_versions.begin() + 400 + 10, 140); // d_mem:[240, 250)
    std::fill(
        excepted_base_versions.begin() + 400 + 10,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[250, 290)
    runVersionChainTest(TestOptions{
        .seg_data = "d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Big)
try
{
    std::vector<RowID> excepted_base_versions(400 + 750 + 50);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 250,
        150); // d_big:[250, 500)
    std::fill(
        excepted_base_versions.begin() + 400 + 250,
        excepted_base_versions.begin() + 400 + 250 + 500,
        NotExistRowID); // d_big:[500, 1000)
    std::iota(excepted_base_versions.begin() + 400 + 250 + 500, excepted_base_versions.end(), 140); // d_mem:[240, 290)
    runVersionChainTest(TestOptions{
        .seg_data = "d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Stable1)
try
{
    std::vector<RowID> excepted_base_versions;
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Stable2)
try
{
    std::vector<RowID> excepted_base_versions;
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)|d_dr:[0, 1023)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH


TEST_P(VersionChainTest, Stable3)
try
{
    std::vector<RowID> excepted_base_versions(10);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.end(), 300); // s:[300, 310)
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Mix)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, Ranges)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});
}
CATCH

TEST_P(VersionChainTest, LogicalSplit)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    runVersionChainTest(TestOptions{
        .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__});

    auto new_seg_id = splitSegmentAt(SEG_ID, 512, Segment::SplitMode::Logical);

    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));
    // segment_range: [-inf, 512)
    // "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)"
    runVersionChainTest(TestOptions{
        .seg_id = SEG_ID,
        .expected_base_versions = excepted_base_versions,
        .caller_line = __LINE__,
    });

    // segment_range: [512, +inf)
    // "s:[0, 1024)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)"
    std::vector<RowID> other_excepted_base_versions(10 + 55 + 7);
    std::iota(other_excepted_base_versions.begin(), other_excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::iota(
        other_excepted_base_versions.begin() + 10,
        other_excepted_base_versions.begin() + 10 + 55,
        200); // d_tiny:[200, 255)
    std::iota(
        other_excepted_base_versions.begin() + 10 + 55,
        other_excepted_base_versions.end(),
        298); // d_mem:[298, 305)
    runVersionChainTest(TestOptions{
        .seg_id = *new_seg_id,
        .expected_base_versions = other_excepted_base_versions,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(VersionChainTest, BigPart_Middle)
try
{
    // For ColumnFileBig, only packs that intersection with the rowkey range will be considered in BitmapFilter.
    // Packs in rowkey_range: [270, 280)|[280, 290)|[290, 300)
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[250, 1000):pack_size_10",
        .rowkey_range = std::make_tuple(275, 295, /*including_right_boundary*/ false),
        .expected_base_versions = std::vector<RowID>(30, NotExistRowID),
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(VersionChainTest, BigPart_Left)
try
{
    // For ColumnFileBig, only packs that intersection with the rowkey range will be considered in BitmapFilter.
    // Packs in rowkey_range: [240, 250)|[250, 260)|[270, 280)|[280, 290)|[290, 300)
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[250, 1000):pack_size_10",
        .rowkey_range = std::make_tuple(240, 295, /*including_right_boundary*/ false),
        .expected_base_versions = std::vector<RowID>(50, NotExistRowID),
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(VersionChainTest, BigPart_Right)
try
{
    // For ColumnFileBig, only packs that intersection with the rowkey range will be considered in BitmapFilter.
    // Packs in rowkey_range: [940, 950)|[950, 960)|[960, 970)|[970, 980)|[980, 990)|[990, 1000)
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[250, 1000):pack_size_10",
        .rowkey_range = std::make_tuple(940, 995, /*including_right_boundary*/ false),
        .expected_base_versions = std::vector<RowID>(60, NotExistRowID),
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(VersionChainTest, Big_NoIntersection)
{
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[0, 10):pack_size_3|d_big:[20, 30):pack_size_3|d_big:[10, 20):pack_size_3",
        .expected_base_versions = std::vector<RowID>(30, NotExistRowID),
        .caller_line = __LINE__,
    });
    checkHandleIndex(0, 4); // 3 cf_big + 1 stable dmfile
}

TEST_P(VersionChainTest, Big_Intersection_FalsePositive)
{
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[0, 10):pack_size_3|d_big:[20, 30):pack_size_3|merge_delta|d_big:[10, 20):pack_size_3",
        .expected_base_versions = std::vector<RowID>(10, NotExistRowID),
        .caller_line = __LINE__,
    });
    // d_big:[0, 10) + d_big:[20, 30) => stable: [0, 30)
    // d_big[10, 20) is intersect with stable.
    checkHandleIndex(10, 1); // 1 stable dmfile
}

TEST_P(VersionChainTest, Big_Intersection_DeleteRange)
{
    runVersionChainTest(TestOptions{
        .seg_data
        = "d_big:[0, 10):pack_size_3|d_big:[20, 30):pack_size_3|merge_delta|d_dr:[10, 20)|d_big:[10, 20):pack_size_3",
        .expected_base_versions = std::vector<RowID>(10, NotExistRowID),
        .caller_line = __LINE__,
    });
    checkHandleIndex(0, 3); // 1 cf_big + 1 delete range + 1 stable dmfile
}

TEST_P(VersionChainTest, Big_NoIntersection_Tiny)
{
    runVersionChainTest(TestOptions{
        .seg_data = "d_big:[0, 10):pack_size_3|d_tiny:[10, 20)|d_big:[20, 30):pack_size_3",
        .expected_base_versions = std::vector<RowID>(30, NotExistRowID),
        .caller_line = __LINE__,
    });
    checkHandleIndex(20, 2); // 1 cf_big + 1 stable dmfile
}
} // namespace DB::DM::tests
