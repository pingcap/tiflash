#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>
#include <gtest/gtest.h>

#include "Storages/DeltaMerge/Remote/RemotePageInputStream.h"

namespace DB::DM::tests
{
class StoreDisaggReadTest : public SimplePKTestBasic
{
public:
};

TEST_F(StoreDisaggReadTest, A)
{
    fill(0, 100);
    flush(0, 100);

    RowKeyRanges read_ranges{};
    size_t num_streams = 1;
    String tracing_id = "disagg_read";
    auto table_snap = store->buildRemoteReadSnapshot(
        *db_context,
        db_context->getSettingsRef(),
        read_ranges,
        num_streams,
        tracing_id);
    for (auto [seg_key, seg] : store->segments)
    {
        auto seg_task = table_snap->popTask(seg->segmentId());
        auto stream = Remote::RemotePageInputStream(seg_task, DEFAULT_BLOCK_SIZE);
        // stream.readCFTinyPage(PageIdU64 page_id);
        stream.readMemTableBlock();
    }
}

} // namespace DB::DM::tests
