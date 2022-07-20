#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>

namespace DB::DM::tests
{
BlockInputStreamPtr StoreInputStreamBuilder::build()
{
    // Query full range by default
    RowKeyRanges actual_query_key_ranges;
    if (!key_ranges)
        actual_query_key_ranges = RowKeyRanges{RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())};
    else
        actual_query_key_ranges = *key_ranges;

    auto streams = store->read(
        query_context,
        query_context.getSettingsRef(),
        column_defines,
        actual_query_key_ranges,
        /*num_streams=*/1,
        read_tso,
        rough_set_filter,
        /*tracing_id=*/"",
        /*is_fast_mode=*/is_fast_mode,
        /*expected_block_size=*/DEFAULT_BLOCK_SIZE,
        /*read_segments=*/SegmentIdSet{},
        /*extra_table_id_index=*/InvalidColumnID);
    RUNTIME_CHECK(streams.size() == 1, DB::Exception, fmt::format("The generate streams size is {} != 1 !", streams.size()));
    return streams[0];
}

} // namespace DB::DM::tests
