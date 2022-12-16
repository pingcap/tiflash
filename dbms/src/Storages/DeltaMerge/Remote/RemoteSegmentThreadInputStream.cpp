#include <Storages/DeltaMerge/Remote/RemoteSegmentThreadInputStream.h>

namespace DB::DM
{
BlockInputStreams RemoteSegmentThreadInputStream::buildInputStreams(
    const Context & db_context,
    const RemoteReadTaskPtr & remote_read_tasks,
    UInt64 read_tso,
    size_t num_streams,
    size_t extra_table_id_index,
    std::string_view extra_info,
    std::string_view tracing_id,
    size_t expected_block_size)
{
    DM::RSOperatorPtr rs_filter = {};
    DM::ColumnDefines columns_to_read;
    TableID physical_table_id = -1;

    BlockInputStreams streams;
    for (size_t i = 0; i < num_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DM::RemoteSegmentThreadInputStream>(
            db_context,
            remote_read_tasks,
            columns_to_read,
            rs_filter,
            read_tso,
            expected_block_size,
            DM::ReadMode::Normal,
            extra_table_id_index,
            physical_table_id,
            tracing_id);
        stream->setExtraInfo(String(extra_info));
        streams.emplace_back(std::move(stream));
    }
    return streams;
}
} // namespace DB::DM
