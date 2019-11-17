#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class ChunkBlockInputStream final : public SkippableBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks &        chunks_,
                          const ColumnDefines & read_columns_,
                          const PageReader &    page_reader_,
                          const RSOperatorPtr & filter)
        : chunks(std::move(chunks_)), use_chunks(chunks.size(), 1), read_columns(read_columns_), page_reader(page_reader_)
    {
        if (filter)
        {
            for (size_t i = 0; i < chunks.size(); ++i)
            {
                auto &       chunk = chunks[i];
                RSCheckParam param;
                for (auto & [col_id, meta] : chunk.getMetas())
                {
                    if (col_id == EXTRA_HANDLE_COLUMN_ID)
                        param.indexes.emplace(col_id, RSIndex(meta.type, meta.minmax));
                }
                use_chunks[i] = filter->roughCheck(0, param) != None;
            }
        }
    }

    ~ChunkBlockInputStream()
    {
        size_t num_use = 0;
        for (const auto & use : use_chunks)
            num_use += use;

        LOG_TRACE(&Logger::get("ChunkBlockInputStream"),
                  String("Skip: ") << (chunks.size() - num_use) << " / " << chunks.size() << " chunks");
    }

    String getName() const override { return "Chunk"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    bool getSkippedRows(size_t & skip_rows) override
    {
        skip_rows = 0;
        for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id)
        {
            skip_rows += chunks[next_chunk_id].getRows();
        }

        return next_chunk_id < use_chunks.size();
    }

    Block read() override
    {
        for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id) {}

        if (next_chunk_id >= use_chunks.size())
            return {};
        return readChunk(chunks[next_chunk_id++], read_columns, page_reader);
    }

private:
    Chunks             chunks;
    std::vector<UInt8> use_chunks;

    ColumnDefines read_columns;
    PageReader    page_reader;

    size_t next_chunk_id = 0;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB