#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Storages/DeltaMerge/Pack.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class PackBlockInputStream final : public SkippableBlockInputStream
{
public:
    PackBlockInputStream(const Packs &        packs_,
                          const ColumnDefines & read_columns_,
                          const PageReader &    page_reader_,
                          const RSOperatorPtr & filter)
        : packs(packs_), use_packs(packs.size(), 1), read_columns(read_columns_), page_reader(page_reader_)
    {
        if (filter)
        {
            //            for (size_t i = 0; i < packs.size(); ++i)
            //            {
            //                auto &       pack = packs[i];
            //                RSCheckParam param;
            //                for (auto & [col_id, meta] : pack.getMetas())
            //                {
            //                    if (col_id == EXTRA_HANDLE_COLUMN_ID)
            //                        param.indexes.emplace(col_id, RSIndex(meta.type, meta.minmax));
            //                }
            //                use_packs[i] = filter->roughCheck(0, param) != None;
            //            }
        }
    }

    ~PackBlockInputStream()
    {
        size_t num_use = 0;
        for (const auto & use : use_packs)
            num_use += use;

        LOG_TRACE(&Logger::get("PackBlockInputStream"),
                  String("Skip: ") << (packs.size() - num_use) << " / " << packs.size() << " packs");
    }

    String getName() const override { return "Pack"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    bool getSkippedRows(size_t & skip_rows) override
    {
        skip_rows = 0;
        for (; next_pack_id < use_packs.size() && !use_packs[next_pack_id]; ++next_pack_id)
        {
            skip_rows += packs[next_pack_id].getRows();
        }

        return next_pack_id < use_packs.size();
    }

    Block read() override
    {
        for (; next_pack_id < use_packs.size() && !use_packs[next_pack_id]; ++next_pack_id) {}

        if (next_pack_id >= use_packs.size())
            return {};
        return readPack(packs[next_pack_id++], read_columns, page_reader);
    }

private:
    Packs             packs;
    std::vector<UInt8> use_packs;

    ColumnDefines read_columns;
    PageReader    page_reader;

    size_t next_pack_id = 0;
};

using PackBlockInputStreamPtr = std::shared_ptr<PackBlockInputStream>;

} // namespace DM
} // namespace DB