#pragma once

#include <Encryption/ReadBufferFromFileProvider.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace ProfileEvents
{
extern const Event DMFileFilterNoFilter;
extern const Event DMFileFilterAftPKAndPackSet;
extern const Event DMFileFilterAftRoughSet;
} // namespace ProfileEvents

namespace DB
{
namespace DM
{

using IdSet    = std::set<UInt64>;
using IdSetPtr = std::shared_ptr<IdSet>;

class DMFilePackFilter
{
public:
    ///
    DMFilePackFilter(const DMFilePtr &       dmfile_,
                     MinMaxIndexCache *      index_cache_,
                     const HandleRange &     handle_range_, // filter by handle range
                     const RSOperatorPtr &   filter_,       // filter by push down where clause
                     const IdSetPtr &        read_packs_,   // filter by pack index
                     const FileProviderPtr & file_provider_)
        : dmfile(dmfile_),
          index_cache(index_cache_),
          handle_range(handle_range_),
          filter(filter_),
          read_packs(read_packs_),
          file_provider(file_provider_),
          handle_res(dmfile->getPacks(), RSResult::All),
          use_packs(dmfile->getPacks()),
          log(&Logger::get("DMFilePackFilter"))
    {

        size_t pack_count = dmfile->getPacks();
        if (!handle_range.all())
        {
            loadIndex(EXTRA_HANDLE_COLUMN_ID);
            auto handle_filter = toFilter(handle_range);
            for (size_t i = 0; i < pack_count; ++i)
            {
                handle_res[i] = handle_filter->roughCheck(i, param);
            }
        }

        ProfileEvents::increment(ProfileEvents::DMFileFilterNoFilter, pack_count);

        size_t after_pk         = 0;
        size_t after_read_packs = 0;
        size_t after_filter     = 0;

        /// Check packs by handle_res
        for (size_t i = 0; i < pack_count; ++i)
        {
            use_packs[i] = handle_res[i] != None;
        }

        for (auto u : use_packs)
            after_pk += u;

        /// Check packs by read_packs
        if (read_packs)
        {
            for (size_t i = 0; i < pack_count; ++i)
            {
                use_packs[i] = ((bool)use_packs[i]) && ((bool)read_packs->count(i));
            }
        }

        for (auto u : use_packs)
            after_read_packs += u;
        ProfileEvents::increment(ProfileEvents::DMFileFilterAftPKAndPackSet, after_read_packs);


        /// Check packs by filter in where clause
        if (filter)
        {
            // Load index based on filter.
            Attrs attrs = filter->getAttrs();
            for (auto & attr : attrs)
            {
                loadIndex(attr.col_id);
            }

            for (size_t i = 0; i < pack_count; ++i)
            {
                use_packs[i] = ((bool)use_packs[i]) && (filter->roughCheck(i, param) != None);
            }
        }

        for (auto u : use_packs)
            after_filter += u;
        ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);

        Float64 filter_rate = (Float64)(after_read_packs - after_filter) * 100 / after_read_packs;
        if (isnan(filter_rate))
        {
            LOG_DEBUG(log,
                      "RSFilter exclude rate is nan, after_pk: "
                          << after_pk << ", after_read_packs: " << after_read_packs << ", after_filter: " << after_filter
                          << ", handle_range: " << handle_range.toDebugString()
                          << ", read_packs: " << ((!read_packs) ? 0 : read_packs->size()) << ", pack_count: " << pack_count);
        }
        else
        {
            LOG_DEBUG(log, "RSFilter exclude rate: " << DB::toString(filter_rate, 2));
        }
    }

    const std::vector<RSResult> & getHandleRes() { return handle_res; }
    const std::vector<UInt8> &    getUsePacks() { return use_packs; }

    Handle getMinHandle(size_t pack_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            loadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(size_t pack_id)
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            loadIndex(VERSION_COLUMN_ID);
        auto & minmax_index = param.indexes.find(VERSION_COLUMN_ID)->second.minmax;
        return minmax_index->getUInt64MinMax(pack_id).second;
    }

    // Get valid rows and bytes after filter invalid packs by handle_range and filter
    std::pair<size_t, size_t> validRowsAndBytes()
    {
        size_t rows       = 0;
        size_t bytes      = 0;
        auto & pack_stats = dmfile->getPackStats();
        for (size_t i = 0; i < pack_stats.size(); ++i)
        {
            if (use_packs[i])
            {
                rows += pack_stats[i].rows;
                bytes += pack_stats[i].bytes;
            }
        }
        return {rows, bytes};
    }

private:
    void loadIndex(const ColId col_id)
    {
        if (param.indexes.count(col_id))
            return;

        auto       index_path = dmfile->colIndexPath(DMFile::getFileNameBase(col_id));
        Poco::File index_file(index_path);
        if (!index_file.exists())
            return;

        auto & type = dmfile->getColumnStat(col_id).type;
        auto   load = [&]() {
            auto index_buf = ReadBufferFromFileProvider(
                file_provider,
                index_path,
                dmfile->encryptionIndexPath(DMFile::getFileNameBase(col_id)),
                std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));
            return MinMaxIndex::read(*type, index_buf);
        };
        MinMaxIndexPtr minmax_index;
        if (index_cache)
        {
            minmax_index = index_cache->getOrSet(index_path, load);
        }
        else
        {
            minmax_index = load();
        }

        param.indexes.emplace(col_id, RSIndex(type, minmax_index));
    }

private:
    DMFilePtr          dmfile;
    MinMaxIndexCache * index_cache;
    HandleRange        handle_range;
    RSOperatorPtr      filter;
    IdSetPtr           read_packs;
    FileProviderPtr    file_provider;

    RSCheckParam param;

    std::vector<RSResult> handle_res;
    std::vector<UInt8>    use_packs;

    Logger * log;
};

} // namespace DM
} // namespace DB
