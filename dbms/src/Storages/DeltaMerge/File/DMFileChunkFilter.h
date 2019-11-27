#pragma once

#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterHelper.h>

namespace DB
{
namespace DM
{

using IdSet    = std::set<UInt64>;
using IdSetPtr = std::shared_ptr<IdSet>;

class DMFileChunkFilter
{
public:
    DMFileChunkFilter(const DMFilePtr &     dmfile_,
                      MinMaxIndexCache *    index_cache_,
                      UInt64                hash_salt_,
                      const HandleRange &   handle_range_,
                      const RSOperatorPtr & filter_,
                      const IdSetPtr &      read_chunks_)
        : dmfile(dmfile_),
          index_cache(index_cache_),
          hash_salt(hash_salt_),
          handle_range(handle_range_),
          filter(filter_),
          read_chunks(read_chunks_),
          handle_res(dmfile->getChunks(), RSResult::All),
          use_chunks(dmfile->getChunks())
    {

        if (!handle_range.all())
        {
            loadIndex(EXTRA_HANDLE_COLUMN_ID);
            auto handle_filter = toFilter(handle_range);
            for (size_t i = 0; i < dmfile->getChunks(); ++i)
            {
                handle_res[i] = handle_filter->roughCheck(i, param);
            }
        }

        if (filter || read_chunks)
        {
            if (filter)
            {
                // Load index based on filter.
                Attrs attrs = filter->getAttrs();
                for (auto & attr : attrs)
                {
                    loadIndex(attr.col_id);
                }
            }

            for (size_t i = 0; i < dmfile->getChunks(); ++i)
            {
                bool use = handle_res[i] != None;
                if (filter && use)
                    use &= filter->roughCheck(i, param) != None;
                if (read_chunks && use)
                    use &= read_chunks->count(i);
                use_chunks[i] = use;
            }
        }
        else
        {
            for (size_t i = 0; i < dmfile->getChunks(); ++i)
            {
                use_chunks[i] = handle_res[i] != None;
            }
        }
    }

    void loadIndex(const ColId col_id)
    {
        if (param.indexes.count(col_id))
            return;

        auto       index_path = dmfile->colIndexPath(col_id);
        Poco::File index_file(index_path);
        if (!index_file.exists())
            return;

        auto & type = dmfile->getColumnStat(col_id).type;
        auto   load = [&]() {
            auto index_buf = openForRead(index_path);
            return MinMaxIndex::read(*type, index_buf);
        };
        MinMaxIndexPtr minmax_index;
        if (index_cache)
        {
            auto key     = MinMaxIndexCache::hash(index_path, hash_salt);
            minmax_index = index_cache->getOrSet(key, load);
        }
        else
        {
            minmax_index = load();
        }

        param.indexes.emplace(col_id, RSIndex(type, minmax_index));
    }

    const std::vector<RSResult> & getHandleRes() { return handle_res; }
    const std::vector<UInt8> &    getUseChunks() { return use_chunks; }

    Handle getMinHandle(size_t chunk_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            loadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(chunk_id).first;
    }

    UInt64 getMaxVersion(size_t chunk_id)
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            loadIndex(VERSION_COLUMN_ID);
        auto & minmax_index = param.indexes.find(VERSION_COLUMN_ID)->second.minmax;
        return minmax_index->getUInt64MinMax(chunk_id).second;
    }

    size_t validRows()
    {
        size_t rows        = 0;
        auto & chunk_stats = dmfile->getChunkStats();
        for (size_t i = 0; i < chunk_stats.size(); ++i)
            rows += use_chunks[i] ? chunk_stats[i].rows : 0;
        return rows;
    }

private:
    DMFilePtr          dmfile;
    MinMaxIndexCache * index_cache;
    UInt64             hash_salt;
    HandleRange        handle_range;
    RSOperatorPtr      filter;
    IdSetPtr           read_chunks;

    RSCheckParam param;

    std::vector<RSResult> handle_res;
    std::vector<UInt8>    use_chunks;
};

} // namespace DM
} // namespace DB