#pragma once

#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

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
                      const RSOperatorPtr & filter_,
                      const IdSetPtr &      read_chunks_)
        : dmfile(dmfile_),
          index_cache(index_cache_),
          hash_salt(hash_salt_),
          filter(filter_),
          read_chunks(read_chunks_),
          use_chunks(dmfile_->getChunks(), 1)
    {
        if (filter || read_chunks)
        {
            RSCheckParam param;

            if (filter)
            {
                // Load index based on filter.

                loadIndex(param, EXTRA_HANDLE_COLUMN_ID);
            }

            for (size_t i = 0; i < dmfile->getChunks(); ++i)
            {
                bool use = true;
                if (filter)
                    use &= filter->roughCheck(i, param) != None;
                if (read_chunks)
                    use &= read_chunks->count(i);

                if (!use)
                    use_chunks[i] = 0;
            }
        }
    }

    void loadIndex(RSCheckParam & param, const ColId col_id)
    {
        auto & type = dmfile->getColumnStat(col_id).type;
        auto   load = [&]() {
            auto index_buf = openForRead(dmfile->colIndexPath(col_id));
            return MinMaxIndex::read(*type, index_buf);
        };
        MinMaxIndexPtr minmax_index;
        if (index_cache)
        {
            auto key     = MinMaxIndexCache::hash(dmfile->colIndexPath(col_id), hash_salt);
            minmax_index = index_cache->getOrSet(key, load);
        }
        else
        {
            minmax_index = load();
        }

        param.indexes.emplace(col_id, RSIndex(type, minmax_index));
    }

    const std::vector<UInt8> & getUseChunks() { return use_chunks; }

    size_t validRows()
    {
        size_t rows        = 0;
        auto & chunk_split = dmfile->getSplit();
        for (size_t i = 0; i < chunk_split.size(); ++i)
            rows += use_chunks[i] ? chunk_split[i] : 0;
        return rows;
    }

private:
    DMFilePtr          dmfile;
    MinMaxIndexCache * index_cache;
    UInt64             hash_salt;
    RSOperatorPtr      filter;
    IdSetPtr           read_chunks;

    std::vector<UInt8> use_chunks;
};

} // namespace DM
} // namespace DB