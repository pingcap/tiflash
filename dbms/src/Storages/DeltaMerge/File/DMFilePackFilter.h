#pragma once

#include <Encryption/ReadBufferFromFileProvider.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

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
    static DMFilePackFilter loadFrom(const DMFilePtr &           dmfile,
                                     const MinMaxIndexCachePtr & index_cache,
                                     UInt64                      hash_salt,
                                     const RowKeyRange &         rowkey_range,
                                     const RSOperatorPtr &       filter,
                                     const IdSetPtr &            read_packs,
                                     const FileProviderPtr &     file_provider)
    {
        auto pack_filter = DMFilePackFilter(dmfile, index_cache, hash_salt, rowkey_range, filter, read_packs, file_provider);
        pack_filter.init();
        return pack_filter;
    }

    const std::vector<RSResult> & getHandleRes() { return handle_res; }
    const std::vector<UInt8> &    getUsePacks() { return use_packs; }

    Handle getMinHandle(size_t pack_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(pack_id).first;
    }

    StringRef getMinStringHandle(size_t pack_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getStringMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(size_t pack_id)
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            tryLoadIndex(VERSION_COLUMN_ID);
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
    DMFilePackFilter(const DMFilePtr &           dmfile_,
                     const MinMaxIndexCachePtr & index_cache_,
                     UInt64                      hash_salt_,
                     const RowKeyRange &         rowkey_range_, // filter by handle range
                     const RSOperatorPtr &       filter_,       // filter by push down where clause
                     const IdSetPtr &            read_packs_,   // filter by pack index
                     const FileProviderPtr &     file_provider_)
        : dmfile(dmfile_),
          index_cache(index_cache_),
          hash_salt(hash_salt_),
          rowkey_range(rowkey_range_),
          filter(filter_),
          read_packs(read_packs_),
          file_provider(file_provider_),
          handle_res(dmfile->getPacks(), RSResult::All),
          use_packs(dmfile->getPacks()),
          log(&Logger::get("DMFilePackFilter"))
    {
    }

    void init()
    {
        size_t pack_count = dmfile->getPacks();
        if (!rowkey_range.all())
        {
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
            auto handle_filter = toFilter(rowkey_range);
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
                tryLoadIndex(attr.col_id);
            }

            for (size_t i = 0; i < pack_count; ++i)
            {
                use_packs[i] = ((bool)use_packs[i]) && (filter->roughCheck(i, param) != None);
            }
        }

        for (auto u : use_packs)
            after_filter += u;
        ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);

        Float64 filter_rate = static_cast<Float64>(after_read_packs - after_filter) * 100 / after_read_packs;
        LOG_DEBUG(log,
                  "RSFilter exclude rate: " << ((after_read_packs == 0) ? "nan" : DB::toString(filter_rate, 2))
                                            << ", after_pk: " << after_pk << ", after_read_packs: " << after_read_packs
                                            << ", after_filter: " << after_filter << ", handle_range: " << rowkey_range.toDebugString()
                                            << ", read_packs: " << ((!read_packs) ? 0 : read_packs->size())
                                            << ", pack_count: " << pack_count);
    }

    friend class DMFileReader;

private:
    static void loadIndex(ColumnIndexes &             indexes,
                          const DMFilePtr &           dmfile,
                          const FileProviderPtr &     file_provider,
                          const MinMaxIndexCachePtr & index_cache,
                          ColId                       col_id)
    {
        auto &     type           = dmfile->getColumnStat(col_id).type;
        const auto file_name_base = DMFile::getFileNameBase(col_id);

        auto load = [&]() {
            auto index_buf
                = ReadBufferFromFileProvider(file_provider,
                                             dmfile->colIndexPath(file_name_base),
                                             dmfile->encryptionIndexPath(file_name_base),
                                             std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), dmfile->colIndexSize(file_name_base)));
            index_buf.seek(dmfile->colIndexOffset(file_name_base));
            return MinMaxIndex::read(*type, index_buf, dmfile->colIndexSize(file_name_base));
        };
        MinMaxIndexPtr minmax_index;
        if (index_cache)
        {
            minmax_index = index_cache->getOrSet(dmfile->colIndexCacheKey(file_name_base), load);
        }
        else
        {
            minmax_index = load();
        }
        indexes.emplace(col_id, RSIndex(type, minmax_index));
    }

    void tryLoadIndex(const ColId col_id)
    {
        if (param.indexes.count(col_id))
            return;

        if (!dmfile->isColIndexExist(col_id))
            return;

        loadIndex(param.indexes, dmfile, file_provider, index_cache, col_id);
    }

private:
    DMFilePtr           dmfile;
    MinMaxIndexCachePtr index_cache;
    UInt64              hash_salt;
    RowKeyRange         rowkey_range;
    RSOperatorPtr       filter;
    IdSetPtr            read_packs;
    FileProviderPtr     file_provider;

    RSCheckParam param;

    std::vector<RSResult> handle_res;
    std::vector<UInt8>    use_packs;

    Logger * log;
};

} // namespace DM
} // namespace DB
