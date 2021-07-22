#include <boost/rational.hpp> /// For calculations related to sampling coefficients.

/// Allow to use __uint128_t as a template parameter for boost::rational.
// https://stackoverflow.com/questions/41198673/uint128-t-not-working-with-clang-and-libstdc
#if !defined(__GLIBCXX_BITSIZE_INT_N_0) && defined(__SIZEOF_INT128__)
namespace std
{
template <>
struct numeric_limits<__uint128_t>
{
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = false;
    static constexpr bool is_integer = true;
    static constexpr int radix = 2;
    static constexpr int digits = 128;
    static constexpr __uint128_t min() { return 0; }               // used in boost 1.65.1+
    static constexpr __uint128_t max() { return __uint128_t(-1); } // used in boost 1.65.1+
};
} // namespace std
#endif

#include <Common/FieldVisitors.h>
#include <Common/TiFlashMetrics.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/AggregatingSortedBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/CollapsingFinalBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/DebugPrintBlockInputStream.h>
#include <DataStreams/DedupSortedBlockInputStream.h>
#include <DataStreams/DeletingDeletedBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/ReplacingDeletingSortedBlockInputStream.h>
#include <DataStreams/ReplacingSortedBlockInputStream.h>
#include <DataStreams/SummingSortedBlockInputStream.h>
#include <DataStreams/TMTSingleSortedBlockInputStream.h>
#include <DataStreams/TMTSortedBlockInputStream.h>
#include <DataStreams/TMTUnionBlockInputStream.h>
#include <DataStreams/VersionFilterBlockInputStream.h>
#include <DataStreams/VersionedCollapsingSortedBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSampleRatio.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadBlockInputStream.h>
#include <Storages/MergeTree/TMTDataPartProperty.h>
#include <Storages/MutableSupport.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Utils.h>
#include <Storages/VirtualColumnUtils.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutorCommon.hpp>

namespace ProfileEvents
{
extern const Event SelectedParts;
extern const Event SelectedRanges;
extern const Event SelectedMarks;
} // namespace ProfileEvents


namespace DB
{

namespace ErrorCodes
{
extern const int INDEX_NOT_USED;
extern const int SAMPLING_NOT_SUPPORTED;
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int ILLEGAL_COLUMN;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int VERSION_ERROR;
extern const int REGION_MISS;
} // namespace ErrorCodes


MergeTreeDataSelectExecutor::MergeTreeDataSelectExecutor(MergeTreeData & data_)
    : data(data_), log(&Logger::get(data.getLogName() + " (SelectExecutor)"))
{}


/// Construct a block consisting only of possible values of virtual columns
static Block getBlockWithPartColumn(const MergeTreeData::DataPartsVector & parts)
{
    auto column = ColumnString::create();

    for (const auto & part : parts)
        column->insert(part->name);

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "_part")};
}


size_t MergeTreeDataSelectExecutor::getApproximateTotalRowsToRead(
    const MergeTreeData::DataPartsVector & parts, const KeyCondition & key_condition, const Settings & settings) const
{
    size_t full_marks_count = 0;

    /// We will find out how many rows we would have read without sampling.
    LOG_DEBUG(log, "Preliminary index scan with condition: " << key_condition.toString());

    for (size_t i = 0; i < parts.size(); ++i)
    {
        const MergeTreeData::DataPartPtr & part = parts[i];
        MarkRanges ranges = markRangesFromPKRange(part->index, key_condition, settings);

        /** In order to get a lower bound on the number of rows that match the condition on PK,
          *  consider only guaranteed full marks.
          * That is, do not take into account the first and last marks, which may be incomplete.
          */
        for (size_t j = 0; j < ranges.size(); ++j)
            if (ranges[j].end - ranges[j].begin > 2)
                full_marks_count += ranges[j].end - ranges[j].begin - 2;
    }

    return full_marks_count * data.index_granularity;
}


using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

std::string toString(const RelativeSize & x)
{
    return ASTSampleRatio::toString(x.numerator()) + "/" + ASTSampleRatio::toString(x.denominator());
}

/// Converts sample size to an approximate number of rows (ex. `SAMPLE 1000000`) to relative value (ex. `SAMPLE 0.1`).
static RelativeSize convertAbsoluteSampleSizeToRelative(const ASTPtr & node, size_t approx_total_rows)
{
    if (approx_total_rows == 0)
        return 1;

    const ASTSampleRatio & node_sample = typeid_cast<const ASTSampleRatio &>(*node);

    auto absolute_sample_size = node_sample.ratio.numerator / node_sample.ratio.denominator;
    return std::min(RelativeSize(1), RelativeSize(absolute_sample_size) / RelativeSize(approx_total_rows));
}

TMTPKType getTMTPKType(const IDataType & rhs)
{
    static const DataTypeInt64 & dataTypeInt64 = {};
    static const DataTypeUInt64 & dataTypeUInt64 = {};
    static const DataTypeString & dataTypeString = {};

    if (rhs.equals(dataTypeInt64))
        return TMTPKType::INT64;
    else if (rhs.equals(dataTypeUInt64))
        return TMTPKType::UINT64;
    else if (rhs.equals(dataTypeString))
        return TMTPKType::STRING;
    return TMTPKType::UNSPECIFIED;
}

struct RegionExecutorData
{
    RegionQueryInfo info;
    HandleRange<HandleID> handle_range;
    Block block;
    RegionScanFilterPtr range_scan_filter;

    RegionExecutorData() = default;
    RegionExecutorData(const RegionQueryInfo & info_, HandleRange<HandleID> && handle_range_)
        : info(info_), handle_range(std::move(handle_range_))
    {}
    RegionExecutorData & operator=(RegionExecutorData && data)
    {
        if (&data == this)
            return *this;
        info = std::move(data.info);
        handle_range = std::move(data.handle_range);
        block = std::move(data.block);
        range_scan_filter = std::move(data.range_scan_filter);
        return *this;
    }
    RegionExecutorData(const RegionExecutorData &) = delete;
    RegionExecutorData(RegionExecutorData && data)
        : info(std::move(data.info)),
          handle_range(std::move(data.handle_range)),
          block(std::move(data.block)),
          range_scan_filter(std::move(data.range_scan_filter))
    {}
    bool operator<(const RegionExecutorData & o) const { return info < o.info; }
};

template <typename HandleType>
bool isFullRegionScan(
    std::vector<HandleRange<HandleType>> & dag_required_handle_ranges, std::vector<HandleRange<HandleType>> & region_ranges)
{
    std::sort(dag_required_handle_ranges.begin(), dag_required_handle_ranges.end(),
        [](const HandleRange<HandleType> & a, const HandleRange<HandleType> & b) { return a.first < b.first; });
    bool need_merge = false;
    for (size_t i = 0; i < dag_required_handle_ranges.size() - 1; i++)
        need_merge |= dag_required_handle_ranges[i].second >= dag_required_handle_ranges[i + 1].first;
    if (need_merge)
    {
        // merge dag_required_handle_ranges
        size_t new_size = 0;
        HandleRange<HandleType> current_range;
        current_range.first = dag_required_handle_ranges[0].first;
        current_range.second = dag_required_handle_ranges[0].second;
        for (size_t i = 1; i < dag_required_handle_ranges.size(); i++)
        {
            if (current_range.second >= dag_required_handle_ranges[i].first)
                current_range.second = current_range.second >= dag_required_handle_ranges[i].second ? current_range.second
                                                                                                    : dag_required_handle_ranges[i].second;
            else
            {
                if (current_range.second > current_range.first)
                    dag_required_handle_ranges[new_size++] = current_range;
                current_range.first = dag_required_handle_ranges[i].first;
                current_range.second = dag_required_handle_ranges[i].second;
            }
        }
        if (current_range.second > current_range.first)
            dag_required_handle_ranges[new_size++] = current_range;
        dag_required_handle_ranges.resize(new_size);
    }

    bool ret = true;
    for (const auto & region_range : region_ranges)
    {
        bool covered = false;
        for (const auto & range : dag_required_handle_ranges)
        {
            if (region_range.first >= range.first && region_range.second <= range.second)
            {
                covered = true;
                break;
            }
        }
        if (!covered && region_range.second > region_range.first)
        {
            ret = false;
            break;
        }
    }
    return ret;
}

void checkAndSetRangeScanFilter(TMTPKType pk_type, std::vector<RegionExecutorData> & regions, TableID table_id)
{
    for (auto & r : regions)
    {
        if (r.info.required_handle_ranges.empty())
        {
            r.range_scan_filter
                = std::make_shared<RegionScanFilter>(true, (std::vector<HandleRange<Int64>>){}, (std::vector<HandleRange<UInt64>>){});
            continue;
        }
        if (pk_type == TMTPKType::UINT64)
        {
            std::vector<HandleRange<UInt64>> dag_handle_ranges;
            for (const auto & dag_range : r.info.required_handle_ranges)
            {
                const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(getHandleRangeByTable(dag_range, table_id));
                for (int i = 0; i < n; i++)
                    dag_handle_ranges.push_back(new_range[i]);
            }
            std::vector<HandleRange<UInt64>> region_ranges;
            // range_in_table should be initialized before calling this
            const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(r.handle_range);
            region_ranges.reserve(n);
            for (int i = 0; i < n; i++)
                region_ranges.push_back(new_range[i]);
            if (isFullRegionScan<UInt64>(dag_handle_ranges, region_ranges))
                r.range_scan_filter
                    = std::make_shared<RegionScanFilter>(true, (std::vector<HandleRange<Int64>>){}, (std::vector<HandleRange<UInt64>>){});
            else
                r.range_scan_filter = std::make_shared<RegionScanFilter>(false, (std::vector<HandleRange<Int64>>){}, dag_handle_ranges);
        }
        else
        {
            std::vector<HandleRange<Int64>> region_ranges;
            // range_in_table should be initialized before calling this
            region_ranges.push_back(r.handle_range);
            std::vector<HandleRange<Int64>> dag_handle_ranges;
            for (const auto & dag_range : r.info.required_handle_ranges)
            {
                dag_handle_ranges.push_back(getHandleRangeByTable(dag_range, table_id));
            }
            if (isFullRegionScan<Int64>(dag_handle_ranges, region_ranges))
                r.range_scan_filter
                    = std::make_shared<RegionScanFilter>(true, (std::vector<HandleRange<Int64>>){}, (std::vector<HandleRange<UInt64>>){});
            else
                r.range_scan_filter = std::make_shared<RegionScanFilter>(false, dag_handle_ranges, (std::vector<HandleRange<UInt64>>){});
        }
    }
}

BlockInputStreams MergeTreeDataSelectExecutor::read(const Names & column_names_to_return,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams,
    Int64 max_block_number_to_read) const
{
    RangesInDataParts parts_with_ranges;

    const bool is_txn_engine = data.merging_params.mode == MergeTreeData::MergingParams::Txn;

    if (query_info.mvcc_query_info == nullptr)
        throw Exception("mvcc query info is null", ErrorCodes::LOGICAL_ERROR);

    const auto & mvcc_query_info = *query_info.mvcc_query_info;

    size_t concurrent_num = std::max<size_t>(num_streams * mvcc_query_info.concurrent, 1);

    std::vector<RegionExecutorData> regions_executor_data;
    {
        regions_executor_data.reserve(mvcc_query_info.regions_query_info.size());
        for (const auto & query : mvcc_query_info.regions_query_info)
        {
            HandleRange<HandleID> handle_range = getHandleRangeByTable(query.range_in_table, data.table_info->id);
            regions_executor_data.emplace_back(query, std::move(handle_range));
        }
    }
    ssize_t special_region_index = -1;
    RegionMap kvstore_region;
    const std::string handle_col_name = is_txn_engine ? data.getPrimarySortDescription()[0].column_name : "";
    size_t region_cnt = 0;
    std::vector<std::vector<RangesInDataParts>> region_group_range_parts;
    std::vector<std::vector<ReadGroup>> region_group_read_groups;

    std::vector<std::vector<HandleRange<HandleID>>> region_group_handle_ranges;
    std::vector<std::vector<HandleRange<UInt64>>> region_group_u64_handle_ranges;

    Names tmt_column_names_to_read;

    // pk, version, delmark is always the first 3 columns.
    // the index of column is constant after MergeTreeBlockInputStream is constructed. exception will be thrown if not found.
    const size_t handle_column_index = 0, version_column_index = 1, delmark_column_index = 2;

    const auto func_throw_retry_region = [&](RegionException::RegionReadStatus status) {
        RegionException::UnavailableRegions region_ids(regions_executor_data.size());
        std::for_each(regions_executor_data.begin(), regions_executor_data.end(), [&](auto & x) { region_ids.emplace(x.info.region_id); });
        throw RegionException(std::move(region_ids), status);
    };

    /// If query contains restrictions on the virtual column `_part` or `_part_index`, select only parts suitable for it.
    /// The virtual column `_sample_factor` (which is equal to 1 / used sample rate) can be requested in the query.
    Names virt_column_names;
    Names real_column_names;

    bool part_column_queried = false;

    bool sample_factor_column_queried = false;
    Float64 used_sample_factor = 1;

    for (const String & name : column_names_to_return)
    {
        if (name == "_part")
        {
            part_column_queried = true;
            virt_column_names.push_back(name);
        }
        else if (name == "_part_index")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_sample_factor")
        {
            sample_factor_column_queried = true;
            virt_column_names.push_back(name);
        }
        else
        {
            real_column_names.push_back(name);
        }
    }

    ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*query_info.query);

    TMTPKType pk_type = TMTPKType::UNSPECIFIED;

    if (is_txn_engine)
    {
        tmt_column_names_to_read = real_column_names;

        // must extend mutable engine column.
        extendMutableEngineColumnNames(tmt_column_names_to_read, handle_col_name);

        if (tmt_column_names_to_read.size() < 3)
            throw Exception("size of tmt_column_names_to_read < 3", ErrorCodes::LOGICAL_ERROR);

        if (tmt_column_names_to_read[handle_column_index] != handle_col_name
            || tmt_column_names_to_read[version_column_index] != MutableSupport::version_column_name
            || tmt_column_names_to_read[delmark_column_index] != MutableSupport::delmark_column_name)
            throw Exception("Wrong column order for txn engine, should not happen", ErrorCodes::LOGICAL_ERROR);
    }

    if (!is_txn_engine)
        ;
    else if (!select.no_kvstore)
    {
        pk_type = getTMTPKType(*data.primary_key_data_types[0]);

        TMTContext & tmt = context.getTMTContext();
        auto & kvstore = tmt.getKVStore();

        if (!tmt.isInitialized())
            throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

        // only for test, because regions_executor_data should never be empty if query is from tidb or tispark.
        if (regions_executor_data.empty())
        {
            auto regions = tmt.getRegionTable().getRegionsByTable(data.table_info->id);
            regions_executor_data.reserve(regions.size());
            for (const auto & [id, region] : regions)
            {
                regions_executor_data.emplace_back(
                    RegionQueryInfo{id, region->version(), region->confVer(), region->getRange()->rawKeys(), {}},
                    getHandleRangeByTable(region->getRange()->rawKeys(), data.table_info->id));
            }
        }

        // check region is not null and store region map.
        for (const auto & query_info : regions_executor_data)
        {
            auto region = kvstore->getRegion(query_info.info.region_id);
            if (region == nullptr)
            {
                LOG_WARNING(log, "[region " << query_info.info.region_id << "] is not found in KVStore, try again");
                func_throw_retry_region(RegionException::RegionReadStatus::NOT_FOUND);
            }
            kvstore_region.emplace(query_info.info.region_id, std::move(region));
        }

        // make sure regions are not duplicated.
        if (kvstore_region.size() != regions_executor_data.size())
            throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);

        checkAndSetRangeScanFilter(pk_type, regions_executor_data, data.table_info->id);

        { // learner read

            concurrent_num = std::max(1, std::min(concurrent_num, regions_executor_data.size()));

            // get data block from region first.
            auto start_time = Clock::now();
            const size_t mem_region_num = regions_executor_data.size();
            const size_t batch_size = mem_region_num / concurrent_num;
            std::atomic<RegionException::RegionReadStatus> region_status = RegionException::RegionReadStatus::OK;

            const auto func_run_learner_read = [&](const size_t region_begin) {
                const size_t region_end = std::min(region_begin + batch_size, mem_region_num);
                for (size_t region_index = region_begin; region_index < region_end; ++region_index)
                {
                    if (region_status != RegionException::RegionReadStatus::OK)
                        return;

                    RegionQueryInfo & region_query_info = regions_executor_data[region_index].info;
                    HandleRange<HandleID> & handle_range = regions_executor_data[region_index].handle_range;
                    // wait learner read index
                    auto region = kvstore_region[region_query_info.region_id];

                    if (region->getMappedTableID() != data.table_info->id)
                        throw Exception(std::string(__PRETTY_FUNCTION__) + ": table id not match, expect "
                                + std::to_string(region->getMappedTableID()) + ", got " + std::to_string(data.table_info->id),
                            ErrorCodes::LOGICAL_ERROR);

                    GET_METRIC(const_cast<Context &>(context).getTiFlashMetrics(), tiflash_raft_read_index_count).Increment();
                    Stopwatch read_index_watch;

                    /// Blocking learner read. Note that learner read must be performed ahead of data read,
                    /// otherwise the desired index will be blocked by the lock of data read.
                    auto read_index_result = region->learnerRead(mvcc_query_info.read_tso);
                    GET_METRIC(const_cast<Context &>(context).getTiFlashMetrics(), tiflash_raft_read_index_duration_seconds)
                        .Observe(read_index_watch.elapsedSeconds());

                    switch (read_index_result.status)
                    {
                        case RegionException::RegionReadStatus::NOT_FOUND:
                        case RegionException::RegionReadStatus::EPOCH_NOT_MATCH:
                        {
                            region_status = read_index_result.status;
                            continue;
                        }
                        case RegionException::RegionReadStatus::OK:
                        {
                            break;
                        }
                    }

                    if (read_index_result.lock_info)
                    {
                        throw LockException(region->id(), std::move(read_index_result.lock_info));
                        continue;
                    }
                    else
                    {
                        Stopwatch wait_index_watch;
                        if (region->waitIndex(read_index_result.read_index, tmt))
                        {
                            region_status = RegionException::RegionReadStatus::NOT_FOUND;
                            continue;
                        }
                        GET_METRIC(const_cast<Context &>(context).getTiFlashMetrics(), tiflash_raft_wait_index_duration_seconds)
                            .Observe(wait_index_watch.elapsedSeconds());
                    }

                    auto block_status = RegionTable::readBlockByRegion(*data.table_info, data.getColumns(), tmt_column_names_to_read,
                        kvstore_region[region_query_info.region_id], region_query_info.version, region_query_info.conf_version,
                        mvcc_query_info.resolve_locks, mvcc_query_info.read_tso, region_query_info.bypass_lock_ts,
                        regions_executor_data[region_index].range_scan_filter);

                    std::visit(variant_op::overloaded{
                                   [&](Block & block) {
                                       if (block.rows())
                                           regions_executor_data[region_index].block = std::move(block);
                                   },
                                   [&](RegionException::RegionReadStatus & status) {
                                       assert(status != RegionException::RegionReadStatus::OK);
                                       LOG_WARNING(log,
                                           "Check memory cache, region " << region_query_info.region_id << ", version "
                                                                         << region_query_info.version << ", handle range ["
                                                                         << handle_range.first.toString() << ", "
                                                                         << handle_range.second.toString() << ") , status "
                                                                         << RegionException::RegionReadStatusString(status));
                                       region_status = status;
                                   },
                               },
                        block_status);
                }
            };

            if (concurrent_num > 1)
            {
                ThreadPool pool(concurrent_num);
                for (size_t region_begin = 0; region_begin < mem_region_num; region_begin += batch_size)
                    pool.schedule([&, region_begin] { func_run_learner_read(region_begin); });
                pool.wait();
            }
            else
            {
                // use current thread to run learner read.
                func_run_learner_read(0);
            }

            if (region_status != RegionException::RegionReadStatus::OK)
                func_throw_retry_region(region_status);

            auto end_time = Clock::now();
            LOG_DEBUG(log,
                "[Learner Read] wait and get data from memory cost "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms");
        }

        { // check all regions
            std::sort(regions_executor_data.begin(), regions_executor_data.end());

            region_cnt = regions_executor_data.size();

            // test in case query_info has wrong meta info.
            bool need_throw_error = false;
            for (size_t i = 1; i < region_cnt; ++i)
                need_throw_error |= regions_executor_data[i].handle_range.first < regions_executor_data[i - 1].handle_range.second;

            if (need_throw_error)
                throw Exception("Regions in query overlap, should not happen", ErrorCodes::LOGICAL_ERROR);

            // remove empty ranges
            bool need_resize = false;
            for (const auto & query_info : regions_executor_data)
                need_resize |= query_info.handle_range.first == query_info.handle_range.second;

            if (need_resize)
            {
                size_t new_size = 0;
                for (size_t i = 0; i < region_cnt; ++i)
                {
                    if (regions_executor_data[i].handle_range.first == regions_executor_data[i].handle_range.second)
                        continue;
                    regions_executor_data[new_size++] = std::move(regions_executor_data[i]);
                }
                regions_executor_data.resize(new_size);

                LOG_DEBUG(log, "After filtering empty ranges, resize regions_query_info from " << region_cnt << " to " << new_size);
            }

            region_cnt = regions_executor_data.size();

            if (pk_type == TMTPKType::UINT64)
            {
                for (size_t i = 0; i < regions_executor_data.size(); ++i)
                {
                    // seek special region with handle range [-xxx, +yyy)
                    if (CHTableHandle::needSplit(regions_executor_data[i].handle_range))
                    {
                        // move this region to end.
                        LOG_DEBUG(log,
                            "Store special region " << regions_executor_data[i].info.region_id << " first"
                                                    << ", handle range [" << regions_executor_data[i].handle_range.first.toString() << ", "
                                                    << regions_executor_data[i].handle_range.second.toString() << ")");

                        // move to end directly, the order of ranges under uint64 will be resorted.
                        std::swap(regions_executor_data[i], regions_executor_data.back());

                        // important step, we need to get all data from memory
                        region_cnt = regions_executor_data.size() - 1;
                        special_region_index = region_cnt;

                        break;
                    }
                }
            }
        }

        region_group_range_parts.assign(concurrent_num, {});
        region_group_read_groups.assign(concurrent_num, {});
        if (pk_type == TMTPKType::UINT64)
            region_group_u64_handle_ranges.assign(concurrent_num, {});
        else
            region_group_handle_ranges.assign(concurrent_num, {});
    }

    size_t part_index = 0;
    MergeTreeData::DataPartsVector parts = data.getDataPartsVector();

    NamesAndTypesList available_real_columns = data.getColumns().getAllPhysical();

    NamesAndTypesList available_real_and_virtual_columns = available_real_columns;
    for (const auto & name : virt_column_names)
        available_real_and_virtual_columns.emplace_back(data.getColumn(name));

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (real_column_names.empty())
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));

    /// If `_part` virtual column is requested, we try to use it as an index.
    Block virtual_columns_block = getBlockWithPartColumn(parts);
    if (part_column_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, context);

    std::multiset<String> part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    data.check(real_column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    const Settings & settings = context.getSettingsRef();
    SortDescription sort_descr = data.getPrimarySortDescription();

    KeyCondition key_condition(query_info, context, available_real_and_virtual_columns, sort_descr, data.getPrimaryExpression());

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        std::stringstream exception_message;
        exception_message << "Primary key (";
        for (size_t i = 0, size = sort_descr.size(); i < size; ++i)
            exception_message << (i == 0 ? "" : ", ") << sort_descr[i].column_name;
        exception_message << ") is not used and setting 'force_primary_key' is set.";

        throw Exception(exception_message.str(), ErrorCodes::INDEX_NOT_USED);
    }

    std::optional<KeyCondition> minmax_idx_condition;
    if (data.minmax_idx_expr)
    {
        minmax_idx_condition.emplace(
            query_info, context, available_real_and_virtual_columns, data.minmax_idx_sort_descr, data.minmax_idx_expr);

        if (settings.force_index_by_date && minmax_idx_condition->alwaysUnknownOrTrue())
        {
            String msg = "MinMax index by columns (";
            bool first = true;
            for (const String & col : data.minmax_idx_columns)
            {
                if (first)
                    first = false;
                else
                    msg += ", ";
                msg += col;
            }
            msg += ") is not used and setting 'force_index_by_date' is set";

            throw Exception(msg, ErrorCodes::INDEX_NOT_USED);
        }
    }

    /// Select the parts in which there can be data that satisfy `minmax_idx_condition` and that match the condition on `_part`,
    ///  as well as `max_block_number_to_read`.
    {
        auto prev_parts = parts;
        parts.clear();

        for (const auto & part : prev_parts)
        {
            if (part_values.find(part->name) == part_values.end())
                continue;

            if (part->isEmpty())
                continue;

            if (minmax_idx_condition
                && !minmax_idx_condition->mayBeTrueInRange(data.minmax_idx_columns.size(), &part->minmax_idx.min_values[0],
                    &part->minmax_idx.max_values[0], data.minmax_idx_column_types))
                continue;

            if (max_block_number_to_read && part->info.max_block > max_block_number_to_read)
                continue;

            parts.push_back(part);
        }
    }

    /// Sampling.
    Names column_names_to_read = real_column_names;
    std::shared_ptr<ASTFunction> filter_function;
    ExpressionActionsPtr filter_expression;

    RelativeSize relative_sample_size = 0;
    RelativeSize relative_sample_offset = 0;

    auto select_sample_size = select.sample_size();
    auto select_sample_offset = select.sample_offset();

    if (select_sample_size)
    {
        relative_sample_size.assign(typeid_cast<const ASTSampleRatio &>(*select_sample_size).ratio.numerator,
            typeid_cast<const ASTSampleRatio &>(*select_sample_size).ratio.denominator);

        if (relative_sample_size < 0)
            throw Exception("Negative sample size", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        relative_sample_offset = 0;
        if (select_sample_offset)
            relative_sample_offset.assign(typeid_cast<const ASTSampleRatio &>(*select_sample_offset).ratio.numerator,
                typeid_cast<const ASTSampleRatio &>(*select_sample_offset).ratio.denominator);

        if (relative_sample_offset < 0)
            throw Exception("Negative sample offset", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        /// Convert absolute value of the sampling (in form `SAMPLE 1000000` - how many rows to read) into the relative `SAMPLE 0.1` (how much data to read).
        size_t approx_total_rows = 0;
        if (relative_sample_size > 1 || relative_sample_offset > 1)
            approx_total_rows = getApproximateTotalRowsToRead(parts, key_condition, settings);

        if (relative_sample_size > 1)
        {
            relative_sample_size = convertAbsoluteSampleSizeToRelative(select_sample_size, approx_total_rows);
            LOG_DEBUG(log, "Selected relative sample size: " << toString(relative_sample_size));
        }

        /// SAMPLE 1 is the same as the absence of SAMPLE.
        if (relative_sample_size == RelativeSize(1))
            relative_sample_size = 0;

        if (relative_sample_offset > 0 && 0 == relative_sample_size)
            throw Exception("Sampling offset is incorrect because no sampling", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (relative_sample_offset > 1)
        {
            relative_sample_offset = convertAbsoluteSampleSizeToRelative(select_sample_offset, approx_total_rows);
            LOG_DEBUG(log, "Selected relative sample offset: " << toString(relative_sample_offset));
        }
    }

    /** Which range of sampling key values do I need to read?
      * First, in the whole range ("universe") we select the interval
      *  of relative `relative_sample_size` size, offset from the beginning by `relative_sample_offset`.
      *
      * Example: SAMPLE 0.4 OFFSET 0.3
      *
      * [------********------]
      *        ^ - offset
      *        <------> - size
      *
      * If the interval passes through the end of the universe, then cut its right side.
      *
      * Example: SAMPLE 0.4 OFFSET 0.8
      *
      * [----------------****]
      *                  ^ - offset
      *                  <------> - size
      *
      * Next, if the `parallel_replicas_count`, `parallel_replica_offset` settings are set,
      *  then it is necessary to break the received interval into pieces of the number `parallel_replicas_count`,
      *  and select a piece with the number `parallel_replica_offset` (from zero).
      *
      * Example: SAMPLE 0.4 OFFSET 0.3, parallel_replicas_count = 2, parallel_replica_offset = 1
      *
      * [----------****------]
      *        ^ - offset
      *        <------> - size
      *        <--><--> - pieces for different `parallel_replica_offset`, select the second one.
      *
      * It is very important that the intervals for different `parallel_replica_offset` cover the entire range without gaps and overlaps.
      * It is also important that the entire universe can be covered using SAMPLE 0.1 OFFSET 0, ... OFFSET 0.9 and similar decimals.
      */

    bool use_sampling = relative_sample_size > 0 || settings.parallel_replicas_count > 1;
    bool no_data = false; /// There is nothing left after sampling.

    if (use_sampling)
    {
        if (!data.sampling_expression)
            throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

        if (sample_factor_column_queried && relative_sample_size != 0)
            used_sample_factor = 1.0 / boost::rational_cast<Float64>(relative_sample_size);

        RelativeSize size_of_universum = 0;
        DataTypePtr type = data.getPrimaryExpression()->getSampleBlock().getByName(data.sampling_expression->getColumnName()).type;

        if (typeid_cast<const DataTypeUInt64 *>(type.get()))
            size_of_universum = RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1);
        else if (typeid_cast<const DataTypeUInt32 *>(type.get()))
            size_of_universum = RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1);
        else if (typeid_cast<const DataTypeUInt16 *>(type.get()))
            size_of_universum = RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1);
        else if (typeid_cast<const DataTypeUInt8 *>(type.get()))
            size_of_universum = RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1);
        else
            throw Exception("Invalid sampling column type in storage parameters: " + type->getName() + ". Must be unsigned integer type.",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

        if (settings.parallel_replicas_count > 1)
        {
            if (relative_sample_size == RelativeSize(0))
                relative_sample_size = 1;

            relative_sample_size /= settings.parallel_replicas_count.value;
            relative_sample_offset += relative_sample_size * RelativeSize(settings.parallel_replica_offset.value);
        }

        if (relative_sample_offset >= RelativeSize(1))
            no_data = true;

        /// Calculate the half-interval of `[lower, upper)` column values.
        bool has_lower_limit = false;
        bool has_upper_limit = false;

        RelativeSize lower_limit_rational = relative_sample_offset * size_of_universum;
        RelativeSize upper_limit_rational = (relative_sample_offset + relative_sample_size) * size_of_universum;

        UInt64 lower = boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational);
        UInt64 upper = boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational);

        if (lower > 0)
            has_lower_limit = true;

        if (upper_limit_rational < size_of_universum)
            has_upper_limit = true;

        /*std::cerr << std::fixed << std::setprecision(100)
            << "relative_sample_size: " << relative_sample_size << "\n"
            << "relative_sample_offset: " << relative_sample_offset << "\n"
            << "lower_limit_float: " << lower_limit_rational << "\n"
            << "upper_limit_float: " << upper_limit_rational << "\n"
            << "lower: " << lower << "\n"
            << "upper: " << upper << "\n";*/

        if ((has_upper_limit && upper == 0) || (has_lower_limit && has_upper_limit && lower == upper))
            no_data = true;

        if (no_data || (!has_lower_limit && !has_upper_limit))
        {
            use_sampling = false;
        }
        else
        {
            /// Let's add the conditions to cut off something else when the index is scanned again and when the request is processed.

            std::shared_ptr<ASTFunction> lower_function;
            std::shared_ptr<ASTFunction> upper_function;

            if (has_lower_limit)
            {
                if (!key_condition.addCondition(data.sampling_expression->getColumnName(), Range::createLeftBounded(lower, true)))
                    throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(data.sampling_expression);
                args->children.push_back(std::make_shared<ASTLiteral>(lower));

                lower_function = std::make_shared<ASTFunction>();
                lower_function->name = "greaterOrEquals";
                lower_function->arguments = args;
                lower_function->children.push_back(lower_function->arguments);

                filter_function = lower_function;
            }

            if (has_upper_limit)
            {
                if (!key_condition.addCondition(data.sampling_expression->getColumnName(), Range::createRightBounded(upper, false)))
                    throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(data.sampling_expression);
                args->children.push_back(std::make_shared<ASTLiteral>(upper));

                upper_function = std::make_shared<ASTFunction>();
                upper_function->name = "less";
                upper_function->arguments = args;
                upper_function->children.push_back(upper_function->arguments);

                filter_function = upper_function;
            }

            if (has_lower_limit && has_upper_limit)
            {
                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(lower_function);
                args->children.push_back(upper_function);

                filter_function = std::make_shared<ASTFunction>();
                filter_function->name = "and";
                filter_function->arguments = args;
                filter_function->children.push_back(filter_function->arguments);
            }

            filter_expression = ExpressionAnalyzer(filter_function, context, nullptr, available_real_columns).getActions(false);

            /// Add columns needed for `sampling_expression` to `column_names_to_read`.
            std::vector<String> add_columns = filter_expression->getRequiredColumns();
            column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
            std::sort(column_names_to_read.begin(), column_names_to_read.end());
            column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
        }
    }

    if (no_data)
    {
        LOG_DEBUG(log, "Sampling yields no data.");
        return {};
    }

    LOG_DEBUG(log, "Key condition: " << key_condition.toString());
    if (minmax_idx_condition)
        LOG_DEBUG(log, "MinMax index condition: " << minmax_idx_condition->toString());

    /// PREWHERE
    ExpressionActionsPtr prewhere_actions;
    String prewhere_column;
    if (select.prewhere_expression)
    {
        ExpressionAnalyzer analyzer(select.prewhere_expression, context, nullptr, available_real_columns);
        prewhere_actions = analyzer.getActions(false);
        prewhere_column = select.prewhere_expression->getColumnName();
        SubqueriesForSets prewhere_subqueries = analyzer.getSubqueriesForSets();

        /** Compute the subqueries right now.
          * NOTE Disadvantage - these calculations do not fit into the query execution pipeline.
          * They are done before the execution of the pipeline; they can not be interrupted; during the computation, packets of progress are not sent.
          */
        if (!prewhere_subqueries.empty())
            CreatingSetsBlockInputStream(std::make_shared<NullBlockInputStream>(Block()), prewhere_subqueries,
                SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode))
                .read();
    }

    /// @todo Make sure partition select works properly when sampling is used!
    NameSet specific_partitions;
    if (select.partition_expression_list)
        specific_partitions = data.getPartitionIDsInLiteral(select.partition_expression_list, context);

    /// Let's find what range to read from each part.
    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    for (auto & part : parts)
    {
        RangesInDataPart ranges(part, part_index++);

        if (data.hasPrimaryKey())
            ranges.ranges = markRangesFromPKRange(part->index, key_condition, settings);
        else
            ranges.ranges = MarkRanges{MarkRange{0, part->marks_count}};

        /// Make sure this part is in mark range and contained by valid partitions.
        if (!ranges.ranges.empty() && (specific_partitions.empty() || specific_partitions.count(part->partition.getID(data))))
        {
            parts_with_ranges.push_back(ranges);

            sum_ranges += ranges.ranges.size();
            for (const auto & range : ranges.ranges)
                sum_marks += range.end - range.begin;
        }
    }

    BlockInputStreams res;

    if (!is_txn_engine || select.raw_for_mutable)
        LOG_DEBUG(log,
            "Selected " << parts.size() << " parts, " << parts_with_ranges.size() << " parts by key, " << sum_marks
                        << " marks to read from " << sum_ranges << " ranges");
    else
    {
        TMTContext & tmt = context.getTMTContext();

        auto safe_point = PDClientHelper::getGCSafePointWithRetry(tmt.getPDClient());
        if (mvcc_query_info.read_tso < safe_point)
            throw Exception("query id: " + context.getCurrentQueryId() + ", read tso: " + toString(mvcc_query_info.read_tso)
                    + " is smaller than tidb gc safe point: " + toString(safe_point),
                ErrorCodes::LOGICAL_ERROR);

        const size_t min_marks_for_seek = computeMinMarksForSeek(settings, data);

        for (const auto & executor_data : regions_executor_data)
        {
            const auto & region_query_info = executor_data.info;
            const auto & handle_range = executor_data.handle_range;
            // check all data, include special region.
            {
                auto region = tmt.getKVStore()->getRegion(region_query_info.region_id);
                RegionException::RegionReadStatus status = RegionException::RegionReadStatus::OK;
                if (region != kvstore_region[region_query_info.region_id])
                    status = RegionException::RegionReadStatus::NOT_FOUND;
                else if (region->version() != region_query_info.version)
                    status = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;

                if (status != RegionException::RegionReadStatus::OK)
                {
                    // ABA problem may cause because one region is removed and inserted back.
                    // if the version of region is changed, the part may has less data because of compaction.
                    LOG_WARNING(log,
                        "Check after getDataPartsVector, region "
                            << region_query_info.region_id << ", version " << region_query_info.version << ", handle range ["
                            << handle_range.first.toString() << ", " << handle_range.second.toString() << ") , status "
                            << RegionException::RegionReadStatusString(status));
                    // throw exception and exit.
                    func_throw_retry_region(status);
                }
            }
        }

        for (size_t batch_size = region_cnt / concurrent_num, rest = region_cnt % concurrent_num, thread_idx = 0, region_begin = 0,
                    thread_region_size;
             thread_idx < concurrent_num; ++thread_idx, region_begin += thread_region_size)
        {

            size_t region_sum_marks = 0;
            size_t region_sum_ranges = 0;

            thread_region_size = thread_idx < rest ? batch_size + 1 : batch_size;
            if (0 == thread_region_size)
                continue;

            size_t mem_rows = 0;

            if (pk_type == TMTPKType::UINT64)
            {
                using UInt64RangeElement = std::pair<HandleRange<UInt64>, size_t>;
                std::vector<UInt64RangeElement> handle_ranges;
                handle_ranges.reserve(thread_region_size);

                for (size_t region_index = region_begin, region_end = region_begin + thread_region_size; region_index < region_end;
                     ++region_index)
                {
                    const auto & region_data = regions_executor_data[region_index];
                    if (region_data.range_scan_filter->isFullRangeScan())
                    {
                        const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(region_data.handle_range);

                        if (n != 1)
                            throw Exception("split for uint64 handle should be only 1 range", ErrorCodes::LOGICAL_ERROR);

                        handle_ranges.emplace_back(new_range[0], region_index);
                    }
                    else
                    {
                        for (const HandleRange<UInt64> & handle_range : region_data.range_scan_filter->getUInt64Ranges())
                            handle_ranges.emplace_back(handle_range, region_index);
                    }
                    mem_rows += region_data.block.rows();
                }

                // the order of uint64 is different with int64.
                std::sort(handle_ranges.begin(), handle_ranges.end(),
                    [](const UInt64RangeElement & a, const UInt64RangeElement & b) { return a.first < b.first; });

                computeHandleRanges<UInt64>(region_group_read_groups[thread_idx],
                    handle_ranges,
                    region_group_range_parts[thread_idx],
                    region_group_u64_handle_ranges[thread_idx],
                    parts_with_ranges,
                    region_sum_marks,
                    region_sum_ranges,
                    settings,
                    min_marks_for_seek);
            }
            else
            {
                std::vector<std::pair<HandleRange<HandleID>, size_t>> handle_ranges;
                handle_ranges.reserve(thread_region_size);

                for (size_t region_index = region_begin, region_end = region_begin + thread_region_size; region_index < region_end;
                     ++region_index)
                {
                    const auto & region_data = regions_executor_data[region_index];
                    if (region_data.range_scan_filter->isFullRangeScan())
                        handle_ranges.emplace_back(region_data.handle_range, region_index);
                    else
                    {
                        for (const HandleRange<Int64> & handle_range : region_data.range_scan_filter->getInt64Ranges())
                            handle_ranges.emplace_back(handle_range, region_index);
                    }
                    mem_rows += region_data.block.rows();
                }

                // handle_ranges is sorted.

                computeHandleRanges<Int64>(region_group_read_groups[thread_idx],
                    handle_ranges,
                    region_group_range_parts[thread_idx],
                    region_group_handle_ranges[thread_idx],
                    parts_with_ranges,
                    region_sum_marks,
                    region_sum_ranges,
                    settings,
                    min_marks_for_seek);
            }

            LOG_DEBUG(log,
                "Stream " << thread_idx << ", read " << thread_region_size << " regions, selected "
                          << region_group_range_parts[thread_idx].size() << " handle ranges, " << region_sum_marks << " marks to read from "
                          << region_sum_ranges << " ranges, read " << mem_rows << " rows from memory");
        }
    }

    if (parts_with_ranges.empty() && !is_txn_engine)
        return {};

    GET_METRIC(const_cast<Context &>(context).getTiFlashMetrics(), tiflash_tmt_read_parts_count).Increment(parts_with_ranges.size());

    ProfileEvents::increment(ProfileEvents::SelectedParts, parts_with_ranges.size());
    ProfileEvents::increment(ProfileEvents::SelectedRanges, sum_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, sum_marks);

    if (is_txn_engine)
    {
        bool use_uncompressed_cache = settings.use_uncompressed_cache;
        const size_t max_marks_to_use_cache
            = (settings.merge_tree_max_rows_to_use_cache + data.index_granularity - 1) / data.index_granularity;
        if (sum_marks > max_marks_to_use_cache)
            use_uncompressed_cache = false;

        if (select.raw_for_mutable)
        {
            res = spreadMarkRangesAmongStreams(std::move(parts_with_ranges),
                num_streams,
                tmt_column_names_to_read,
                max_block_size,
                use_uncompressed_cache,
                prewhere_actions,
                prewhere_column,
                virt_column_names,
                settings);

            { // read all data from memory
                BlocksList blocks;
                for (auto & executor_data : regions_executor_data)
                {
                    auto & block = executor_data.block;
                    if (block)
                        blocks.emplace_back(std::move(block));
                }
                if (!blocks.empty())
                    res.emplace_back(std::make_shared<BlocksListBlockInputStream>(std::move(blocks)));
            }
        }
        else if (!select.no_kvstore)
        {
            const auto func_make_merge_tree_input = [&](const RangesInDataPart & part, const MarkRanges & mark_ranges) {
                return std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                    settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, tmt_column_names_to_read,
                    mark_ranges, use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                    settings.max_read_buffer_size, true, true, virt_column_names, part.part_index_in_query);
            };

            const auto func_make_version_filter_input = [&](const BlockInputStreamPtr & source_stream) {
                return std::make_shared<VersionFilterBlockInputStream>(source_stream, version_column_index, mvcc_query_info.read_tso);
            };

            const auto func_make_range_filter_input
                = [&](const BlockInputStreamPtr & source_stream, const HandleRange<Int64> & handle_ranges) {
                      return std::make_shared<RangesFilterBlockInputStream<Int64>>(source_stream, handle_ranges, handle_column_index);
                  };

            const auto func_make_uint64_range_filter_input
                = [&](const BlockInputStreamPtr & source_stream, const HandleRange<UInt64> & handle_ranges) {
                      return std::make_shared<RangesFilterBlockInputStream<UInt64>>(source_stream, handle_ranges, handle_column_index);
                  };

            auto func_make_multi_way_merge_sort_input_impl = makeMultiWayMergeSortInput<TMTPKType::UNSPECIFIED>;
            switch (pk_type)
            {
                case TMTPKType::UINT64:
                    func_make_multi_way_merge_sort_input_impl = makeMultiWayMergeSortInput<TMTPKType::UINT64>;
                    break;
                case TMTPKType::INT64:
                    func_make_multi_way_merge_sort_input_impl = makeMultiWayMergeSortInput<TMTPKType::INT64>;
                    break;
                default:
                    break;
            }

            const auto func_make_multi_way_merge_sort_input = [&](const BlockInputStreams & merging) -> BlockInputStreamPtr {
                return func_make_multi_way_merge_sort_input_impl(
                    merging, data.getPrimarySortDescription(), version_column_index, delmark_column_index, DEFAULT_MERGE_BLOCK_SIZE);
            };

            for (size_t thread_idx = 0; thread_idx < concurrent_num; ++thread_idx)
            {
                BlockInputStreams union_regions_stream;

                auto & range_parts = region_group_range_parts[thread_idx];
                auto & read_groups = region_group_read_groups[thread_idx];
                for (size_t read_group_idx = 0; read_group_idx < read_groups.size(); ++read_group_idx)
                {
                    auto & read_group = read_groups[read_group_idx];
                    BlockInputStreams merging;

                    for (size_t range_idx = read_group.start_range_index; range_idx <= read_group.end_range_index; ++range_idx)
                    {
                        auto & data_parts = range_parts[range_idx];
                        for (size_t part_idx = 0; part_idx < data_parts.size(); ++part_idx)
                        {
                            const auto & part = data_parts[part_idx];
                            BlockInputStreamPtr source_stream = func_make_merge_tree_input(part, part.ranges);

                            if (pk_type == TMTPKType::UINT64)
                                source_stream = func_make_uint64_range_filter_input(
                                    source_stream, region_group_u64_handle_ranges[thread_idx][range_idx]);
                            else
                                source_stream
                                    = func_make_range_filter_input(source_stream, region_group_handle_ranges[thread_idx][range_idx]);

                            source_stream = func_make_version_filter_input(source_stream);

                            merging.emplace_back(source_stream);
                        }
                    }
                    auto & block_indexes = read_group.mem_block_indexes;
                    {
                        BlocksList blocks;
                        for (size_t idx : block_indexes)
                        {
                            if (regions_executor_data[idx].block)
                                blocks.emplace_back(std::move(regions_executor_data[idx].block));
                        }

                        if (!blocks.empty())
                        {
                            BlockInputStreamPtr region_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
                            merging.emplace_back(region_input_stream);
                        }
                    }

                    if (!merging.empty())
                    {
                        union_regions_stream.emplace_back(func_make_multi_way_merge_sort_input(merging));
                    }
                }

                if (pk_type == TMTPKType::UINT64 && thread_idx == 0 && special_region_index != -1)
                {
                    size_t region_sum_ranges = 0, region_sum_marks = 0;
                    BlockInputStreams merging;

                    std::vector<HandleRange<UInt64>> ranges;
                    const auto & special_region_info = regions_executor_data[special_region_index];
                    if (special_region_info.range_scan_filter->isFullRangeScan())
                    {
                        const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(special_region_info.handle_range);
                        for (int i = 0; i < n; ++i)
                            ranges.push_back(new_range[i]);
                    }
                    else
                    {
                        for (const HandleRange<UInt64> & range : special_region_info.range_scan_filter->getUInt64Ranges())
                            ranges.push_back(range);
                    }

                    for (size_t i = 0; i < ranges.size(); ++i)
                    {
                        const auto & handle_range = ranges[i];

                        for (const RangesInDataPart & part : parts_with_ranges)
                        {
                            MarkRanges mark_ranges = markRangesFromRegionRange<UInt64>(*part.data_part, handle_range.first,
                                handle_range.second, part.ranges, computeMinMarksForSeek(settings, data), settings);

                            if (mark_ranges.empty())
                                continue;

                            region_sum_ranges += mark_ranges.size();
                            for (const auto & range : mark_ranges)
                                region_sum_marks += range.end - range.begin;

                            BlockInputStreamPtr source_stream = func_make_merge_tree_input(part, mark_ranges);

                            source_stream = func_make_uint64_range_filter_input(source_stream, handle_range);

                            source_stream = func_make_version_filter_input(source_stream);

                            merging.emplace_back(source_stream);
                        }
                    }

                    if (log->debug())
                    {
                        std::stringstream ss;

                        for (size_t i = 0; i < ranges.size(); ++i)
                        {
                            ss << "[";
                            ranges[i].first.toString(ss);
                            ss << ",";
                            ranges[i].second.toString(ss);
                            ss << ") ";
                        }
                        LOG_DEBUG(log,
                            "[PK_IS_UINT64] special region "
                                << special_region_info.info.region_id << ", split range into " << ranges.size() << ": " << ss.str() << ", "
                                << region_sum_marks << " marks to read from " << region_sum_ranges << " ranges, read "
                                << regions_executor_data[special_region_index].block.rows() << " rows from memory");
                    }

                    {
                        BlocksList blocks;
                        if (regions_executor_data[special_region_index].block)
                            blocks.emplace_back(std::move(regions_executor_data[special_region_index].block));

                        if (!blocks.empty())
                        {
                            BlockInputStreamPtr region_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
                            merging.emplace_back(region_input_stream);
                        }
                    }

                    if (!merging.empty())
                    {
                        union_regions_stream.emplace_back(func_make_multi_way_merge_sort_input(merging));
                    }
                }

                if (!union_regions_stream.empty())
                    res.emplace_back(std::make_shared<TMTUnionBlockInputStream>(std::move(union_regions_stream)));
            }
        }
        else
        {
            // no use for select nokvstore ...
        }
    }
    else if (data.merging_params.mode == MergeTreeData::MergingParams::Mutable && !select.raw_for_mutable)
    {
        extendMutableEngineColumnNames(column_names_to_read, data);

        res = spreadMarkRangesAmongStreamsOnMutableEngine(parts_with_ranges,
            column_names_to_read,
            max_block_size,
            num_streams,
            settings.use_uncompressed_cache,
            prewhere_actions,
            prewhere_column,
            virt_column_names,
            settings);
    }
    else if (select.final())
    {
        /// Add columns needed to calculate primary key and the sign.
        std::vector<String> add_columns = data.getPrimaryExpression()->getRequiredColumns();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        res = spreadMarkRangesAmongStreamsFinal(std::move(parts_with_ranges),
            column_names_to_read,
            max_block_size,
            settings.use_uncompressed_cache,
            prewhere_actions,
            prewhere_column,
            virt_column_names,
            settings);
    }
    else
    {
        res = spreadMarkRangesAmongStreams(std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            max_block_size,
            settings.use_uncompressed_cache,
            prewhere_actions,
            prewhere_column,
            virt_column_names,
            settings);
    }

    if (use_sampling)
        for (auto & stream : res)
            stream = std::make_shared<FilterBlockInputStream>(stream, filter_expression, filter_function->getColumnName());

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (sample_factor_column_queried)
        for (auto & stream : res)
            stream = std::make_shared<AddingConstColumnBlockInputStream<Float64>>(
                stream, std::make_shared<DataTypeFloat64>(), used_sample_factor, "_sample_factor");

    return res;
}


BlockInputStreams MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreams(RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    size_t max_block_size,
    bool use_uncompressed_cache,
    ExpressionActionsPtr prewhere_actions,
    const String & prewhere_column,
    const Names & virt_columns,
    const Settings & settings) const
{
    const size_t min_marks_for_concurrent_read
        = (settings.merge_tree_min_rows_for_concurrent_read + data.index_granularity - 1) / data.index_granularity;
    const size_t max_marks_to_use_cache = (settings.merge_tree_max_rows_to_use_cache + data.index_granularity - 1) / data.index_granularity;

    /// Count marks for each part.
    std::vector<size_t> sum_marks_in_parts(parts.size());
    size_t sum_marks = 0;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// Let the ranges be listed from right to left so that the leftmost range can be dropped using `pop_back()`.
        std::reverse(parts[i].ranges.begin(), parts[i].ranges.end());

        for (const auto & range : parts[i].ranges)
            sum_marks_in_parts[i] += range.end - range.begin;

        sum_marks += sum_marks_in_parts[i];
    }

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    BlockInputStreams res;

    if (sum_marks > 0 && settings.merge_tree_uniform_read_distribution == 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (sum_marks < num_streams * min_marks_for_concurrent_read && parts.size() < num_streams)
            num_streams = std::max((sum_marks + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, parts.size());

        MergeTreeReadPoolPtr pool = std::make_shared<MergeTreeReadPool>(num_streams, sum_marks, min_marks_for_concurrent_read, parts, data,
            prewhere_actions, prewhere_column, true, column_names, MergeTreeReadPool::BackoffSettings(settings),
            settings.preferred_block_size_bytes, false);

        /// Let's estimate total number of rows for progress bar.
        const size_t total_rows = data.index_granularity * sum_marks;
        LOG_TRACE(log, "Reading approx. " << total_rows << " rows");

        for (size_t i = 0; i < num_streams; ++i)
        {
            res.emplace_back(std::make_shared<MergeTreeThreadBlockInputStream>(i, pool, min_marks_for_concurrent_read, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, data, use_uncompressed_cache,
                prewhere_actions, prewhere_column, settings, virt_columns));

            if (i == 0)
            {
                /// Set the approximate number of rows for the first source only
                static_cast<IProfilingBlockInputStream &>(*res.front()).addTotalRowsApprox(total_rows);
            }
        }
    }
    else if (sum_marks > 0)
    {
        const size_t min_marks_per_stream = (sum_marks - 1) / num_streams + 1;

        for (size_t i = 0; i < num_streams && !parts.empty(); ++i)
        {
            size_t need_marks = min_marks_per_stream;

            /// Loop over parts.
            /// We will iteratively take part or some subrange of a part from the back
            ///  and assign a stream to read from it.
            while (need_marks > 0 && !parts.empty())
            {
                RangesInDataPart part = parts.back();
                size_t & marks_in_part = sum_marks_in_parts.back();

                /// We will not take too few rows from a part.
                if (marks_in_part >= min_marks_for_concurrent_read && need_marks < min_marks_for_concurrent_read)
                    need_marks = min_marks_for_concurrent_read;

                /// Do not leave too few rows in the part.
                if (marks_in_part > need_marks && marks_in_part - need_marks < min_marks_for_concurrent_read)
                    need_marks = marks_in_part;

                MarkRanges ranges_to_get_from_part;

                /// We take the whole part if it is small enough.
                if (marks_in_part <= need_marks)
                {
                    /// Restore the order of segments.
                    std::reverse(part.ranges.begin(), part.ranges.end());

                    ranges_to_get_from_part = part.ranges;

                    need_marks -= marks_in_part;
                    parts.pop_back();
                    sum_marks_in_parts.pop_back();
                }
                else
                {
                    /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                    while (need_marks > 0)
                    {
                        if (part.ranges.empty())
                            throw Exception("Unexpected end of ranges while spreading marks among streams", ErrorCodes::LOGICAL_ERROR);

                        MarkRange & range = part.ranges.back();

                        const size_t marks_in_range = range.end - range.begin;
                        const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                        ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                        range.begin += marks_to_get_from_range;
                        marks_in_part -= marks_to_get_from_range;
                        need_marks -= marks_to_get_from_range;
                        if (range.begin == range.end)
                            part.ranges.pop_back();
                    }
                }

                BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                    settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names,
                    ranges_to_get_from_part, use_uncompressed_cache, prewhere_actions, prewhere_column, true,
                    settings.min_bytes_to_use_direct_io, settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);

                res.push_back(source_stream);
            }
        }

        if (!parts.empty())
            throw Exception("Couldn't spread marks among streams", ErrorCodes::LOGICAL_ERROR);
    }

    return res;
}

BlockInputStreams MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsFinal(RangesInDataParts && parts,
    const Names & column_names,
    size_t max_block_size,
    bool use_uncompressed_cache,
    ExpressionActionsPtr prewhere_actions,
    const String & prewhere_column,
    const Names & virt_columns,
    const Settings & settings) const
{
    const size_t max_marks_to_use_cache = (settings.merge_tree_max_rows_to_use_cache + data.index_granularity - 1) / data.index_granularity;

    size_t sum_marks = 0;
    for (size_t i = 0; i < parts.size(); ++i)
        for (size_t j = 0; j < parts[i].ranges.size(); ++j)
            sum_marks += parts[i].ranges[j].end - parts[i].ranges[j].begin;

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    BlockInputStreams to_merge;

    /// NOTE `merge_tree_uniform_read_distribution` is not used for FINAL

    for (size_t part_index = 0; part_index < parts.size(); ++part_index)
    {
        RangesInDataPart & part = parts[part_index];

        BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
            settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
            use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
            settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);

        to_merge.emplace_back(std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression()));
    }

    BlockInputStreamPtr merged;

    switch (data.merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged = std::make_shared<MergingSortedBlockInputStream>(to_merge, data.getSortDescription(), max_block_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged
                = std::make_shared<CollapsingFinalBlockInputStream>(to_merge, data.getSortDescription(), data.merging_params.sign_column);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged = std::make_shared<SummingSortedBlockInputStream>(
                to_merge, data.getSortDescription(), data.merging_params.columns_to_sum, max_block_size);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged = std::make_shared<AggregatingSortedBlockInputStream>(to_merge, data.getSortDescription(), max_block_size);
            break;

        case MergeTreeData::MergingParams::Replacing: /// TODO Make ReplacingFinalBlockInputStream
            merged = std::make_shared<ReplacingSortedBlockInputStream>(
                to_merge, data.getSortDescription(), data.merging_params.version_column, max_block_size);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing: /// TODO Make VersionedCollapsingFinalBlockInputStream
            merged = std::make_shared<VersionedCollapsingSortedBlockInputStream>(
                to_merge, data.getSortDescription(), data.merging_params.sign_column, max_block_size, true);
            break;

        case MergeTreeData::MergingParams::Graphite:
            throw Exception("GraphiteMergeTree doesn't support FINAL", ErrorCodes::LOGICAL_ERROR);

        case MergeTreeData::MergingParams::Mutable:
        case MergeTreeData::MergingParams::Txn:
            throw Exception("MutableMergeTree doesn't handle here", ErrorCodes::LOGICAL_ERROR);
    }

    return {merged};
}


void MergeTreeDataSelectExecutor::createPositiveSignCondition(
    ExpressionActionsPtr & out_expression, String & out_column, const Context & context) const
{
    auto function = std::make_shared<ASTFunction>();
    auto arguments = std::make_shared<ASTExpressionList>();
    auto sign = std::make_shared<ASTIdentifier>(data.merging_params.sign_column);
    auto one = std::make_shared<ASTLiteral>(Field(static_cast<Int64>(1)));

    function->name = "equals";
    function->arguments = arguments;
    function->children.push_back(arguments);

    arguments->children.push_back(sign);
    arguments->children.push_back(one);

    out_expression = ExpressionAnalyzer(function, context, {}, data.getColumns().getAllPhysical()).getActions(false);
    out_column = function->getColumnName();
}


/// Calculates a set of mark ranges, that could possibly contain keys, required by condition.
/// In other words, it removes subranges from whole range, that definitely could not contain required keys.
MarkRanges MergeTreeDataSelectExecutor::markRangesFromPKRange(
    const MergeTreeData::DataPart::Index & index, const KeyCondition & key_condition, const Settings & settings) const
{
    MarkRanges res;

    size_t marks_count = index.at(0)->size();
    if (marks_count == 0)
        return res;

    /// If index is not used.
    if (key_condition.alwaysUnknownOrTrue())
    {
        res.push_back(MarkRange(0, marks_count));
    }
    else
    {
        size_t used_key_size = key_condition.getMaxKeyColumn() + 1;
        size_t min_marks_for_seek = computeMinMarksForSeek(settings, data);
        // size_t min_marks_for_seek = (settings.merge_tree_min_rows_for_seek + data.index_granularity - 1) / data.index_granularity;
        /** There will always be disjoint suspicious segments on the stack, the leftmost one at the top (back).
            * At each step, take the left segment and check if it fits.
            * If fits, split it into smaller ones and put them on the stack. If not, discard it.
            * If the segment is already of one mark length, add it to response and discard it.
            */
        std::vector<MarkRange> ranges_stack{{0, marks_count}};

        /// NOTE Creating temporary Field objects to pass to KeyCondition.
        Row index_left(used_key_size);
        Row index_right(used_key_size);

        while (!ranges_stack.empty())
        {
            MarkRange range = ranges_stack.back();
            ranges_stack.pop_back();

            bool may_be_true;
            if (range.end == marks_count)
            {
                for (size_t i = 0; i < used_key_size; ++i)
                {
                    index[i]->get(range.begin, index_left[i]);
                }

                may_be_true = key_condition.mayBeTrueAfter(used_key_size, &index_left[0], data.primary_key_data_types);
            }
            else
            {
                for (size_t i = 0; i < used_key_size; ++i)
                {
                    index[i]->get(range.begin, index_left[i]);
                    index[i]->get(range.end, index_right[i]);
                }

                may_be_true = key_condition.mayBeTrueInRange(used_key_size, &index_left[0], &index_right[0], data.primary_key_data_types);
            }

            if (!may_be_true)
                continue;

            if (range.end == range.begin + 1)
            {
                /// We saw a useful gap between neighboring marks. Either add it to the last range, or start a new range.
                if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                    res.push_back(range);
                else
                    res.back().end = range.end;
            }
            else
            {
                /// Break the segment and put the result on the stack from right to left.
                size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
                size_t end;

                for (end = range.end; end > range.begin + step; end -= step)
                    ranges_stack.push_back(MarkRange(end - step, end));

                ranges_stack.push_back(MarkRange(range.begin, end));
            }
        }
    }

    return res;
}

BlockInputStreams MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsOnMutableEngine(const RangesInDataParts & parts_with_ranges,
    const Names & column_names,
    size_t max_block_size,
    unsigned num_streams,
    bool use_uncompressed_cache,
    ExpressionActionsPtr prewhere_actions,
    const String & prewhere_column,
    const Names & virt_columns,
    const Settings & settings) const
{
    LOG_DEBUG(log, "Number of streams: " << num_streams);

    size_t sum_marks = 0;

    const size_t max_marks_to_use_cache = (settings.merge_tree_max_rows_to_use_cache + data.index_granularity - 1) / data.index_granularity;
    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    std::unordered_map<std::string, RangesInDataParts> partitions;
    for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
    {
        const RangesInDataPart & part = parts_with_ranges[part_index];
        auto partition_id = part.data_part->partition.getID(data);
        if (partitions.find(partition_id) == partitions.end())
            partitions[partition_id] = RangesInDataParts();
        partitions[partition_id].emplace_back(part);
    }

    // TODO: Use one stream if data is small enough

    BlockInputStreams res;

    MutableSupport::DeduperType type = MutableSupport::toDeduperType(settings.mutable_deduper);

    if (type == MutableSupport::DeduperOriginStreams)
    {
        for (auto it = parts_with_ranges.begin(); it != parts_with_ranges.end(); ++it)
        {
            const RangesInDataPart & part = *it;
            BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
                use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);
            // source_stream = std::make_shared<DebugPrintBlockInputStream>(source_stream);
            source_stream = std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression());
            res.emplace_back(source_stream);
        }
    }
    else if (type == MutableSupport::DeduperOriginUnity)
    {
        BlockInputStreams merging;
        for (auto it = parts_with_ranges.begin(); it != parts_with_ranges.end(); ++it)
        {
            const RangesInDataPart & part = *it;
            BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
                use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);
            // source_stream = std::make_shared<DebugPrintBlockInputStream>(source_stream);
            source_stream = std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression());
            merging.emplace_back(source_stream);
        }
        res.emplace_back(std::make_shared<ReplacingDeletingSortedBlockInputStream>(merging, data.getSortDescription(),
            MutableSupport::version_column_name, MutableSupport::delmark_column_name, DEFAULT_MERGE_BLOCK_SIZE, nullptr, false));
    }
    else if (type == MutableSupport::DeduperReplacingUnity)
    {
        BlockInputStreams merging;
        for (auto it = partitions.begin(); it != partitions.end(); ++it)
        {
            RangesInDataParts & parts = it->second;

            for (size_t i = 0; i < parts.size(); ++i)
            {
                const RangesInDataPart & part = parts[i];
                BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                    settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
                    use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                    settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);
                // source_stream = std::make_shared<DebugPrintBlockInputStream>(source_stream);
                source_stream = std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression());
                merging.emplace_back(source_stream);
            }
        }
        res.emplace_back(std::make_shared<ReplacingDeletingSortedBlockInputStream>(merging, data.getSortDescription(),
            MutableSupport::version_column_name, MutableSupport::delmark_column_name, DEFAULT_MERGE_BLOCK_SIZE, nullptr, false));
    }
    else if (type == MutableSupport::DeduperReplacingPartitioning || type == MutableSupport::DeduperReplacingPartitioningOpt)
    {
        BlockInputStreams partitionStreams;
        for (auto it = partitions.begin(); it != partitions.end(); ++it)
        {
            RangesInDataParts & parts = it->second;
            BlockInputStreams merging;

            for (size_t i = 0; i < parts.size(); ++i)
            {
                const RangesInDataPart & part = parts[i];

                BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                    settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
                    use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                    settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);
                // source_stream = std::make_shared<DebugPrintBlockInputStream>(source_stream,
                //     "partition-" + part.data_part->partition.getID(data) + ".part-" + part.data_part->name);
                source_stream = std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression());
                // source_stream = std::make_shared<AsynchronousBlockInputStream>(source_stream);
                merging.emplace_back(source_stream);
            }

            // TODO: Optimization: if (merging.size() == 1)
            BlockInputStreamPtr merged = std::make_shared<ReplacingDeletingSortedBlockInputStream>(merging, data.getSortDescription(),
                MutableSupport::version_column_name, MutableSupport::delmark_column_name, DEFAULT_MERGE_BLOCK_SIZE, nullptr,
                type == MutableSupport::DeduperReplacingPartitioningOpt);

            // merged = std::make_shared<DebugPrintBlockInputStream>(merged, "partition-" + parts[0].data_part->partition.getID(data));
            // merged = std::make_shared<AsynchronousBlockInputStream>(merged);
            partitionStreams.emplace_back(merged);
        }

        size_t streams = partitionStreams.size();
        size_t cpu = getNumberOfPhysicalCPUCores();
        if (streams <= cpu)
        {
            res = partitionStreams;
        }
        else
        {
            size_t n = streams / cpu;
            size_t r = streams % cpu;
            size_t offset = 0;
            for (size_t i = 0; i < cpu; i++)
            {
                int count = (r != 0 && i <= (r - 1)) ? n + 1 : n;
                BlockInputStreams sub(partitionStreams.begin() + offset, partitionStreams.begin() + offset + count);
                res.emplace_back(std::make_shared<ConcatBlockInputStream>(sub));
                offset += count;
            }
        }
    }
    else if (type == MutableSupport::DeduperDedupPartitioning)
    {
        for (auto it = partitions.begin(); it != partitions.end(); ++it)
        {
            RangesInDataParts & parts = it->second;
            BlockInputStreams merging;

            for (size_t i = 0; i < parts.size(); ++i)
            {
                const RangesInDataPart & part = parts[i];

                BlockInputStreamPtr source_stream = std::make_shared<MergeTreeBlockInputStream>(data, part.data_part, max_block_size,
                    settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges,
                    use_uncompressed_cache, prewhere_actions, prewhere_column, true, settings.min_bytes_to_use_direct_io,
                    settings.max_read_buffer_size, true, true, virt_columns, part.part_index_in_query);
                // source_stream = std::make_shared<DebugPrintBlockInputStream>(source_stream,
                //     "partition-" + part.data_part->partition.getID(data) + ".part-" + part.data_part->name);
                source_stream = std::make_shared<ExpressionBlockInputStream>(source_stream, data.getPrimaryExpression());
                merging.emplace_back(source_stream);
            }

            BlockInputStreamPtr merged = std::make_shared<DedupSortedBlockInputStream>(merging, data.getSortDescription());
            // merged = std::make_shared<DebugPrintBlockInputStream>(merged, "partition-" + parts[0].data_part->partition.getID(data));
            res.emplace_back(merged);
        }
    }

    return res;
}

} // namespace DB
