#include <Common/typeid_cast.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

// TODO: Remove this
#include <Storages/Transaction/tests/region_helper.h>

#include <Debug/dbgTools.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncRegion.h>

#include <Interpreters/executeQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

TableID getTableID(Context & context, const std::string & database_name, const std::string & table_name)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
        return table->id();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto storage = context.getTable(database_name, table_name);
    auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
    auto table_info = merge_tree->getTableInfo();
    return table_info.id;
}

void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 5)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name",
            ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionKey start = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    RegionKey end = (RegionKey)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;

    TableID table_id = getTableID(context, database_name, table_name);

    TMTContext & tmt = context.getTMTContext();
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
    tmt.kvstore->onSnapshot(region, &context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")" <<
        " to table #" << table_id << " with kvstore.onSnapshot";
    output(ss.str());
}

void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 5)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name",
            ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionKey start = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    RegionKey end = (RegionKey)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;

    TableID table_id = getTableID(context, database_name, table_name);

    TMTContext & tmt = context.getTMTContext();

    enginepb::SnapshotRequest req;
    bool is_readed = false;

    *(req.mutable_state()->mutable_peer()) = createPeer(1, true);

    metapb::Region region_info;
    region_info.set_id(region_id);
    region_info.set_start_key(RecordKVFormat::genKey(table_id, start).getStr());
    region_info.set_end_key(RecordKVFormat::genKey(table_id, end).getStr());
    *(region_info.mutable_peers()->Add()) = createPeer(1, true);
    *(region_info.mutable_peers()->Add()) = createPeer(2, false);
    *(req.mutable_state()->mutable_region()) = region_info;

    *(req.mutable_state()->mutable_apply_state()) = initialApplyState();

    // TODO: Put data into snapshot cmd

    auto reader = [&] (enginepb::SnapshotRequest * out)
    {
        if (is_readed)
            return false;
        *out = req;
        is_readed = true;
        return true;
    };
    applySnapshot(tmt.kvstore, reader, &context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")" <<
        " to table #" << table_id << " with raft commands";
    output(ss.str());
}

std::string getRegionKeyString(const HandleID s, const TiKVKey & k)
{
    try
    {
        if (s == std::numeric_limits<HandleID>::min() || s == std::numeric_limits<HandleID>::max())
        {
            String raw_key = k.empty() ? "" : std::get<0>(RecordKVFormat::decodeTiKVKey(k));
            bool is_record = RecordKVFormat::isRecord(raw_key);
            std::stringstream ss;
            if (is_record)
            {
                ss << "h:" << RecordKVFormat::getHandle(raw_key);
                ss << "#" << RecordKVFormat::getTableId(raw_key);
            }
            else
            {
                ss << "v:[" << raw_key << "]";
            }
            return ss.str();
        }
        return toString(s);
    }
    catch (...)
    {
        return "e:" + k.toHex();
    }
}

std::string getStartKeyString(TableID table_id, const TiKVKey & start_key)
{
    try
    {
        HandleID start_handle = TiKVRange::getRangeHandle<true>(start_key, table_id);
        return getRegionKeyString(start_handle, start_key);
    }
    catch (...)
    {
        return"e: " + start_key.toHex();
    }
}

std::string getEndKeyString(TableID table_id, const TiKVKey & end_key)
{
    try
    {
        HandleID end_handle = TiKVRange::getRangeHandle<false>(end_key, table_id);
        return getRegionKeyString(end_handle, end_key);
    }
    catch (...)
    {
        return"e: " + end_key.toHex();
    }
}

void dbgFuncRegionPartition(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() > 1)
    {
        throw Exception("Args not matched, should be: [show-region-range=false]",
            ErrorCodes::BAD_ARGUMENTS);
    }

    bool show_region = false;
    if (args.size() > 0)
        show_region = (std::string(typeid_cast<const ASTIdentifier &>(*args[0]).name) == "true");

    auto & tmt = context.getTMTContext();

    RegionPartition::RegionMap regions;
    tmt.region_partition.dumpRegionMap(regions);

    for (const auto & it: regions)
    {
        auto region_id = it.first;
        auto & t2p = it.second.table_to_partition;
        for (const auto & info: t2p)
        {
            auto table_id = info.first;
            auto partition_id = info.second;

            std::stringstream region_range_info;
            if (show_region)
            {
                region_range_info << " (";
                RegionPtr region = tmt.kvstore->getRegion(region_id);
                if (!region)
                {
                    region_range_info << "not in kvstore";
                }
                else
                {
                    auto [start_key, end_key] = region->getRange();
                    region_range_info << getStartKeyString(table_id, start_key);
                    region_range_info << ", ";
                    region_range_info << getEndKeyString(table_id, end_key);
                    region_range_info << ")";
                }
            }

            std::stringstream ss;
            ss << "table #" << table_id << " partition #" << partition_id <<
                " region #" << region_id << region_range_info.str();
            output(ss.str());
        }
    }
}

void dbgFuncRegionRmData(Context & /*context*/, const ASTs & /*args*/, DBGInvoker::Printer /*output*/)
{
    // TODO: port from RegionPartitionMgr to RegionPartition
    /*
    if (args.size() != 1)
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);

    TMTContext & tmt = context.getTMTContext();

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionPtr region = tmt.kvstore.get(region_id);

    TiKVKey start_key(region->getMeta().region.start_key());
    TiKVKey end_key(region->getMeta().region.end_key());
    RegionKey start = RecordKVFormat::getHandle(start_key);
    RegionKey end = RecordKVFormat::getHandle(end_key);

    TableID table_id = RecordKVFormat::getTableId(start_key);

    // TODO: lock structure?
    RegionPartitionMgrPtr partitions = tmt.partition_mgrs.tryGet(table_id);
    partitions->removeRegion(region_id);

    std::stringstream ss;
    ss << "delete data of region #" << region_id << ", range[" << start << ", " << end << ")" << " in table #" << table_id;
    output(ss.str());
    */
}

size_t executeQueryAndCountRows(Context & context,const std::string & query)
{
    size_t count = 0;
    Context query_context = context;
    query_context.setSessionContext(query_context);
    BlockInputStreamPtr input = executeQuery(query, query_context, true, QueryProcessingStage::Complete).in;
    input->readPrefix();
    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        count += block.rows();
    }
    input->readSuffix();
    return count;
}

std::vector<std::tuple<HandleID, HandleID, RegionID>> getPartitionRegionRanges(
    Context & context, TableID table_id, PartitionID partition_id, std::vector<RegionRange> * vec = nullptr)
{
    std::vector<std::tuple<HandleID, HandleID, RegionID>> handle_ranges;
    std::function<void(Regions)> callback = [&](Regions regions)
    {
        for (auto region : regions)
        {
            auto [start_key, end_key] = region->getRange();
            HandleID start_handle = TiKVRange::getRangeHandle<true>(start_key, table_id);
            HandleID end_handle = TiKVRange::getRangeHandle<false>(end_key, table_id);
            handle_ranges.push_back({start_handle, end_handle, region->id()});
            if (vec)
            {
                vec->push_back(region->getRange());
            }
        }
    };

    TMTContext & tmt = context.getTMTContext();
    tmt.region_partition.traverseRegionsByTablePartition(table_id, partition_id, context, callback);
    return handle_ranges;
}

void dbgFuncCheckPartitionRegionRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 2)
    {
        throw Exception("Args not matched, should be: database-name, table-name, [show-region-ranges]",
            ErrorCodes::BAD_ARGUMENTS);
    }

    const static String pk_name = "_tidb_rowid";

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    bool show_region = false;
    if (args.size() > 2)
        show_region = (std::string(typeid_cast<const ASTIdentifier &>(*args[2]).name) == "true");

    TableID table_id = getTableID(context, database_name, table_name);
    const auto partition_number = context.getMergeTreeSettings().mutable_mergetree_partition_number;

    size_t partitions_rows = 0;

    for (UInt64 partition_id = 0; partition_id < partition_number; ++partition_id)
    {
        std::vector<RegionRange> tikv_keys;
        auto handle_ranges = getPartitionRegionRanges(context, table_id, partition_id, &tikv_keys);
        std::vector<size_t> rows_list(handle_ranges.size(), 0);

        size_t regions_rows = 0;
        for (size_t i = 0; i < handle_ranges.size(); ++i)
        {
            const auto [start_handle, end_handle, region_id] = handle_ranges[i];
            (void)region_id; // ignore region_id
            std::stringstream query;
            query << "SELECT " << pk_name << " FROM " << database_name << "." << table_name <<
               " PARTITION ('" << partition_id << "') WHERE (" <<
               start_handle << " <= " << pk_name << ") AND (" <<
               pk_name << " < " << end_handle << ")";
            size_t rows = executeQueryAndCountRows(context, query.str());
            regions_rows += rows;
            rows_list[i] = rows;
        }

        std::stringstream query;
        query << "SELECT " << pk_name << " FROM " << database_name << "." <<
            table_name << " PARTITION ('" << partition_id << "')";
        size_t partition_rows = executeQueryAndCountRows(context, query.str());

        std::stringstream out;
        out << ((partition_rows != regions_rows) ? "EE" : "OK") <<
            " partition #" << partition_id << ": " << partition_rows << " rows, " <<
            handle_ranges.size() << " regions, sum(regions rows): " << regions_rows;
        if (show_region && !handle_ranges.empty())
        {
            std::stringstream ss;
            ss << ", regions:";
            for (size_t i = 0; i< handle_ranges.size(); ++i)
            {
                const auto & [start_handle, end_handle, region_id] = handle_ranges[i];
                const auto & [start_key, end_key] = tikv_keys[i];
                ss << " #" << region_id << "(" << getRegionKeyString(start_handle, start_key) <<
                    ", " << getRegionKeyString(end_handle, end_key) << ". " << rows_list[i] << " rows)";
            }
            out << ss.str();
        }
        output(out.str());
        partitions_rows += partition_rows;
    }

    output("sum(partitions rows): " + toString(partitions_rows));
}

void dbgFuncScanPartitionExtraRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const static String pk_name = "_tidb_rowid";

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    TableID table_id = getTableID(context, database_name, table_name);
    const auto partition_number = context.getMergeTreeSettings().mutable_mergetree_partition_number;

    size_t partitions_rows = 0;

    for (UInt64 partition_id = 0; partition_id < partition_number; ++partition_id)
    {
        auto handle_ranges = getPartitionRegionRanges(context, table_id, partition_id);
        sort(handle_ranges.begin(), handle_ranges.end());

        std::vector<std::pair<HandleID, int>> handle_ranges_check;

        for (int i = 0; i < (int) handle_ranges.size(); ++i)
        {
            handle_ranges_check.push_back({std::get<0>(handle_ranges[i]), i});
            handle_ranges_check.push_back({std::get<1>(handle_ranges[i]), i});
        }
        sort(handle_ranges_check.begin(), handle_ranges_check.end());

        std::stringstream ss;
        ss << "SELECT " << pk_name << " FROM " << database_name << "." << table_name <<
           " PARTITION ('" << partition_id << "')";
        std::string query = ss.str();

        Context query_context = context;
        query_context.setSessionContext(query_context);
        BlockInputStreamPtr input = executeQuery(query, query_context, true, QueryProcessingStage::Complete).in;

        input->readPrefix();
        while (true)
        {
            Block block = input->read();
            if (!block || block.rows() == 0)
                break;
            partitions_rows += block.rows();

            auto col = block.begin()->column;
            for (size_t i = 0, n = col->size(); i < n; ++i)
            {
                Field field = (*col)[i];
                auto handle_id = field.get<Int64>();
                auto it = std::upper_bound(handle_ranges_check.begin(), handle_ranges_check.end(),
                    std::make_pair(handle_id, std::numeric_limits<int>::max()));

                bool is_handle_not_found = false;
                if (it == handle_ranges_check.end())
                {
                    is_handle_not_found = true;
                }
                else
                {
                    int idx = it->second;
                    if (!(std::get<0>(handle_ranges[idx]) <= handle_id &&
                        std::get<1>(handle_ranges[idx]) > handle_id))
                    {
                        is_handle_not_found = true;
                    }
                }

                if (is_handle_not_found)
                {
                    output("partition #" + toString(partition_id) + " handle " + toString(handle_id) +
                        " not in the regions of partition");
                }
            }
        }
        input->readSuffix();
    }

    output("sum(partitions rows): " + toString(partitions_rows));
}

void dbgFuncCheckRegionCorrect(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const static String pk_name = "_tidb_rowid";

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    TableID table_id = getTableID(context, database_name, table_name);
    const auto partition_number = context.getMergeTreeSettings().mutable_mergetree_partition_number;

    // TODO: usa a struct
    std::vector<std::tuple<HandleID, HandleID, UInt64>> handle_ranges;
    auto & kvstore = context.getTMTContext().kvstore;
    kvstore->traverseRegions([&](Region * region)
    {
        auto [start_key, end_key] = region->getRange();
        HandleID start_handle = TiKVRange::getRangeHandle<true>(start_key, table_id);
        HandleID end_handle = TiKVRange::getRangeHandle<false>(end_key, table_id);
        // TODO: use plain ptr?
        handle_ranges.push_back({start_handle, end_handle, reinterpret_cast<UInt64>(region)});
    });

    sort(handle_ranges.begin(), handle_ranges.end());

    std::vector<std::pair<HandleID, int>> handle_ranges_check;
    for (int i = 0; i < (int) handle_ranges.size(); ++i)
    {
        handle_ranges_check.push_back({std::get<0>(handle_ranges[i]), i});
        handle_ranges_check.push_back({std::get<1>(handle_ranges[i]), i});
    }
    sort(handle_ranges_check.begin(), handle_ranges_check.end());

    TMTContext & tmt = context.getTMTContext();
    size_t partitions_rows = 0;

    for (UInt64 partition_id = 0; partition_id < partition_number; ++partition_id)
    {
        std::unordered_map<RegionID, RegionPtr> partition_regions;
        tmt.region_partition.traverseRegionsByTablePartition(table_id, partition_id, context, [&](Regions regions)
        {
            for (auto region : regions)
                partition_regions[region->id()] = region;
        });

        std::stringstream query;
        query << "SELECT " << pk_name << " FROM " << database_name << "." << table_name <<
           " PARTITION ('" << partition_id << "')";
        Context query_context = context;
        query_context.setSessionContext(query_context);
        BlockInputStreamPtr input = executeQuery(query.str(), query_context, true, QueryProcessingStage::Complete).in;

        while (true)
        {
            Block block = input->read();
            if (!block || block.rows() == 0)
                break;
            partitions_rows += block.rows();

            auto col = block.begin()->column;
            for (size_t i = 0, n = col->size(); i< n; ++i)
            {
                Field field = (*col)[i];
                auto handle_id = field.get<Int64>();
                auto it = std::upper_bound(handle_ranges_check.begin(), handle_ranges_check.end(),
                    std::make_pair(handle_id, std::numeric_limits<int>::max()));
                bool is_handle_not_found = false;
                int idx;
                if (it == handle_ranges_check.end())
                {
                    is_handle_not_found = true;
                }
                else
                {
                    idx = it->second;
                    if (!(std::get<0>(handle_ranges[idx]) <= handle_id && std::get<1>(handle_ranges[idx]) > handle_id))
                        is_handle_not_found = true;
                }

                if (is_handle_not_found)
                {
                    output("partition #" + toString(partition_id) + " handle " + toString(handle_id) + " not in the regions of partition");
                    continue;
                }

                Region * region = reinterpret_cast<Region*>(std::get<2>(handle_ranges[idx]));
                auto partition_regions_it = partition_regions.find(region->id());
                if (partition_regions_it == partition_regions.end())
                {
                    output("region not found in kvstore. region info: " + region->toString());
                }
                else
                {
                    if ((*partition_regions_it).second.get() != region)
                        output("region not match. region info:" + region->toString());
                }
            }
        }
    }

    output("sum(patitions rows): " + toString(partitions_rows));
}

}
