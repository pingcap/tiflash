// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/dbgKVStore.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/FFI/ColumnFamily.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/ApplySnapshot.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDB.h>
#include <pingcap/pd/IClient.h>

#include <random>


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_DATABASE;
} // namespace DB::ErrorCodes

namespace DB::RegionBench
{
metapb::Peer createPeer(UInt64 id, bool)
{
    metapb::Peer peer;
    peer.set_id(id);
    return peer;
}

metapb::Region createMetaRegion(
    RegionID region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    std::optional<metapb::RegionEpoch> maybe_epoch,
    std::optional<std::vector<metapb::Peer>> maybe_peers)
{
    TiKVKey start_key = RecordKVFormat::genKey(table_id, start);
    TiKVKey end_key = RecordKVFormat::genKey(table_id, end);

    return createMetaRegionCommonHandle(region_id, start_key, end_key, maybe_epoch, maybe_peers);
}

metapb::Region createMetaRegionCommonHandle(
    RegionID region_id,
    const std::string & start_key,
    const std::string & end_key,
    std::optional<metapb::RegionEpoch> maybe_epoch,
    std::optional<std::vector<metapb::Peer>> maybe_peers)
{
    metapb::Region meta;
    meta.set_id(region_id);

    meta.set_start_key(start_key);
    meta.set_end_key(end_key);

    if (maybe_epoch)
    {
        *meta.mutable_region_epoch() = maybe_epoch.value();
    }
    else
    {
        meta.mutable_region_epoch()->set_version(5);
        meta.mutable_region_epoch()->set_conf_ver(6);
    }

    if (maybe_peers)
    {
        const auto & peers = maybe_peers.value();
        for (const auto & peer : peers)
        {
            *(meta.mutable_peers()->Add()) = peer;
        }
    }
    else
    {
        *(meta.mutable_peers()->Add()) = createPeer(1, true);
        *(meta.mutable_peers()->Add()) = createPeer(2, false);
    }

    return meta;
}


RegionPtr makeRegion(RegionMeta && meta)
{
    return std::make_shared<Region>(std::move(meta), nullptr);
}

RegionPtr makeRegionForRange(
    UInt64 id,
    std::string start_key,
    std::string end_key,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    return std::make_shared<Region>(
        RegionMeta(
            createPeer(2, true),
            createMetaRegionCommonHandle(id, std::move(start_key), std::move(end_key)),
            initialApplyState()),
        proxy_helper);
}

RegionPtr makeRegionForTable(
    UInt64 region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    return makeRegionForRange(
        region_id,
        RecordKVFormat::genKey(table_id, start).toString(),
        RecordKVFormat::genKey(table_id, end).toString(),
        proxy_helper);
}

// Generates a lock value which fills all fields, only for test use.
TiKVValue encodeFullLockCfValue(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value,
    Timestamp min_commit_ts,
    Timestamp for_update_ts,
    uint64_t txn_size,
    const std::vector<std::string> & async_commit,
    const std::vector<uint64_t> & rollback,
    UInt64 generation)
{
    auto lock_value = RecordKVFormat::encodeLockCfValue(lock_type, primary, ts, ttl, short_value, min_commit_ts);
    WriteBufferFromOwnString res;
    res.write(lock_value.getStr().data(), lock_value.getStr().size());
    {
        res.write(RecordKVFormat::MIN_COMMIT_TS_PREFIX);
        RecordKVFormat::encodeUInt64(min_commit_ts, res);
    }
    {
        res.write(RecordKVFormat::FOR_UPDATE_TS_PREFIX);
        RecordKVFormat::encodeUInt64(for_update_ts, res);
    }
    {
        res.write(RecordKVFormat::TXN_SIZE_PREFIX);
        RecordKVFormat::encodeUInt64(txn_size, res);
    }
    {
        res.write(RecordKVFormat::ROLLBACK_TS_PREFIX);
        TiKV::writeVarUInt(rollback.size(), res);
        for (auto ts : rollback)
        {
            RecordKVFormat::encodeUInt64(ts, res);
        }
    }
    {
        res.write(RecordKVFormat::ASYNC_COMMIT_PREFIX);
        TiKV::writeVarUInt(async_commit.size(), res);
        for (const auto & s : async_commit)
        {
            writeVarInt(s.size(), res);
            res.write(s.data(), s.size());
        }
    }
    {
        res.write(RecordKVFormat::LAST_CHANGE_PREFIX);
        RecordKVFormat::encodeUInt64(12345678, res);
        TiKV::writeVarUInt(87654321, res);
    }
    {
        res.write(RecordKVFormat::TXN_SOURCE_PREFIX_FOR_LOCK);
        TiKV::writeVarUInt(876543, res);
    }
    {
        res.write(RecordKVFormat::PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX);
    }
    if (generation > 0)
    {
        res.write(RecordKVFormat::GENERATION_PREFIX);
        RecordKVFormat::encodeUInt64(generation, res);
    }
    return TiKVValue(res.releaseStr());
}

using TiDB::ColumnInfo;

void setupPutRequest(raft_cmdpb::Request * req, const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    req->set_cmd_type(raft_cmdpb::CmdType::Put);
    raft_cmdpb::PutRequest * put = req->mutable_put();
    put->set_cf(cf.c_str());
    put->set_key(key.getStr());
    put->set_value(value.getStr());
}

void setupDelRequest(raft_cmdpb::Request * req, const std::string & cf, const TiKVKey & key)
{
    req->set_cmd_type(raft_cmdpb::CmdType::Delete);
    raft_cmdpb::DeleteRequest * del = req->mutable_delete_();
    del->set_cf(cf.c_str());
    del->set_key(key.getStr());
}

void addRequestsToRaftCmd(
    raft_cmdpb::RaftCmdRequest & request,
    const TiKVKey & key,
    const TiKVValue & value,
    UInt64 prewrite_ts,
    UInt64 commit_ts,
    bool del,
    const String & pk)
{
    TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);
    const TiKVKey & lock_key = key;

    if (del)
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, pk, prewrite_ts, 0);
        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, prewrite_ts);

        setupPutRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key, lock_value);
        setupPutRequest(request.add_requests(), ColumnFamilyName::Write, commit_key, commit_value);
        setupDelRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key);
        return;
    }

    if (value.dataSize() <= RecordKVFormat::SHORT_VALUE_MAX_LEN)
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, pk, prewrite_ts, 0);

        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts, value.toString());

        setupPutRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key, lock_value);
        setupPutRequest(request.add_requests(), ColumnFamilyName::Write, commit_key, commit_value);
        setupDelRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key);
    }
    else
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, pk, prewrite_ts, 0);

        TiKVKey prewrite_key = RecordKVFormat::appendTs(key, prewrite_ts);
        const TiKVValue & prewrite_value = value;

        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts);

        setupPutRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key, lock_value);
        setupPutRequest(request.add_requests(), ColumnFamilyName::Write, commit_key, commit_value);
        setupPutRequest(request.add_requests(), ColumnFamilyName::Default, prewrite_key, prewrite_value);
        setupDelRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key);
    }
}

template <typename T>
T convertNumber(const Field & field)
{
    switch (field.getType())
    {
    case Field::Types::Int64:
        return static_cast<T>(field.get<Int64>());
    case Field::Types::UInt64:
        return static_cast<T>(field.get<UInt64>());
    case Field::Types::Float64:
        return static_cast<T>(field.get<Float64>());
    case Field::Types::Decimal32:
        return static_cast<T>(field.get<DecimalField<Decimal32>>());
    case Field::Types::Decimal64:
        return static_cast<T>(field.get<DecimalField<Decimal64>>());
    case Field::Types::Decimal128:
        return static_cast<T>(field.get<DecimalField<Decimal128>>());
    case Field::Types::Decimal256:
        return static_cast<T>(field.get<DecimalField<Decimal256>>());
    default:
        throw Exception(
            String("Unable to convert field type ") + field.getTypeName() + " to number",
            ErrorCodes::LOGICAL_ERROR);
    }
}

Field convertDecimal(const ColumnInfo & column_info, const Field & field)
{
    switch (field.getType())
    {
    case Field::Types::Int64:
        return column_info.getDecimalValue(std::to_string(field.get<Int64>()));
    case Field::Types::UInt64:
        return column_info.getDecimalValue(std::to_string(field.get<UInt64>()));
    case Field::Types::Float64:
        return column_info.getDecimalValue(std::to_string(field.get<Float64>()));
    case Field::Types::Decimal32:
        return column_info.getDecimalValue(field.get<Decimal32>().toString(column_info.decimal));
    case Field::Types::Decimal64:
        return column_info.getDecimalValue(field.get<Decimal64>().toString(column_info.decimal));
    case Field::Types::Decimal128:
        return column_info.getDecimalValue(field.get<Decimal128>().toString(column_info.decimal));
    case Field::Types::Decimal256:
        return column_info.getDecimalValue(field.get<Decimal256>().toString(column_info.decimal));
    default:
        throw Exception(
            String("Unable to convert field type ") + field.getTypeName() + " to number",
            ErrorCodes::LOGICAL_ERROR);
    }
}

Field convertEnum(const ColumnInfo & column_info, const Field & field)
{
    switch (field.getType())
    {
    case Field::Types::Int64:
    case Field::Types::UInt64:
        return convertNumber<UInt64>(field);
    case Field::Types::String:
        return static_cast<UInt64>(column_info.getEnumIndex(field.get<String>()));
    default:
        throw Exception(
            String("Unable to convert field type ") + field.getTypeName() + " to Enum",
            ErrorCodes::LOGICAL_ERROR);
    }
}

Field convertField(const ColumnInfo & column_info, const Field & field)
{
    if (field.isNull())
        return field;

    switch (column_info.tp)
    {
    case TiDB::TypeTiny:
    case TiDB::TypeShort:
    case TiDB::TypeLong:
    case TiDB::TypeLongLong:
    case TiDB::TypeInt24:
        if (column_info.hasUnsignedFlag())
            return convertNumber<UInt64>(field);
        else
            return convertNumber<Int64>(field);
    case TiDB::TypeFloat:
    case TiDB::TypeDouble:
        return convertNumber<Float64>(field);
    case TiDB::TypeDate:
    case TiDB::TypeDatetime:
    case TiDB::TypeTimestamp:
        return parseMyDateTime(field.safeGet<String>());
    case TiDB::TypeVarchar:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeBlob:
    case TiDB::TypeVarString:
    case TiDB::TypeString:
        return field;
    case TiDB::TypeEnum:
        return convertEnum(column_info, field);
    case TiDB::TypeNull:
        return Field();
    case TiDB::TypeDecimal:
    case TiDB::TypeNewDecimal:
        return convertDecimal(column_info, field);
    case TiDB::TypeTime:
    case TiDB::TypeYear:
        return convertNumber<Int64>(field);
    case TiDB::TypeSet:
    case TiDB::TypeBit:
        return convertNumber<UInt64>(field);
    default:
        return Field();
    }
}

void encodeRow(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss)
{
    if (table_info.columns.size() < fields.size() + table_info.pk_is_handle)
        throw Exception(
            "Encoding row has less columns than encode values [num_columns=" + DB::toString(table_info.columns.size())
                + "] [num_fields=" + DB::toString(fields.size()) + "] . ",
            ErrorCodes::LOGICAL_ERROR);

    std::vector<Field> flatten_fields;
    std::unordered_set<String> pk_column_names;
    if (table_info.is_common_handle)
    {
        for (const auto & idx_col : table_info.getPrimaryIndexInfo().idx_cols)
        {
            // todo support prefix index
            pk_column_names.insert(idx_col.name);
        }
    }
    for (size_t i = 0; i < fields.size(); i++)
    {
        const auto & column_info = table_info.columns[i];
        /// skip the columns encoded in the key
        if (pk_column_names.find(column_info.name) != pk_column_names.end())
            continue;
        Field field = convertField(column_info, fields[i]);
        TiDB::DatumBumpy datum = TiDB::DatumBumpy(field, column_info.tp);
        flatten_fields.emplace_back(datum.field());
    }

    static bool row_format_flip = false;
    // Ping-pong encoding using row format V1/V2.
    (row_format_flip = !row_format_flip) ? encodeRowV1(table_info, flatten_fields, ss)
                                         : encodeRowV2(table_info, flatten_fields, ss);
}

void insert( //
    const TiDB::TableInfo & table_info,
    RegionID region_id,
    HandleID handle_id, //
    ASTs::const_iterator values_begin,
    ASTs::const_iterator values_end, //
    Context & context,
    const std::optional<std::tuple<Timestamp, UInt8>> & tso_del)
{
    // Parse the fields in the inserted row
    std::vector<Field> fields;
    {
        for (auto it = values_begin; it != values_end; ++it)
        {
            auto field = typeid_cast<const ASTLiteral *>((*it).get())->value;
            fields.emplace_back(field);
        }
        if (fields.size() + table_info.pk_is_handle != table_info.columns.size())
            throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);
    }
    TMTContext & tmt = context.getTMTContext();
    pingcap::pd::ClientPtr pd_client = tmt.getPDClient();
    RegionPtr region = tmt.getKVStore()->getRegion(region_id);

    // Using the region meta's table ID rather than table_info's, as this could be a partition table so that the table ID should be partition ID.
    const auto range = region->getRange();
    TableID table_id = RecordKVFormat::getTableId(*range->rawKeys().first);

    TiKVKey key;
    if (table_info.is_common_handle)
    {
        std::vector<Field> keys;
        const auto & pk_index = table_info.getPrimaryIndexInfo();
        for (const auto & idx_col : pk_index.idx_cols)
        {
            const auto & column_info = table_info.columns[idx_col.offset];
            auto start_field = RegionBench::convertField(column_info, fields[idx_col.offset]);
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
            keys.emplace_back(start_datum.field());
        }
        key = RecordKVFormat::genKey(table_info, keys);
    }
    else
        key = RecordKVFormat::genKey(table_id, handle_id);
    WriteBufferFromOwnString ss;
    encodeRow(table_info, fields, ss);
    TiKVValue value(ss.releaseStr());

    UInt64 prewrite_ts = pd_client->getTS();
    UInt64 commit_ts = pd_client->getTS();
    bool is_del = false;

    if (tso_del.has_value())
    {
        auto [tso, del] = *tso_del;
        prewrite_ts = tso;
        commit_ts = tso;
        is_del = del;
    }

    raft_cmdpb::RaftCmdRequest request;
    addRequestsToRaftCmd(request, key, value, prewrite_ts, commit_ts, is_del);
    RegionBench::applyWriteRaftCmd(
        *tmt.getKVStore(),
        std::move(request),
        region_id,
        MockTiKV::instance().getNextRaftIndex(region_id),
        MockTiKV::instance().getRaftTerm(region_id),
        tmt);
}

void remove(const TiDB::TableInfo & table_info, RegionID region_id, HandleID handle_id, Context & context)
{
    static const TiKVValue value;

    TiKVKey key = RecordKVFormat::genKey(table_info.id, handle_id);

    TMTContext & tmt = context.getTMTContext();
    pingcap::pd::ClientPtr pd_client = tmt.getPDClient();
    RegionPtr region = tmt.getKVStore()->getRegion(region_id);

    UInt64 prewrite_ts = pd_client->getTS();
    UInt64 commit_ts = pd_client->getTS();

    raft_cmdpb::RaftCmdRequest request;
    addRequestsToRaftCmd(request, key, value, prewrite_ts, commit_ts, true);
    RegionBench::applyWriteRaftCmd(
        *tmt.getKVStore(),
        std::move(request),
        region_id,
        MockTiKV::instance().getNextRaftIndex(region_id),
        MockTiKV::instance().getRaftTerm(region_id),
        tmt);
}

struct BatchCtrl
{
    String default_str;
    Int64 concurrent_id;
    Int64 flush_num;
    Int64 batch_num;
    UInt64 min_strlen;
    UInt64 max_strlen;
    Context * context;
    RegionPtr region;
    HandleID handle_begin;
    bool del;

    BatchCtrl(
        Int64 concurrent_id_,
        Int64 flush_num_,
        Int64 batch_num_,
        UInt64 min_strlen_,
        UInt64 max_strlen_,
        Context * context_,
        RegionPtr region_,
        HandleID handle_begin_,
        bool del_)
        : concurrent_id(concurrent_id_)
        , flush_num(flush_num_)
        , batch_num(batch_num_)
        , min_strlen(min_strlen_)
        , max_strlen(max_strlen_)
        , context(context_)
        , region(region_)
        , handle_begin(handle_begin_)
        , del(del_)
    {
        assert(max_strlen >= min_strlen);
        assert(min_strlen >= 1);
        auto str_len = static_cast<size_t>(random() % (max_strlen - min_strlen + 1) + min_strlen);
        default_str = String(str_len, '_');
    }

    void encodeDatum(WriteBuffer & ss, TiDB::CodecFlag flag, Int64 magic_num)
    {
        Int8 target = (magic_num % 70) + '0';
        EncodeUInt(static_cast<UInt8>(flag), ss);
        switch (flag)
        {
        case TiDB::CodecFlagJson:
            throw Exception(
                "Not implemented yet: BatchCtrl::encodeDatum, TiDB::CodecFlagJson",
                ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagVectorFloat32:
            throw Exception(
                "Not implemented yet: BatchCtrl::encodeDatum, TiDB::CodecFlagVectorFloat32",
                ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagMax:
            throw Exception(
                "Not implemented yet: BatchCtrl::encodeDatum, TiDB::CodecFlagMax",
                ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagDuration:
            throw Exception(
                "Not implemented yet: BatchCtrl::encodeDatum, TiDB::CodecFlagDuration",
                ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagNil:
            return;
        case TiDB::CodecFlagBytes:
            memset(default_str.data(), target, default_str.size());
            return EncodeBytes(default_str, ss);
        //case TiDB::CodecFlagDecimal:
        //    return EncodeDecimal(Decimal(magic_num), ss);
        case TiDB::CodecFlagCompactBytes:
            memset(default_str.data(), target, default_str.size());
            return EncodeCompactBytes(default_str, ss);
        case TiDB::CodecFlagFloat:
            return EncodeFloat64(static_cast<Float64>(magic_num) / 1111.1, ss);
        case TiDB::CodecFlagUInt:
            return EncodeUInt<UInt64>(static_cast<UInt64>(magic_num), ss);
        case TiDB::CodecFlagInt:
            return EncodeInt64((magic_num), ss);
        case TiDB::CodecFlagVarInt:
            return EncodeVarInt((magic_num), ss);
        case TiDB::CodecFlagVarUInt:
            return EncodeVarUInt(static_cast<UInt64>(magic_num), ss);
        default:
            throw Exception("Not implemented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
        }
    }

    TiKVValue encodeRow(const TiDB::TableInfo & table_info, Int64 magic_num)
    {
        WriteBufferFromOwnString ss;
        for (const auto & column : table_info.columns)
        {
            encodeDatum(ss, TiDB::CodecFlagInt, column.id);
            // TODO: May need to use BumpyDatum to flatten before encoding.
            encodeDatum(ss, column.getCodecFlag(), magic_num);
        }
        return TiKVValue(ss.releaseStr());
    }
};

void batchInsert(
    const TiDB::TableInfo & table_info,
    std::unique_ptr<BatchCtrl> batch_ctrl,
    std::function<Int64(Int64)> fn_gen_magic_num)
{
    RegionPtr & region = batch_ctrl->region;

    TMTContext & tmt = batch_ctrl->context->getTMTContext();
    pingcap::pd::ClientPtr pd_client = tmt.getPDClient();

    Int64 index = batch_ctrl->handle_begin;

    for (Int64 flush_cnt = 0; flush_cnt < batch_ctrl->flush_num; ++flush_cnt)
    {
        UInt64 prewrite_ts = pd_client->getTS();
        UInt64 commit_ts = pd_client->getTS();

        raft_cmdpb::RaftCmdRequest request;

        for (Int64 cnt = 0; cnt < batch_ctrl->batch_num; ++index, ++cnt)
        {
            TiKVKey key = RecordKVFormat::genKey(table_info.id, index);
            TiKVValue value = batch_ctrl->encodeRow(table_info, fn_gen_magic_num(index));
            addRequestsToRaftCmd(request, key, value, prewrite_ts, commit_ts, batch_ctrl->del);
        }

        RegionBench::applyWriteRaftCmd(
            *tmt.getKVStore(),
            std::move(request),
            region->id(),
            MockTiKV::instance().getNextRaftIndex(region->id()),
            MockTiKV::instance().getRaftTerm(region->id()),
            tmt);
    }
}

void concurrentBatchInsert(
    const TiDB::TableInfo & table_info,
    Int64 concurrent_num,
    Int64 flush_num,
    Int64 batch_num,
    UInt64 min_strlen,
    UInt64 max_strlen,
    Context & context)
{
    TMTContext & tmt = context.getTMTContext();

    RegionID curr_max_region_id(InvalidRegionID);
    HandleID curr_max_handle_id = 0;
    tmt.getKVStore()->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        curr_max_region_id
            = (curr_max_region_id == InvalidRegionID) ? region_id : std::max<RegionID>(curr_max_region_id, region_id);
        const auto range = region->getRange();
        curr_max_handle_id = std::max(RecordKVFormat::getHandle(*range->rawKeys().second), curr_max_handle_id);
    });

    Int64 key_num_each_region = flush_num * batch_num;
    HandleID handle_begin = curr_max_handle_id;


    auto debug_kvstore = RegionBench::DebugKVStore(*tmt.getKVStore());
    Regions regions = MockTiKV::instance().createRegions( //
        table_info.id,
        concurrent_num,
        key_num_each_region,
        handle_begin,
        curr_max_region_id + 1);
    for (const RegionPtr & region : regions)
        debug_kvstore.onSnapshot<RegionPtrWithSnapshotFiles>(RegionPtrWithSnapshotFiles{region, {}}, nullptr, 0, tmt);

    std::list<std::thread> threads;
    for (Int64 i = 0; i < concurrent_num; i++, handle_begin += key_num_each_region)
    {
        auto batch_ptr = std::make_unique<
            BatchCtrl>(i, flush_num, batch_num, min_strlen, max_strlen, &context, regions[i], handle_begin, false);
        threads.push_back(
            std::thread(&batchInsert, table_info, std::move(batch_ptr), [](Int64 index) -> Int64 { return index; }));
    }
    for (auto & thread : threads)
    {
        thread.join();
    }
}

Int64 concurrentRangeOperate(
    const TiDB::TableInfo & table_info,
    HandleID start_handle,
    HandleID end_handle,
    Context & context,
    Int64 magic_num,
    bool del)
{
    Regions regions;

    {
        TMTContext & tmt = context.getTMTContext();
        for (auto && [_, r] : tmt.getRegionTable().getRegionsByTable(NullspaceID, table_info.id))
        {
            std::ignore = _;
            if (r == nullptr)
                continue;
            regions.push_back(r);
        }
    }

    std::shuffle(regions.begin(), regions.end(), std::default_random_engine());

    std::list<std::thread> threads;
    Int64 tol = 0;
    for (const auto & region : regions)
    {
        const auto range = region->getRange();
        const auto & [ss, ee] = getHandleRangeByTable(range->rawKeys(), table_info.id);
        TiKVRange::Handle handle_begin = std::max<TiKVRange::Handle>(ss, start_handle);
        TiKVRange::Handle handle_end = std::min<TiKVRange::Handle>(ee, end_handle);
        if (handle_end <= handle_begin)
            continue;
        Int64 batch_num = handle_end - handle_begin;
        tol += batch_num;
        auto batch_ptr
            = std::make_unique<BatchCtrl>(-1, 1, batch_num, 1, 1, &context, region, handle_begin.handle_id, del);
        threads.push_back(std::thread(&batchInsert, table_info, std::move(batch_ptr), [=](Int64 index) -> Int64 {
            std::ignore = index;
            return magic_num;
        }));
    }
    for (auto & thread : threads)
    {
        thread.join();
    }
    return tol;
}

TableID getTableID(
    Context & context,
    const std::string & database_name,
    const std::string & table_name,
    const std::string & partition_id)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

        if (table->isPartitionTable())
            return std::strtol(partition_id.c_str(), nullptr, 0);

        return table->id();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto mapped_table_name = mappedTable(context, database_name, table_name).second;
    auto mapped_database_name = mappedDatabase(context, database_name);
    auto storage = context.getTable(mapped_database_name, mapped_table_name);
    auto managed_storage = std::static_pointer_cast<IManageableStorage>(storage);
    auto table_info = managed_storage->getTableInfo();
    return table_info.id;
}

const TiDB::TableInfo & getTableInfo(Context & context, const String & database_name, const String & table_name)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

        return table->table_info;
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto mapped_table_name = mappedTable(context, database_name, table_name).second;
    auto mapped_database_name = mappedDatabase(context, database_name);
    auto storage = context.getTable(mapped_database_name, mapped_table_name);
    auto managed_storage = std::static_pointer_cast<IManageableStorage>(storage);
    return managed_storage->getTableInfo();
}


EngineStoreApplyRes applyWriteRaftCmd(
    KVStore & kvstore,
    raft_cmdpb::RaftCmdRequest && request,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt,
    DM::WriteResult * write_result_ptr)
{
    std::vector<BaseBuffView> keys;
    std::vector<BaseBuffView> vals;
    std::vector<WriteCmdType> cmd_types;
    std::vector<ColumnFamilyType> cmd_cf;
    keys.reserve(request.requests_size());
    vals.reserve(request.requests_size());
    cmd_types.reserve(request.requests_size());
    cmd_cf.reserve(request.requests_size());

    for (const auto & req : request.requests())
    {
        auto type = req.cmd_type();

        switch (type)
        {
        case raft_cmdpb::CmdType::Put:
            keys.push_back({req.put().key().data(), req.put().key().size()});
            vals.push_back({req.put().value().data(), req.put().value().size()});
            cmd_types.push_back(WriteCmdType::Put);
            cmd_cf.push_back(NameToCF(req.put().cf()));
            break;
        case raft_cmdpb::CmdType::Delete:
            keys.push_back({req.delete_().key().data(), req.delete_().key().size()});
            vals.push_back({nullptr, 0});
            cmd_types.push_back(WriteCmdType::Del);
            cmd_cf.push_back(NameToCF(req.delete_().cf()));
            break;
        default:
            throw Exception(
                fmt::format("Unsupport raft cmd {}", raft_cmdpb::CmdType_Name(type)),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    if (write_result_ptr)
    {
        return kvstore.handleWriteRaftCmdInner(
            WriteCmdsView{
                .keys = keys.data(),
                .vals = vals.data(),
                .cmd_types = cmd_types.data(),
                .cmd_cf = cmd_cf.data(),
                .len = keys.size()},
            region_id,
            index,
            term,
            tmt,
            *write_result_ptr);
    }
    else
    {
        DM::WriteResult write_result;
        return kvstore.handleWriteRaftCmdInner(
            WriteCmdsView{
                .keys = keys.data(),
                .vals = vals.data(),
                .cmd_types = cmd_types.data(),
                .cmd_cf = cmd_cf.data(),
                .len = keys.size()},
            region_id,
            index,
            term,
            tmt,
            write_result);
    }
}

void handleApplySnapshot(
    KVStore & kvstore,
    metapb::Region && region,
    uint64_t peer_id,
    SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index,
    TMTContext & tmt)
{
    auto new_region = kvstore.genRegionPtr(std::move(region), peer_id, index, term, tmt.getRegionTable());
    auto prehandle_result = kvstore.preHandleSnapshotToFiles(new_region, snaps, index, term, deadline_index, tmt);
    kvstore.applyPreHandledSnapshot(
        RegionPtrWithSnapshotFiles{new_region, std::move(prehandle_result.ingest_ids)},
        tmt);
}
} // namespace DB::RegionBench
