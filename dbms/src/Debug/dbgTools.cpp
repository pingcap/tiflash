#include <random>

#include <Common/typeid_cast.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Raft/RaftContext.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/MyTimeParser.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace RegionBench
{

using TiDB::ColumnInfo;
using TiDB::TableInfo;

RegionPtr createRegion(TableID table_id, RegionID region_id, const HandleID & start, const HandleID & end)
{
    enginepb::SnapshotRequest request;
    enginepb::SnapshotState * state = request.mutable_state();
    state->mutable_region()->set_id(region_id);

    TiKVKey start_key = RecordKVFormat::genKey(table_id, start);
    TiKVKey end_key = RecordKVFormat::genKey(table_id, end);

    state->mutable_region()->set_start_key(start_key.getStr());
    state->mutable_region()->set_end_key(end_key.getStr());

    RegionMeta region_meta(state->peer(), state->region(), initialApplyState());
    region_meta.setApplied(MockTiKV::instance().getRaftIndex(region_id), RAFT_INIT_LOG_TERM);
    RegionPtr region = std::make_shared<Region>(std::move(region_meta));
    return region;
}

Regions createRegions(TableID table_id, size_t region_num, size_t key_num_each_region, HandleID handle_begin, RegionID new_region_id_begin)
{
    Regions regions;
    for (RegionID region_id = new_region_id_begin; region_id < static_cast<RegionID>(new_region_id_begin + region_num);
         ++region_id, handle_begin += key_num_each_region)
    {
        auto ptr = createRegion(table_id, region_id, handle_begin, handle_begin + key_num_each_region);
        regions.push_back(ptr);
    }
    return regions;
}

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

void addRequestsToRaftCmd(enginepb::CommandRequest * cmd, RegionID region_id, const TiKVKey & key, const TiKVValue & value,
    UInt64 prewrite_ts, UInt64 commit_ts, bool del, const String pk = "pk")
{
    {
        enginepb::CommandRequestHeader * header = cmd->mutable_header();
        header->set_region_id(region_id);
        header->set_term(MockTiKV::instance().getRaftTerm(region_id));
        header->set_index(MockTiKV::instance().getRaftIndex(region_id));
        header->set_sync_log(false);
    }

    TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);
    const TiKVKey & lock_key = key;

    if (del)
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, pk, prewrite_ts, 0);
        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, prewrite_ts);

        setupPutRequest(cmd->add_requests(), Region::lock_cf_name, lock_key, lock_value);
        setupPutRequest(cmd->add_requests(), Region::write_cf_name, commit_key, commit_value);
        setupDelRequest(cmd->add_requests(), Region::lock_cf_name, lock_key);
        return;
    }

    if (value.dataSize() <= RecordKVFormat::SHORT_VALUE_MAX_LEN)
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, pk, prewrite_ts, 0, value.toString());

        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts, value.toString());

        setupPutRequest(cmd->add_requests(), Region::lock_cf_name, lock_key, lock_value);
        setupPutRequest(cmd->add_requests(), Region::write_cf_name, commit_key, commit_value);
        setupDelRequest(cmd->add_requests(), Region::lock_cf_name, lock_key);
    }
    else
    {
        TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, pk, prewrite_ts, 0);

        TiKVKey prewrite_key = RecordKVFormat::appendTs(key, prewrite_ts);
        const TiKVValue & prewrite_value = value;

        TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts);

        setupPutRequest(cmd->add_requests(), Region::lock_cf_name, lock_key, lock_value);
        setupPutRequest(cmd->add_requests(), Region::default_cf_name, prewrite_key, prewrite_value);
        setupPutRequest(cmd->add_requests(), Region::write_cf_name, commit_key, commit_value);
        setupDelRequest(cmd->add_requests(), Region::lock_cf_name, lock_key);
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
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to number", ErrorCodes::LOGICAL_ERROR);
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
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to number", ErrorCodes::LOGICAL_ERROR);
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
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to Enum", ErrorCodes::LOGICAL_ERROR);
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
        case TiDB::TypeBit:
            if (column_info.hasUnsignedFlag())
                return convertNumber<UInt64>(field);
            else
                return convertNumber<Int64>(field);
        case TiDB::TypeFloat:
        case TiDB::TypeDouble:
            return convertNumber<Float64>(field);
        case TiDB::TypeDate:
        {
            auto text = field.get<String>();
            ReadBufferFromMemory buf(text.data(), text.size());
            DayNum_t date;
            readDateText(date, buf);
            return static_cast<UInt64>(date);
        }
        case TiDB::TypeDatetime:
        case TiDB::TypeTimestamp:
        {
            auto text = field.get<String>();
            ReadBufferFromMemory buf(text.data(), text.size());
            time_t dt;
            readDateTimeText(dt, buf);
            return static_cast<Int64>(dt);
        }
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
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to Time", ErrorCodes::LOGICAL_ERROR);
        case TiDB::TypeYear:
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to Year", ErrorCodes::LOGICAL_ERROR);
        case TiDB::TypeSet:
            throw Exception(String("Unable to convert field type ") + field.getTypeName() + " to Set", ErrorCodes::LOGICAL_ERROR);
        default:
            return Field();
    }
}

void encodeRow(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, std::stringstream & ss)
{
    if (table_info.columns.size() != fields.size())
        throw Exception("Encoding row has different sizes between columns and values", ErrorCodes::LOGICAL_ERROR);
    for (size_t i = 0; i < fields.size(); i++)
    {
        const TiDB::ColumnInfo & column_info = table_info.columns[i];
        EncodeDatum(Field(column_info.id), TiDB::CodecFlagInt, ss);
        Field field = convertField(column_info, fields[i]);
        TiDB::DatumBumpy datum = TiDB::DatumBumpy(field, column_info.tp);
        EncodeDatum(datum.field(), column_info.getCodecFlag(), ss);
    }
}

void insert(const TiDB::TableInfo & table_info, RegionID region_id, HandleID handle_id, ASTs::const_iterator begin,
    ASTs::const_iterator end, Context & context, const std::optional<std::tuple<Timestamp, UInt8>> & tso_del)
{
    std::vector<Field> fields;
    ASTs::const_iterator it;
    while ((it = begin++) != end)
    {
        auto field = typeid_cast<const ASTLiteral *>((*it).get())->value;
        fields.emplace_back(field);
    }
    if (fields.size() != table_info.columns.size())
        throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);

    TMTContext & tmt = context.getTMTContext();
    pingcap::pd::ClientPtr pd_client = tmt.getPDClient();
    RegionPtr region = tmt.getKVStore()->getRegion(region_id);

    // Using the region meta's table ID rather than table_info's, as this could be a partition table so that the table ID should be partition ID.
    TableID table_id = RecordKVFormat::getTableId(region->getRange().first);

    TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
    std::stringstream ss;
    encodeRow(table_info, fields, ss);
    TiKVValue value(ss.str());

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

    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;
    addRequestsToRaftCmd(cmds.add_requests(), region_id, key, value, prewrite_ts, commit_ts, is_del);
    tmt.getKVStore()->onServiceCommand(std::move(cmds), raft_ctx);
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

    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;

    addRequestsToRaftCmd(cmds.add_requests(), region_id, key, value, prewrite_ts, commit_ts, true);

    tmt.getKVStore()->onServiceCommand(std::move(cmds), raft_ctx);
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

    BatchCtrl(Int64 concurrent_id_, Int64 flush_num_, Int64 batch_num_, UInt64 min_strlen_, UInt64 max_strlen_, Context * context_,
        RegionPtr region_, HandleID handle_begin_, bool del_)
        : concurrent_id(concurrent_id_),
          flush_num(flush_num_),
          batch_num(batch_num_),
          min_strlen(min_strlen_),
          max_strlen(max_strlen_),
          context(context_),
          region(region_),
          handle_begin(handle_begin_),
          del(del_)
    {
        assert(max_strlen >= min_strlen);
        assert(min_strlen >= 1);
        auto str_len = static_cast<size_t>(random() % (max_strlen - min_strlen + 1) + min_strlen);
        default_str = String(str_len, '_');
    }

    void EncodeDatum(std::stringstream & ss, TiDB::CodecFlag flag, Int64 magic_num)
    {
        Int8 target = (magic_num % 70) + '0';
        ss << UInt8(flag);
        switch (flag)
        {
            case TiDB::CodecFlagJson:
                throw Exception("Not implented yet: BatchCtrl::EncodeDatum, TiDB::CodecFlagJson", ErrorCodes::LOGICAL_ERROR);
            case TiDB::CodecFlagMax:
                throw Exception("Not implented yet: BatchCtrl::EncodeDatum, TiDB::CodecFlagMax", ErrorCodes::LOGICAL_ERROR);
            case TiDB::CodecFlagDuration:
                throw Exception("Not implented yet: BatchCtrl::EncodeDatum, TiDB::CodecFlagDuration", ErrorCodes::LOGICAL_ERROR);
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
                return EncodeFloat64(Float64(magic_num) / 1111.1, ss);
            case TiDB::CodecFlagUInt:
                return EncodeUInt<UInt64>(UInt64(magic_num), ss);
            case TiDB::CodecFlagInt:
                return EncodeInt64(Int64(magic_num), ss);
            case TiDB::CodecFlagVarInt:
                return EncodeVarInt(Int64(magic_num), ss);
            case TiDB::CodecFlagVarUInt:
                return EncodeVarUInt(UInt64(magic_num), ss);
            default:
                throw Exception("Not implented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
        }
    }

    TiKVValue EncodeRow(const TiDB::TableInfo & table_info, Int64 magic_num)
    {
        std::stringstream ss;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            const TiDB::ColumnInfo & column = table_info.columns[i];
            EncodeDatum(ss, TiDB::CodecFlagInt, column.id);
            // TODO: May need to use BumpyDatum to flatten before encoding.
            EncodeDatum(ss, column.getCodecFlag(), magic_num);
        }
        return TiKVValue(ss.str());
    }
};

void batchInsert(const TiDB::TableInfo & table_info, std::unique_ptr<BatchCtrl> batch_ctrl, std::function<Int64(Int64)> fn_gen_magic_num)
{
    RegionPtr & region = batch_ctrl->region;

    TMTContext & tmt = batch_ctrl->context->getTMTContext();
    pingcap::pd::ClientPtr pd_client = tmt.getPDClient();

    Int64 index = batch_ctrl->handle_begin;

    for (Int64 flush_cnt = 0; flush_cnt < batch_ctrl->flush_num; ++flush_cnt)
    {
        UInt64 prewrite_ts = pd_client->getTS();
        UInt64 commit_ts = pd_client->getTS();

        RaftContext raft_ctx(batch_ctrl->context, nullptr, nullptr);
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest * cmd = cmds.add_requests();

        for (Int64 cnt = 0; cnt < batch_ctrl->batch_num; ++index, ++cnt)
        {
            TiKVKey key = RecordKVFormat::genKey(table_info.id, index);
            TiKVValue value = batch_ctrl->EncodeRow(table_info, fn_gen_magic_num(index));
            addRequestsToRaftCmd(cmd, region->id(), key, value, prewrite_ts, commit_ts, batch_ctrl->del);
        }

        tmt.getKVStore()->onServiceCommand(std::move(cmds), raft_ctx);
    }
}

void concurrentBatchInsert(const TiDB::TableInfo & table_info, Int64 concurrent_num, Int64 flush_num, Int64 batch_num, UInt64 min_strlen,
    UInt64 max_strlen, Context & context)
{
    TMTContext & tmt = context.getTMTContext();

    RegionID curr_max_region_id(InvalidRegionID);
    HandleID curr_max_handle_id = 0;
    tmt.getKVStore()->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        curr_max_region_id = (curr_max_region_id == InvalidRegionID) ? region_id : std::max<RegionID>(curr_max_region_id, region_id);
        auto range = region->getRange();
        curr_max_handle_id = std::max(RecordKVFormat::getHandle(range.second), curr_max_handle_id);
    });

    Int64 key_num_each_region = flush_num * batch_num;
    HandleID handle_begin = curr_max_handle_id;

    Regions regions = createRegions(table_info.id, concurrent_num, key_num_each_region, handle_begin, curr_max_region_id + 1);
    for (const RegionPtr & region : regions)
        tmt.getKVStore()->onSnapshot(region, &context);

    std::list<std::thread> threads;
    for (Int64 i = 0; i < concurrent_num; i++, handle_begin += key_num_each_region)
    {
        auto batch_ptr
            = std::make_unique<BatchCtrl>(i, flush_num, batch_num, min_strlen, max_strlen, &context, regions[i], handle_begin, false);
        threads.push_back(std::thread(&batchInsert, table_info, std::move(batch_ptr), [](Int64 index) -> Int64 { return index; }));
    }
    for (auto & thread : threads)
    {
        thread.join();
    }
}

Int64 concurrentRangeOperate(
    const TiDB::TableInfo & table_info, HandleID start_handle, HandleID end_handle, Context & context, Int64 magic_num, bool del)
{
    Regions regions;

    {
        TMTContext & tmt = context.getTMTContext();
        for (auto && [_, r] : tmt.getRegionTable().getRegionsByTable(table_info.id))
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
    for (auto region : regions)
    {
        auto [start_key, end_key] = region->getRange();
        auto ss = TiKVRange::getRangeHandle<true>(start_key, table_info.id);
        auto ee = TiKVRange::getRangeHandle<false>(end_key, table_info.id);
        TiKVRange::Handle handle_begin = std::max<TiKVRange::Handle>(ss, start_handle);
        TiKVRange::Handle handle_end = std::min<TiKVRange::Handle>(ee, end_handle);
        if (handle_end <= handle_begin)
            continue;
        Int64 batch_num = handle_end - handle_begin;
        tol += batch_num;
        auto batch_ptr = std::make_unique<BatchCtrl>(-1, 1, batch_num, 1, 1, &context, region, handle_begin.handle_id, del);
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


} // namespace RegionBench

} // namespace DB
