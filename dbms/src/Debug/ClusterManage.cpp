#include <Common/typeid_cast.h>
#include <Debug/ClusterManage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

static const std::string TABLE_SYNC_STATUS_PREFIX = "/tiflash/sync-status/";

uint8_t CheckHttpUriAvailable(BaseBuffView path_)
{
    std::string_view path(path_.data, path_.len);
    return path.size() > TABLE_SYNC_STATUS_PREFIX.size() && path.substr(0, TABLE_SYNC_STATUS_PREFIX.size()) == TABLE_SYNC_STATUS_PREFIX;
}

HttpRequestRes HandleHttpRequest(EngineStoreServerWrap * server, BaseBuffView path_)
{
    HttpRequestStatus status = HttpRequestStatus::Ok;
    TableID table_id = 0;
    {
        std::string_view path(path_.data, path_.len);
        if (CheckHttpUriAvailable(path_))
        {
            std::string table_id_str(path.substr(TABLE_SYNC_STATUS_PREFIX.size()));
            try
            {
                table_id = std::stoll(table_id_str);
            }
            catch (...)
            {
                status = HttpRequestStatus::ErrorParam;
            }
        }
        else
        {
            status = HttpRequestStatus::ErrorParam;
        }
        if (status != HttpRequestStatus::Ok)
            return HttpRequestRes{.status = status, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
    }

    WriteBufferFromOwnString ss;
    auto & tmt = *server->tmt;

    std::vector<RegionID> region_list;
    size_t count = 0;

    // if storage is not created in ch, flash replica should not be available.
    if (tmt.getStorages().get(table_id))
    {
        tmt.getRegionTable().handleInternalRegionsByTable(table_id, [&](const RegionTable::InternalRegions & regions) {
            count = regions.size();
            region_list.reserve(regions.size());
            for (const auto & region : regions)
                region_list.push_back(region.first);
        });
    }
    ss << count << std::endl;
    for (const auto & region_id : region_list)
        ss << region_id << ' ';
    ss << std::endl;

    auto s = RawCppString::New(ss.str());
    return HttpRequestRes{.status = status,
        .res = CppStrWithView{.inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String), .view = BaseBuffView{s->data(), s->size()}}};
}

inline std::string ToPdKey(const char * key, const size_t len)
{
    std::string res(len * 2, 0);
    size_t i = 0;
    for (size_t k = 0; k < len; ++k)
    {
        uint8_t o = key[k];
        res[i++] = o / 16;
        res[i++] = o % 16;
    }

    for (char & re : res)
    {
        if (re < 10)
            re = re + '0';
        else
            re = re - 10 + 'A';
    }
    return res;
}

inline std::string ToPdKey(const std::string & key) { return ToPdKey(key.data(), key.size()); }

inline std::string FromPdKey(const char * key, const size_t len)
{
    std::string res(len / 2, 0);
    for (size_t k = 0; k < len; k += 2)
    {
        int s[2];

        for (size_t i = 0; i < 2; ++i)
        {
            char p = key[k + i];
            if (p >= 'A')
                s[i] = p - 'A' + 10;
            else
                s[i] = p - '0';
        }

        res[k / 2] = s[0] * 16 + s[1];
    }
    return res;
}

void ClusterManage::findRegionByRange(Context & context, const ASTs & args, Printer output)
{
    enum Mode : UInt64
    {
        DEFAULT = 0,
        ID_LIST = 1,
    };

    if (args.size() < 2)
        throw Exception("Args not matched, should be: start-key, end-key", ErrorCodes::BAD_ARGUMENTS);

    Mode mode = DEFAULT;
    const auto start_key = safeGet<std::string>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    const auto end_key = safeGet<std::string>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    if (args.size() > 2)
        mode = static_cast<Mode>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();

    auto start = FromPdKey(start_key.data(), start_key.size());
    auto end = FromPdKey(end_key.data(), end_key.size());
    RegionMap regions;
    kvstore->handleRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(std::move(start), std::move(end)),
        [&regions](RegionMap regions_, const KVStoreTaskLock &) { regions = std::move(regions_); });

    output(toString(regions.size()));
    if (mode == ID_LIST)
    {
        WriteBufferFromOwnString ss;
        if (!regions.empty())
            ss << "regions: ";
        for (const auto & region : regions)
            ss << region.second->id() << ' ';
        output(ss.str());
    }
}

void ClusterManage::checkTableOptimize(DB::Context & context, const DB::ASTs & args, DB::Printer)
{
    if (args.size() < 2)
        throw Exception("Args not matched, should be: table-id, threshold", ErrorCodes::BAD_ARGUMENTS);

    auto & tmt = context.getTMTContext();
    TableID table_id = (TableID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto a = typeid_cast<const ASTLiteral &>(*args[1]).value.safeGet<DecimalField<Decimal32>>();
    tmt.getRegionTable().checkTableOptimize(table_id, a.getValue().toFloat<Float32>(a.getScale()));
}

} // namespace DB
