#include <Common/typeid_cast.h>
#include <Debug/ClusterManage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes


HttpRequestRes HandleHttpRequestSyncStatus(EngineStoreServerWrap * server, BaseBuffView path_, const std::string & method_name)
{
    HttpRequestStatus status = HttpRequestStatus::Ok;
    TableID table_id = 0;
    {
        std::string_view path(path_.data, path_.len);

        std::string table_id_str(path.substr(method_name.size()));
        try
        {
            table_id = std::stoll(table_id_str);
        }
        catch (...)
        {
            status = HttpRequestStatus::ErrorParam;
        }

        if (status != HttpRequestStatus::Ok)
            return HttpRequestRes{.status = status, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
    }

    std::stringstream ss;
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

HttpRequestRes HandleHttpRequestStoreStatus(EngineStoreServerWrap * server, BaseBuffView, const std::string &)
{
    auto name = RawCppString::New(IntoStoreStatusName(server->tmt->getStoreStatus(std::memory_order_relaxed)));
    return HttpRequestRes{.status = HttpRequestStatus::Ok,
        .res = CppStrWithView{.inner = GenRawCppPtr(name, RawCppPtrTypeImpl::String), .view = BaseBuffView{name->data(), name->size()}}};
}

typedef HttpRequestRes (*HANDLE_HTTP_URI_METHOD)(EngineStoreServerWrap *, BaseBuffView, const std::string &);
static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI
    = {{"/tiflash/sync-status/", HandleHttpRequestSyncStatus}, {"/tiflash/store-status", HandleHttpRequestStoreStatus}};

uint8_t CheckHttpUriAvailable(BaseBuffView path_)
{
    std::string_view path(path_.data, path_.len);
    for (auto & [str, method] : AVAILABLE_HTTP_URI)
    {
        std::ignore = method;
        if (path.size() >= str.size() && path.substr(0, str.size()) == str)
            return true;
    }
    return false;
}

HttpRequestRes HandleHttpRequest(EngineStoreServerWrap * server, BaseBuffView path_)
{
    std::string_view path(path_.data, path_.len);
    for (auto & [str, method] : AVAILABLE_HTTP_URI)
    {
        if (path.size() >= str.size() && path.substr(0, str.size()) == str)
        {
            return method(server, path_, str);
        }
    }
    return HttpRequestRes{.status = HttpRequestStatus::ErrorParam, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
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
        std::stringstream ss;
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
