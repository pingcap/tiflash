#include <tikv/Backoff.h>
#include <common/CltException.h>

namespace pingcap {
namespace kv {

BackoffPtr newBackoff(BackoffType tp) {
    switch(tp) {
    case boTiKVRPC:
        return std::make_shared<Backoff>(100, 2000, EqualJitter);
    case boTxnLock:
        return std::make_shared<Backoff>(200, 3000, EqualJitter);
    case boTxnLockFast:
        return std::make_shared<Backoff>(100, 3000, EqualJitter);
    case boPDRPC:
        return std::make_shared<Backoff>(500, 3000, EqualJitter);
    case boRegionMiss:
        return std::make_shared<Backoff>(2, 500, NoJitter);
    case boUpdateLeader:
        return std::make_shared<Backoff>(1, 10, NoJitter);
    case boServerBusy:
        return std::make_shared<Backoff>(2000, 10000, EqualJitter);
    }
    return nullptr;
}

Exception Type2Exception(BackoffType tp) {
    switch(tp) {
        case boTiKVRPC:
            return Exception("TiKV Timeout", TimeoutError);
        case boTxnLock:
        case boTxnLockFast:
            return Exception("Resolve lock Timeout", TimeoutError);
        case boPDRPC:
            return Exception("PD Timeout", TimeoutError);
        case boRegionMiss:
        case boUpdateLeader:
            return Exception("Region Unavaliable", RegionUnavailable);
        case boServerBusy:
            return Exception("TiKV Server Busy", TimeoutError);
    }
    return Exception("Unknown Exception, tp is :" + std::to_string(tp));
}

void Backoffer::backoff(BackoffType tp, const Exception & exc) {
    if (exc.code() == MismatchClusterIDCode) {
        exc.rethrow();
    }

    BackoffPtr bo;
    auto it = backoff_map.find(tp);
    if (it != backoff_map.end()) {
        bo = it -> second;
    } else {
        bo = newBackoff(tp);
        backoff_map[tp] = bo;
    }
    total_sleep += bo->sleep();
    if (max_sleep > 0 && total_sleep > max_sleep) {
        throw Type2Exception(tp);
    }
}

}
}
