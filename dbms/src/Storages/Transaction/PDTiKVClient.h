#include <pingcap/pd/IClient.h>
#include <pingcap/kv/Backoff.h>

namespace DB
{

// TODO:: Implement Read Index Client;

struct PDClientHelper
{

    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    static uint64_t getGCSafePointWithRetry(pingcap::pd::ClientPtr pd_client)
    {
        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                auto safe_point = pd_client->getGCSafePoint();
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }
};


} // namespace DB
