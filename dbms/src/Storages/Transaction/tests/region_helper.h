#pragma once
#include <Storages/Transaction/RegionPersister.h>

using namespace DB;

#define ASSERT_CHECK(cond, res)                                 \
    do                                                          \
    {                                                           \
        if (!(cond))                                            \
        {                                                       \
            std::cerr << __FILE__ << ":" << __LINE__ << ":"     \
                      << " Assertion " << #cond << " failed.\n"; \
            if ((res))                                          \
            {                                                   \
                (res) = false;                                  \
            }                                                   \
        }                                                       \
    } while (0)

#define ASSERT_CHECK_EQUAL(a, b, res)                                        \
    do                                                                       \
    {                                                                        \
        if (!(a == b))                                                       \
        {                                                                    \
            std::cerr << __FILE__ << ":" << __LINE__ << ":"                  \
                      << " Assertion " << #a << " == " << #b << " failed.\n"; \
            if ((res))                                                       \
            {                                                                \
                (res) = false;                                               \
            }                                                                \
        }                                                                    \
    } while (0)


inline metapb::Peer createPeer(UInt64 id, bool is_learner)
{
    metapb::Peer peer;
    peer.set_id(id);
    peer.set_is_learner(is_learner);
    return peer;
}

inline metapb::Region createRegionInfo(UInt64 id, const std::string start_key, const std::string end_key)
{
    metapb::Region region_info;
    region_info.set_id(id);
    region_info.set_start_key(start_key);
    region_info.set_end_key(end_key);

    *(region_info.mutable_peers()->Add()) = createPeer(1, true);
    *(region_info.mutable_peers()->Add()) = createPeer(2, false);

    return region_info;
}

inline RegionMeta createRegionMeta(UInt64 id)
{
    return RegionMeta(createPeer(31, true), createRegionInfo(id, "", ""), initialApplyState());
}
