
#pragma once

#include <Common/HashTable/Hash.h>
#include <pingcap/pd/Types.h>

namespace DB
{
using ResourceGroupWithKeyspace = std::pair<pingcap::pd::KeyspaceID, std::string>;
struct PairHash
{
    size_t operator()(const std::pair<pingcap::pd::KeyspaceID, std::string> & p) const
    {
        uint64_t seed = 0;
        hash_combine(seed, p.first);
        hash_combine(seed, p.second);
        return static_cast<size_t>(seed);
    }
};
} // namespace DB
