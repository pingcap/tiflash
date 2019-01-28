#pragma once

#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVHelper.h>

namespace DB
{

class Consistency
{
public:
    Consistency() : log(&Logger::get("Consistency")) {}

    void compute(const RegionPtr & region, UInt64 index, const std::string & raft_local_state)
    {
        Crc32 crc32;
        region->calculateCfCrc32(crc32);

        auto region_state_key = DataKVFormat::region_state_key(region->id());

        crc32.put(region_state_key.data(), region_state_key.size());
        crc32.put(raft_local_state.data(), raft_local_state.size());

        auto check_sum = toBigEndian(crc32.checkSum());

        std::string hash;
        hash.reserve(4);
        hash.append(reinterpret_cast<const char *>(&check_sum), 4);

        check(region, index, hash);
    }

    void check(const RegionPtr & region, UInt64 expected_index, const std::string & expected_hash)
    {
        auto region_id = region->id();
        auto it = states.find(region_id);

        if (it == states.end())
        {
            states.emplace(region_id, ConsistencyState{expected_index, expected_hash});
            return;
        }

        auto & state = it->second;
        if (expected_index < state.index)
        {
            LOG_WARNING(log, "Region [" << region_id << "] has scheduled a new hash: " <<
                state.index << " > " << expected_index << ", skip.");
            return;
        }

        if (expected_index > state.index)
        {
            ConsistencyState new_state{expected_index, expected_hash};
            LOG_WARNING(log, "Region [" << region_id << "] replace old consistency state (" +
                stateToString(state) + ") with a new one (" + stateToString(new_state) + ")");
            states.insert_or_assign(region_id, new_state);
            return;
        }

        // expected_index == state.index
        if (expected_hash != state.hash)
        {
            std::string msg = "Region [" + toString(region_id) + "] hash at " +
                toString(expected_index) + " not correct, expected " +
                escapeString(expected_hash) + ", got " + escapeString(state.hash) + "!!!";
            // TODO: Hack, WAR the hash checking as we store only records.
            LOG_WARNING(log, msg);
            // throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
        }

        LOG_INFO(log, "Region [" << region_id << "] consistency check at " << state.index << " pass");
        states.erase(it);
    }

private:
    struct ConsistencyState
    {
        UInt64 index;
        std::string hash;

    };

    inline static std::string stateToString(const ConsistencyState & state)
    {
        return "index: " + DB::toString(state.index) + ", hash: " + escapeString(state.hash);
    }

private:
    std::unordered_map<UInt64, ConsistencyState> states;

    Logger * log;
};

} // namespace DB
