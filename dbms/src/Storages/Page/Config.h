#pragma once

// todo move this util into other dir
#include <Core/Types.h>
#include <Storages/Page/PageUtil.h>

namespace DB::MVCC
{
// MVCC config
struct VersionSetConfig
{
    size_t compact_hint_delta_deletions = 5000;
    size_t compact_hint_delta_entries = 200 * 1000;

    void setSnapshotCleanupProb(UInt32 prob)
    {
        // Range from [0, 1000)
        prob = std::max(1U, prob);
        prob = std::min(1000U, prob);

        prob_cleanup_invalid_snapshot = prob;
    }

    bool doCleanup() const { return PageUtil::randInt(0, 1000) < prob_cleanup_invalid_snapshot; }

private:
    // Probability to cleanup invalid snapshots. 10 out of 1000 by default.
    size_t prob_cleanup_invalid_snapshot = 10;
};
} // namespace DB::MVCC