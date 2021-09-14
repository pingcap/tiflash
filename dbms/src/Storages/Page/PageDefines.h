#pragma once

#include <Core/Defines.h>
#include <Core/Types.h>

#include <chrono>
#include <unordered_set>
#include <vector>

namespace DB
{
using Clock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;

static constexpr UInt64 MB = 1ULL * 1024 * 1024;
static constexpr UInt64 GB = MB * 1024;

static constexpr UInt64 PAGE_SIZE_STEP = (1 << 10) * 16; // 16 KB
static constexpr UInt64 PAGE_BUFFER_SIZE = DBMS_DEFAULT_BUFFER_SIZE;
static constexpr UInt64 PAGE_MAX_BUFFER_SIZE = 128 * MB;
static constexpr UInt64 PAGE_SPLIT_SIZE = 1 * MB;
static constexpr UInt64 PAGE_FILE_MAX_SIZE = 1024 * 2 * MB;
static constexpr UInt64 PAGE_FILE_SMALL_SIZE = 2 * MB;
static constexpr UInt64 PAGE_FILE_ROLL_SIZE = 128 * MB;
static constexpr UInt64 PAGE_META_ROLL_SIZE = 2 * MB;

static_assert(PAGE_SIZE_STEP >= ((1 << 10) * 16), "PAGE_SIZE_STEP should be at least 16 KB");
static_assert((PAGE_SIZE_STEP & (PAGE_SIZE_STEP - 1)) == 0, "PAGE_SIZE_STEP should be power of 2");
static_assert(PAGE_BUFFER_SIZE % PAGE_SIZE_STEP == 0, "PAGE_BUFFER_SIZE should be dividable by PAGE_SIZE_STEP");

using PageId = UInt64;
using PageIds = std::vector<PageId>;
using PageIdSet = std::unordered_set<PageId>;

using PageFieldOffset = UInt64;
using PageFieldOffsets = std::vector<PageFieldOffset>;
using PageFieldSizes = std::vector<UInt64>;

using PageFieldOffsetChecksums = std::vector<std::pair<PageFieldOffset, UInt64>>;

using PageFileId = UInt64;
using PageFileLevel = UInt32;
using PageFileIdAndLevel = std::pair<PageFileId, PageFileLevel>;
using PageFileIdAndLevels = std::vector<PageFileIdAndLevel>;

using PageSize = UInt64;

struct ByteBuffer
{
    using Pos = char *;

    ByteBuffer() = default;
    ByteBuffer(Pos begin_pos_, Pos end_pos_)
        : begin_pos(begin_pos_)
        , end_pos(end_pos_)
    {}

    inline Pos begin() const { return begin_pos; }
    inline Pos end() const { return end_pos; }
    inline size_t size() const { return end_pos - begin_pos; }

private:
    Pos begin_pos;
    Pos end_pos; /// 1 byte after the end of the buffer
};

/// https://stackoverflow.com/a/13938417
inline size_t alignPage(size_t n)
{
    return (n + PAGE_SIZE_STEP - 1) & ~(PAGE_SIZE_STEP - 1);
}

} // namespace DB
