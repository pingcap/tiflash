//
// Created by schrodinger on 7/5/21.
//

#ifndef CLICKHOUSE_CHECKSUM_H
#define CLICKHOUSE_CHECKSUM_H
#include <Common/Exception.h>
#include <IO/HashingWriteBuffer.h>
#include <zlib.h>

#include <crc64.hpp>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <type_traits>
namespace DB::DM
{

enum class ChecksumAlgo : uint64_t
{
    None,
    CRC32,
    CRC64,
    City128
};

namespace Digest
{

class None
{
public:
    using HashType                   = std::array<uint8_t, 0>;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto  algorithm = ::DB::DM::ChecksumAlgo::None;
    void                   update(const void *, size_t) { ; }
    [[nodiscard]] HashType checksum() const { return {}; }
};

class CRC32
{
public:
    using HashType                   = uint32_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto  algorithm = ::DB::DM::ChecksumAlgo::CRC32;
    void                   update(const void * src, size_t length) { state = crc32(state, reinterpret_cast<const Bytef *>(src), length); }
    [[nodiscard]] HashType checksum() const { return static_cast<HashType>(~state); }

private:
    uLong state = 0xFFFFFFFF;
};

class City128
{
public:
    using HashType                    = unsigned __int128;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::DM::ChecksumAlgo::City128;
    void                  update(const void * src, size_t length)
    {
        state = CityHash_v1_0_2::CityHash128WithSeed(static_cast<const char *>(src), length, state);
    }
    [[nodiscard]] HashType checksum() const { return (static_cast<HashType>(state.first) << 64) | state.second; }

private:
    CityHash_v1_0_2::uint128 state = {0, 0};
};

class CRC64
{
public:
    using HashType                   = uint64_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto  algorithm = ::DB::DM::ChecksumAlgo::CRC64;
    void                   update(const void * src, size_t length) { state.update(src, length); }
    [[nodiscard]] HashType checksum() const { return state.checksum(); }

private:
    crc64::Digest state{};
};
} // namespace Digest

struct FixedChecksumFrame
{
    size_t  bytes;
    uint8_t checksum[64];
};

template <typename Algorithm>
struct ChecksumFrame
{
    size_t                       bytes;
    typename Algorithm::HashType checksum;
};

#define BASIC_CHECK_FOR_FRAME(ALGO)                                                                                        \
    static_assert(std::is_standard_layout_v<ChecksumFrame<Digest::ALGO>>, "DMChecksumFrame must be in standard-layout"); \
    static_assert(std::is_trivial_v<ChecksumFrame<Digest::ALGO>>, "DMChecksumFrame must be trivial");

BASIC_CHECK_FOR_FRAME(CRC32)
BASIC_CHECK_FOR_FRAME(CRC64)
BASIC_CHECK_FOR_FRAME(City128)
BASIC_CHECK_FOR_FRAME(None)
#undef BASIC_CHECK_FOR_FRAME

} // namespace DB::DM
#endif //CLICKHOUSE_CHECKSUM_H
