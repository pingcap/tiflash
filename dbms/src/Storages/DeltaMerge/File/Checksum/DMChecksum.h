//
// Created by schrodinger on 7/5/21.
//

#ifndef CLICKHOUSE_DMCHECKSUM_H
#define CLICKHOUSE_DMCHECKSUM_H
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

enum class CheckSumAlgo : uint8_t
{
    CRC32,
    CRC64,
    City128
};

namespace Digest
{

class CRC32
{
public:
    using HashType                   = uint32_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto  algorithm = ::DB::DM::CheckSumAlgo::CRC32;
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
    static constexpr auto algorithm = ::DB::DM::CheckSumAlgo::City128;
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
    static constexpr auto  algorithm = ::DB::DM::CheckSumAlgo::CRC64;
    void                   update(const void * src, size_t length) { state.update(src, length); }
    [[nodiscard]] HashType checksum() const { return state.checksum(); }

private:
    crc64::Digest state{};
};
} // namespace Digest

struct DMChecksumMeta
{
    CheckSumAlgo algorithm;
    uint8_t      _padding[128 - sizeof(CheckSumAlgo)]; // in case there are additional information;
};                                                     // used once in the beginning of file

static_assert(std::is_standard_layout_v<DMChecksumMeta>, "DMChecksumMeta must be in standard-layout");
static_assert(std::is_trivial_v<DMChecksumMeta>, "DMChecksumMeta must be trivial");
static_assert(sizeof(DMChecksumMeta) == 128, "DMChecksumMeta should be of 128 bytes");

template <typename Algorithm>
struct DMChecksumFrame
{
    size_t                       bytes;
    typename Algorithm::HashType checksum;
};

#define BASIC_CHECK_FOR_FRAME(ALGO)                                                                                        \
    static_assert(std::is_standard_layout_v<DMChecksumFrame<Digest::ALGO>>, "DMChecksumFrame must be in standard-layout"); \
    static_assert(std::is_trivial_v<DMChecksumFrame<Digest::ALGO>>, "DMChecksumFrame must be trivial");

BASIC_CHECK_FOR_FRAME(CRC32)
BASIC_CHECK_FOR_FRAME(CRC64)
BASIC_CHECK_FOR_FRAME(City128)


} // namespace DB::DM
#endif //CLICKHOUSE_DMCHECKSUM_H
