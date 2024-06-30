// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <city.h>
#include <common/crc64.h>
#ifdef __x86_64__
#include <xxh_x86dispatch.h>
#else
#include <xxh3.h>
#endif
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <type_traits>

namespace ProfileEvents
{
extern const Event ChecksumDigestBytes;
} // namespace ProfileEvents

namespace DB
{
enum class ChecksumAlgo : uint64_t
{
    None,
    CRC32,
    CRC64,
    City128,
    XXH3,
};

namespace Digest
{
class None
{
public:
    using HashType = std::byte;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::ChecksumAlgo::None;
    static void update(const void *, size_t length)
    {
        ProfileEvents::increment(ProfileEvents::ChecksumDigestBytes, length);
    }
    [[nodiscard]] static HashType checksum() { return std::byte{0}; }
};

class CRC32
{
public:
    using HashType = uint32_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::ChecksumAlgo::CRC32;
    void update(const void * src, size_t length)
    {
        ProfileEvents::increment(ProfileEvents::ChecksumDigestBytes, length);
        state = crc32(state, reinterpret_cast<const Bytef *>(src), length);
    }
    [[nodiscard]] HashType checksum() const { return state; }

private:
    uLong state = 0;
};

class City128
{
public:
    using HashType = unsigned __int128;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::ChecksumAlgo::City128;
    void update(const void * src, size_t length)
    {
        ProfileEvents::increment(ProfileEvents::ChecksumDigestBytes, length);
        state = CityHash_v1_0_2::CityHash128WithSeed(static_cast<const char *>(src), length, state);
    }
    [[nodiscard]] HashType checksum() const { return (static_cast<HashType>(state.first) << 64) | state.second; }

private:
    CityHash_v1_0_2::uint128 state = {0, 0};
};

class CRC64
{
public:
    using HashType = uint64_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::ChecksumAlgo::CRC64;
    void update(const void * src, size_t length)
    {
        ProfileEvents::increment(ProfileEvents::ChecksumDigestBytes, length);
        state.update(src, length);
    }
    [[nodiscard]] HashType checksum() const { return state.checksum(); }

private:
    crc64::Digest state{};
};

class XXH3
{
public:
    using HashType = XXH64_hash_t;
    static constexpr size_t hash_size = sizeof(HashType);
    static constexpr auto algorithm = ::DB::ChecksumAlgo::XXH3;
    void update(const void * src, size_t length)
    {
        ProfileEvents::increment(ProfileEvents::ChecksumDigestBytes, length);
#ifdef __x86_64__ // dispatched version can utilize hardware resource
        state = XXH3_64bits_withSeed_dispatch(src, length, state);
#else // use inlined version
        state = XXH_INLINE_XXH3_64bits_withSeed(src, length, state);
#endif
    }
    [[nodiscard]] HashType checksum() const { return state; }

private:
    XXH64_hash_t state = 0;
};
} // namespace Digest

template <typename Algorithm>
struct ChecksumFrame
{
    size_t bytes;
    typename Algorithm::HashType checksum;
    // clang-format off
    uint8_t pad[alignof(size_t) > alignof(typename Algorithm::HashType)
                ? alignof(size_t) - sizeof(typename Algorithm::HashType)
                : 0];
    // clang-format on
    uint8_t data[0];
};

#define BASIC_CHECK_FOR_FRAME(ALGO)                             \
    static_assert(                                              \
        std::is_standard_layout_v<ChecksumFrame<Digest::ALGO>>, \
        "DMChecksumFrame must be in standard-layout");          \
    static_assert(std::is_trivial_v<ChecksumFrame<Digest::ALGO>>, "DMChecksumFrame must be trivial");

BASIC_CHECK_FOR_FRAME(CRC32)
BASIC_CHECK_FOR_FRAME(CRC64)
BASIC_CHECK_FOR_FRAME(City128)
BASIC_CHECK_FOR_FRAME(None)
BASIC_CHECK_FOR_FRAME(XXH3)
#undef BASIC_CHECK_FOR_FRAME

using FrameUnion = std::aligned_union_t<
    256,
    ChecksumFrame<Digest::None>,
    ChecksumFrame<Digest::CRC32>,
    ChecksumFrame<Digest::CRC64>,
    ChecksumFrame<Digest::City128>,
    ChecksumFrame<Digest::XXH3>>;


struct UnifiedDigestBase
{
    virtual void update(const void * data, size_t length) = 0;
    virtual bool compareRaw(std::string_view data) = 0;
    virtual bool compareRaw(const void * data) = 0;
    virtual bool compareFrame(const FrameUnion & frame) = 0;
    [[nodiscard]] virtual std::string raw() const = 0;
    virtual ~UnifiedDigestBase() = default;
    virtual size_t hashSize() const = 0;
    virtual size_t headerSize() const = 0;
    virtual void reset() = 0;
    template <class T>
    void update(const T & val)
    {
        update(std::addressof(val), sizeof(T));
    }
};

template <class Backend>
class UnifiedDigest : public UnifiedDigestBase
{
public:
    void update(const void * data, size_t length) override { backend.update(data, length); }

    bool compareRaw(const void * data) override
    {
        auto checksum = backend.checksum();
        return std::memcmp(data, &checksum, sizeof(checksum)) == 0;
    }

    bool compareRaw(const std::string_view data) override
    {
        auto checksum = backend.checksum();
        return data.length() == sizeof(checksum) && ::memcmp(data.begin(), &checksum, sizeof(checksum)) == 0;
    }

    bool compareFrame(const FrameUnion & frame) override
    {
        auto checksum = backend.checksum();
        auto real_frame = reinterpret_cast<const ChecksumFrame<Backend> &>(frame);
        return checksum == real_frame.checksum;
    }

    [[nodiscard]] std::string raw() const override
    {
        auto checksum = backend.checksum();
        std::string data(sizeof(checksum), '\0');
        ::memcpy(data.data(), &checksum, sizeof(checksum));
        return data;
    }

    [[nodiscard]] size_t hashSize() const override { return Backend::hash_size; }
    [[nodiscard]] size_t headerSize() const override { return sizeof(ChecksumFrame<Backend>); }
    void reset() override { backend = Backend{}; }

private:
    Backend backend{};
};

using UnifiedDigestBaseBox = std::unique_ptr<UnifiedDigestBase>;
using UnifiedDigestBasePtr = std::shared_ptr<UnifiedDigestBase>;

} // namespace DB
