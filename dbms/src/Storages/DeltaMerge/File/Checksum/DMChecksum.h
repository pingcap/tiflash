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
    static constexpr auto  algorithm = ::DB::DM::CheckSumAlgo::CRC32;
    void                   update(const void * src, size_t length) { state = crc32(state, reinterpret_cast<const Bytef *>(src), length); }
    [[nodiscard]] HashType checksum() const { return static_cast<HashType>(~state); }

private:
    uLong state = 0xFFFFFFFF;
};

class City128
{
public:
    using HashType                  = CityHash_v1_0_2::uint128;
    static constexpr auto algorithm = ::DB::DM::CheckSumAlgo::City128;
    void                  update(const void * src, size_t length)
    {
        state = CityHash_v1_0_2::CityHash128WithSeed(static_cast<const char *>(src), length, state);
    }
    [[nodiscard]] HashType checksum() const { return state; }

private:
    HashType state = {0, 0};
};

class CRC64
{
public:
    using HashType                   = uint64_t;
    static constexpr auto  algorithm = ::DB::DM::CheckSumAlgo::CRC64;
    void                   update(const void * src, size_t length) { state.update(src, length); }
    [[nodiscard]] HashType checksum() const { return state.checksum(); }

private:
    crc64::Digest state{};
};


template <typename Backend, typename Buffer>
class IDigestBuffer : public BufferWithOwnMemory<Buffer>
{
public:
    using HashType = typename Backend::HashType;

    explicit IDigestBuffer(size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_), block_pos(0), block_size(block_size_)
    {
    }

    virtual HashType getHash()
    {
        if (block_pos)
            state.update(&BufferWithOwnMemory<Buffer>::memory[0], block_pos);

        return state.checksum();
    }

    void append(DB::BufferBase::Position data) { state.update(data, block_size); }

    /// computation of the hash depends on the partitioning of blocks
    /// so you need to compute a hash of n complete pieces and one incomplete
    void calculateHash(DB::BufferBase::Position data, size_t len);

protected:
    size_t  block_pos;
    size_t  block_size;
    Backend state{};
};

/** Computes the hash from the data to write and passes it to the specified WriteBuffer.
  * The buffer of the nested WriteBuffer is used as the main buffer.
  */
template <typename Backend>
class DigestWriteBuffer : public IDigestBuffer<Backend, WriteBuffer>
{
private:
    WriteBuffer & out;

    void nextImpl() override
    {
        size_t len = this->offset();

        auto data = this->working_buffer.begin();
        this->calculateHash(data, len);

        out.position() = this->pos;
        out.next();
        this->working_buffer = out.buffer();
    }

public:
    using HashType = typename Backend::HashType;

    explicit DigestWriteBuffer(WriteBuffer & out_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IDigestBuffer<Backend, DB::WriteBuffer>(block_size_), out(out_)
    {
        out.next(); /// If something has already been written to `out` before us, we will not let the remains of this data affect the hash.
        this->working_buffer = out.buffer();
        this->pos            = this->working_buffer.begin();
    }

    HashType getHash() override
    {
        this->next();
        return IDigestBuffer<Backend, WriteBuffer>::getHash();
    }
};


} // namespace Digest

static inline size_t getChecksumLength(CheckSumAlgo algo)
{
    switch (algo)
    {
    case CheckSumAlgo::CRC32:
        return 4;
    case CheckSumAlgo::CRC64:
        return 8;
    case CheckSumAlgo::City128:
        return 16;
    default:
        std::stringstream ss;
        ss << "Checksum Algorithm with id: " << static_cast<size_t>(algo) << " cannot be recognized";
        throw Exception(ss.str());
    }
};

typedef struct __attribute__((packed))
{
    CheckSumAlgo algorithm;
    uint8_t      _padding[128 - sizeof(CheckSumAlgo)]; // in case there are additional information;
} DMChecksumMeta;                                      // used once in the beginning of file

static_assert(std::is_standard_layout_v<DMChecksumMeta>, "DMChecksumMeta must be in standard-layout");
static_assert(std::is_trivial_v<DMChecksumMeta>, "DMChecksumMeta must be trivial");
static_assert(sizeof(DMChecksumMeta) == 128, "DMChecksumMeta should be of 128 bytes");


typedef struct __attribute__((packed))
{
    uint8_t data[0];
} DMChecksumBlock;

static_assert(std::is_standard_layout_v<DMChecksumBlock>, "DMChecksumBlock must be in standard-layout");
static_assert(std::is_trivial_v<DMChecksumBlock>, "DMChecksumBlock must be trivial");

} // namespace DB::DM
#endif //CLICKHOUSE_DMCHECKSUM_H
