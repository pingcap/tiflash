//
// Created by schrodinger on 7/5/21.
//

#ifndef CLICKHOUSE_CHECKSUMBUFFER_H
#define CLICKHOUSE_CHECKSUMBUFFER_H
#include <IO/HashingWriteBuffer.h>
#include "DMChecksum.h"

namespace DB::DM::Checksum
{
template <typename Backend, typename Buffer>
class IDigestBuffer : public BufferWithOwnMemory<Buffer>
{
public:
    using HashType = typename Backend::HashType;

    explicit IDigestBuffer(size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_, nullptr, 512), block_pos(0), block_size(block_size_)
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

/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
template <typename Backend>
class DigestReadBuffer : public IDigestBuffer<Backend, ReadBuffer>
{
public:
    explicit DigestReadBuffer(ReadBuffer & in_, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE) :
        IDigestBuffer<Backend, ReadBuffer>(block_size), in(in_)
    {
        this->working_buffer = in.buffer();
        this->pos = in.position();

        /// calculate hash from the data already read
        if (this->working_buffer.size())
        {
            this->calculateHash(this->pos, this->working_buffer.end() - this->pos);
        }
    }

private:
    bool nextImpl() override
    {
        in.position() = this->pos;
        bool res = in.next();
        this->working_buffer = in.buffer();
        this->pos = in.position();

        this->calculateHash(this->working_buffer.begin(), this->working_buffer.size());

        return res;
    }

private:
    ReadBuffer & in;
};

} // namespace DB::DM::Checksum


#endif //CLICKHOUSE_CHECKSUMBUFFER_H
