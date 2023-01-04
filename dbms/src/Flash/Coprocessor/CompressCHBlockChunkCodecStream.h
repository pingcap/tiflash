#pragma once

#include <algorithm>
#include <boost/noncopyable.hpp>
#include <cstddef>
#include <functional>
#include <numeric>
#include <utility>
#include <vector>

#include "Columns/IColumn.h"
#include "CompressedCHBlockChunkCodec.h"

namespace DB
{

struct WriteBufferFromOwnStringList final
    : public WriteBuffer
    , public boost::noncopyable
{
    WriteBufferFromOwnStringList()
        : WriteBuffer(nullptr, 0)
    {
        reset();
    }

    void nextImpl() override
    {
        buffs.emplace_back(std::string(128 * 1024, 0)); // 128KB
        WriteBuffer::set(buffs.back().data(), buffs.back().size());
    };

    std::string getString()
    {
        next();

        if (!buffs.empty() && buffs[0].size() >= bytes)
        {
            buffs[0].resize(bytes);
            return std::move(buffs[0]);
        }

        std::string res;
        res.resize(bytes);
        for (size_t sz = 0; sz < bytes;)
        {
            for (auto && s : buffs)
            {
                if (sz + s.size() < bytes)
                {
                    std::memcpy(res.data() + sz, s.data(), s.size());
                    sz += s.size();
                }
                else
                {
                    std::memcpy(res.data() + sz, s.data(), bytes - sz);
                    sz = bytes;
                    break;
                }
            }
        }
        return res;
    }

    void reset()
    {
        buffs.clear();
        nextImpl();
        bytes = 0;
    }

    std::vector<std::string> buffs;
};

struct CompressCHBlockChunkCodecStream
{
    explicit CompressCHBlockChunkCodecStream(CompressionMethod compress_method_)
        : compress_method(compress_method_)
    {
        output_buffer = std::make_unique<WriteBufferFromOwnStringList>();
        compress_write_buffer = std::make_unique<CompressedCHBlockChunkCodec::CompressedWriteBuffer>(
            *output_buffer,
            CompressionSettings(compress_method),
            DBMS_DEFAULT_BUFFER_SIZE);
    }

    void reset() const
    {
        compress_write_buffer->next();
        output_buffer->reset();
    }

    WriteBufferFromOwnStringList * getWriterWithoutCompress() const
    {
        return output_buffer.get();
    }

    CompressedCHBlockChunkCodec::CompressedWriteBuffer * getWriter() const
    {
        return compress_write_buffer.get();
    }

    std::string getString() const
    {
        if (compress_write_buffer == nullptr)
        {
            throw Exception("The output should not be null in getString()");
        }
        compress_write_buffer->next();
        return output_buffer->getString();
    }
    ~CompressCHBlockChunkCodecStream() = default;

    // bool enable_compress{true};
    CompressionMethod compress_method;
    std::unique_ptr<WriteBufferFromOwnStringList> output_buffer{};
    std::unique_ptr<CompressedCHBlockChunkCodec::CompressedWriteBuffer> compress_write_buffer{};
};

void EncodeHeader(WriteBuffer & ostr, const Block & header, size_t rows);
void EncodeColumn__(WriteBuffer & ostr, const ColumnPtr & column, const ColumnWithTypeAndName & type_name);
void DecodeColumns(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size = 0);
Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & rows);

std::unique_ptr<CompressCHBlockChunkCodecStream> NewCompressCHBlockChunkCodecStream(CompressionMethod compress_method);
} // namespace DB