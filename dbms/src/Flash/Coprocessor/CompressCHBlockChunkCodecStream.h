#pragma once

#include <algorithm>
#include <boost/noncopyable.hpp>
#include <cstddef>
#include <functional>
#include <numeric>
#include <vector>

#include "Columns/IColumn.h"
#include "CompressedCHBlockChunkCodec.h"

namespace DB
{

struct WriteBufferFromOwnStringList
    : public WriteBuffer
    , public boost::noncopyable
{
    WriteBufferFromOwnStringList()
        : WriteBuffer(nullptr, 0)
    {
    }

    void nextImpl() override
    {
        buffs.emplace_back(std::string(DBMS_DEFAULT_BUFFER_SIZE, 0));
        WriteBuffer::set(buffs.back().data(), buffs.back().size());
    };

    std::string getString()
    {
        next();

        std::string res;
        size_t sz = std::accumulate(buffs.begin(), buffs.end(), 0, [](const auto r, const auto & s) {
            return r + s.size();
        });
        res.resize(sz);
        char * start = res.data();
        std::for_each(buffs.begin(), buffs.end(), [&](auto & s) {
            std::memcpy(start, s.data(), s.size());
            start += s.size();
            s.clear();
        });

        clear();
        return res;
    }

    void clear()
    {
        buffs.clear();
        WriteBuffer::set(nullptr, 0);
    }

    std::vector<std::string> buffs;
};

struct CompressCHBlockChunkCodecStream
{
    explicit CompressCHBlockChunkCodecStream(CompressionMethod compress_method_)
        : compress_method(compress_method_)
    {
        output_buffer = std::make_unique<WriteBufferFromOwnStringList>();
        compress_write_buffer = std::make_unique<CompressedCHBlockChunkCodec::CompressedWriteBuffer>(*output_buffer, CompressionSettings(compress_method));
    }
    void clear()
    {
        compress_write_buffer->next();
        output_buffer->clear();
    }

    WriteBuffer * getWriterWithoutCompress() const
    {
        return output_buffer.get();
    }

    WriteBuffer * getWriter()
    {
        return compress_write_buffer.get();
    }

    std::string getString()
    {
        if (compress_write_buffer == nullptr)
        {
            throw Exception("The output should not be null in getString()");
        }
        compress_write_buffer->next();
        output_buffer->next();
        return output_buffer->getString();
    }
    ~CompressCHBlockChunkCodecStream() = default;

    // void disableCompress() { enable_compress = false; }
    // void enableCompress() { enable_compress = true; }

    void encodeHeader(const Block & header, size_t rows);
    void encodeColumn(const ColumnPtr & column, const ColumnWithTypeAndName & type_name);

    // bool enable_compress{true};
    CompressionMethod compress_method;
    std::unique_ptr<WriteBufferFromOwnStringList> output_buffer{};
    std::unique_ptr<CompressedCHBlockChunkCodec::CompressedWriteBuffer> compress_write_buffer{};
};

void DecodeColumns(ReadBuffer & istr, Block & res, size_t columns, size_t rows, size_t reserve_size = 0);
Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & rows);

std::unique_ptr<CompressCHBlockChunkCodecStream> NewCompressCHBlockChunkCodecStream(CompressionMethod compress_method);
} // namespace DB