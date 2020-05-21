#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <common/Types.h>
#include <math.h>

#include <ext/scope_guard.h>
#include <roaring.hh>

namespace DB
{

template <typename T>
class UniqueRoaringBitmap
{
    static_assert(std::is_integral_v<T>);

private:
    using Value = typename std::conditional_t<sizeof(T) <= 4, UInt32, UInt64>;
    using Bitmap = typename std::conditional_t<sizeof(T) <= 4, Roaring, Roaring64Map>;

    Bitmap bitmap;

public:
    UniqueRoaringBitmap() {}

    ~UniqueRoaringBitmap() {}

    void insert(T v) { bitmap.add((Value)v); }

    size_t size() const { return bitmap.cardinality(); }

    void merge(const UniqueRoaringBitmap<T> & rhs) { bitmap |= rhs.bitmap; }

    void write(DB::WriteBuffer & wb) const
    {
        static Allocator<false> allocator;

        auto bitmap_size = bitmap.getSizeInBytes();
        writeVarUInt(0, wb);
        writeVarUInt(bitmap_size, wb);
        auto & buf = wb.buffer();
        size_t remain_buf_size = buf.end() - wb.position();
        if (remain_buf_size >= bitmap_size)
        {
            // Directly copy to working buffer.
            auto actual_write = bitmap.write(wb.position());
            if (unlikely(actual_write != bitmap_size))
                throw Exception("Actual written bytes and bitmap size not equal");
            wb.position() += actual_write;
            wb.nextIfAtEnd();
        }
        else
        {
            char * buf = (char *)allocator.alloc(bitmap_size);
            SCOPE_EXIT({ allocator.free(buf, bitmap_size); });

            auto actual_write = bitmap.write(buf);
            if (unlikely(actual_write != bitmap_size))
                throw Exception("Actual written bytes and bitmap size not equal");
            wb.write(buf, actual_write);
        }
    }


    static inline Bitmap readBitmap(DB::ReadBuffer & rb)
    {
        static Allocator<false> allocator;

        UInt64 version, bitmap_size;
        readVarUInt(version, rb);
        if (unlikely(version != 0))
            throw Exception("Version expected 0, got " + DB::toString(version));

        readVarUInt(bitmap_size, rb);

        auto & buf = rb.buffer();
        size_t remain_buf_size = buf.end() - rb.position();
        if (remain_buf_size >= bitmap_size)
        {
            return Bitmap::readSafe(rb.position(), bitmap_size);
        }
        else
        {
            char * buf = (char *)allocator.alloc(bitmap_size);
            SCOPE_EXIT({ allocator.free(buf, bitmap_size); });
            rb.readStrict(buf, bitmap_size);
            return Bitmap::readSafe(buf, bitmap_size);
        }
    }


    void read(DB::ReadBuffer & rb) { bitmap = readBitmap(rb); }

    void readAndMerge(DB::ReadBuffer & rb)
    {
        if (bitmap.isEmpty())
            bitmap = readBitmap(rb);
        else
            bitmap |= readBitmap(rb);
    }

    static void skip(DB::ReadBuffer & rb) { readBitmap(rb); }

    void writeText(DB::WriteBuffer & wb) const
    {
        wb.write('{');
        writeIntText(bitmap.cardinality(), wb);
        wb.write(':');
        for (auto it = bitmap.begin(), end = bitmap.end(); it != end; ++it)
        {
            writeIntText((*it), wb);
            if (it != end)
                wb.write(',');
        }
        wb.write('}');
    }

    void readText(DB::ReadBuffer & rb)
    {
        size_t size;
        assertChar('{', rb);
        readIntText(size, rb);
        assertChar(':', rb);

        Value v;
        for (size_t i = 0; i < size; ++i)
        {
            readIntText(v, rb);
            bitmap.add(v);
            if (i != size - 1)
                assertChar(',', rb);
        }
        assertChar('}', rb);
    }
};

} // namespace DB