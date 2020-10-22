#pragma once

#include <tuple>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>


namespace DB
{

/** Mark is the position in the compressed file. The compressed file consists of adjacent compressed blocks.
  * Mark is a tuple - the offset in the file to the start of the compressed block, the offset in the decompressed block to the start of the data.
  */
struct MarkInCompressedFile
{
    size_t offset_in_compressed_file;
    size_t offset_in_decompressed_block;

    bool operator==(const MarkInCompressedFile & rhs) const
    {
        return std::tie(offset_in_compressed_file, offset_in_decompressed_block)
            == std::tie(rhs.offset_in_compressed_file, rhs.offset_in_decompressed_block);
    }
    bool operator!=(const MarkInCompressedFile & rhs) const
    {
        return !(*this == rhs);
    }

    String toString() const
    {
        return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
    }
};

using MarksInCompressedFile = PODArray<MarkInCompressedFile>;
using MarksInCompressedFilePtr = std::shared_ptr<MarksInCompressedFile>;

struct MarkWithSizeInCompressedFile
{
    MarkInCompressedFile mark;
    size_t mark_size;

    bool operator==(const MarkWithSizeInCompressedFile & rhs) const
    {
        return std::tie(mark, mark_size) == std::tie(rhs.mark, rhs.mark_size);
    }
    bool operator!=(const MarkWithSizeInCompressedFile & rhs) const
    {
        return !(*this == rhs);
    }

    String toString() const
    {
        return "(" + mark.toString() + "," + DB::toString(mark_size) + ")";
    }
};

using MarkWithSizesInCompressedFile = PODArray<MarkWithSizeInCompressedFile>;
using MarkWithSizesInCompressedFilePtr = std::shared_ptr<MarkWithSizesInCompressedFile>;

}
