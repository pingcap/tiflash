#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
namespace DM
{

class SkippableBlockInputStream : public IBlockInputStream
{
public:
    virtual ~SkippableBlockInputStream() = default;

    /// Return the rows count before read next block.
    virtual size_t getSkippedRows() = 0;
};

class EmptySkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    EmptySkippableBlockInputStream(const ColumnDefines & read_columns_) : read_columns(read_columns_) {}

    String getName() const override { return "EmptySkippable"; }

    Block getHeader() const override { return toEmptyBlock(read_columns); }

    size_t getSkippedRows() override { return 0; }

    Block read() override { return {}; }

private:
    ColumnDefines read_columns;
};

using SkippableBlockInputStreamPtr = std::shared_ptr<SkippableBlockInputStream>;

} // namespace DM
} // namespace DB