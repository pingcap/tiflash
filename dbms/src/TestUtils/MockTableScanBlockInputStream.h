#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
class MockTableScanBlockInputStream : public IProfilingBlockInputStream
{
public:
    MockTableScanBlockInputStream(ColumnsWithTypeAndName columns, size_t max_block_size);
    Block getHeader() const override
    {
        ;
        return Block(columns);
    }
    String getName() const override { return "MockTableScan"; }
    ColumnsWithTypeAndName columns;
    size_t output_index;
    size_t max_block_size;

protected:
    Block readImpl() override;
};

} // namespace DB
