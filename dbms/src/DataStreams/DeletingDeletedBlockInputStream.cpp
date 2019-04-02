#include <DataStreams/dedupUtils.h>
#include <DataStreams/DeletingDeletedBlockInputStream.h>


namespace DB
{

Block DeletingDeletedBlockInputStream::readImpl()
{
    Block block = input->read();
    if (!block)
        return block;
    IColumn::Filter filter(block.rows());
    size_t count = setFilterByDelMarkColumn(block, filter);
    if (count > 0)
        deleteRows(block, filter);
    return block;
}

}
