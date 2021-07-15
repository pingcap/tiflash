#include <DataStreams/ExchangeSender.h>
namespace DB
{

Block ExchangeSender::readImpl()
{
    Block block = children.back()->read();
    if (!block)
        return block;
    writer->write(block);
    return block;
}
} // namespace DB
