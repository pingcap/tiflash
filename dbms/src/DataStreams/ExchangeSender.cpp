#include <DataStreams/ExchangeSender.h>
namespace DB
{
Block ExchangeSender::readImpl()
{
    Block block = children.back()->read();
    auto timer = getSelfTimer();

    if (block)
        writer->write(block);
    return block;
}
} // namespace DB
