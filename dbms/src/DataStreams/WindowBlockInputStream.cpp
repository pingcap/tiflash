#include <DataStreams/WindowBlockInputStream.h>

namespace DB
{

Block WindowBlockInputStream::getHeader() const
{

}

Block WindowBlockInputStream::readImpl()
{
    const auto & stream = children.back();
    while (Block block = stream->read())
    {

    }

}

}