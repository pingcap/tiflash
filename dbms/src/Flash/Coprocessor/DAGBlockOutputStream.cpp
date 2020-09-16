#include <Flash/Coprocessor/DAGBlockOutputStream.h>

namespace DB
{

DAGBlockOutputStream::DAGBlockOutputStream(Block && header_, DAGResponseWriter & response_writer_)
    : header(std::move(header_)), response_writer(response_writer_)
{}

void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::write(const Block & block) { response_writer.write(block); }

void DAGBlockOutputStream::writeSuffix()
{
    // todo error handle
    response_writer.finishWrite();
}

} // namespace DB
