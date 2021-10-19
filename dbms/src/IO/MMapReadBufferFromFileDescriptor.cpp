#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <fmt/format.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
} // namespace ErrorCodes


void MMapReadBufferFromFileDescriptor::init()
{
    size_t length = mapped.getLength();
    BufferBase::set(mapped.getData(), length, 0);

#if 0
    // These lines are ported from Clickhouse, but not used by TiFlash now
    size_t page_size = static_cast<size_t>(::getPageSize());
    ReadBuffer::padded = (length % page_size) > 0 && (length % page_size) <= (page_size - 15);
#endif
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset, size_t length)
    : mapped(fd, offset, length)
{
    init();
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset)
    : mapped(fd, offset)
{
    init();
}


void MMapReadBufferFromFileDescriptor::finish()
{
    mapped.finish();
}


std::string MMapReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(mapped.getFD()) + ")";
}

int MMapReadBufferFromFileDescriptor::getFD() const
{
    return mapped.getFD();
}

off_t MMapReadBufferFromFileDescriptor::getPositionInFile()
{
    return count();
}

off_t MMapReadBufferFromFileDescriptor::doSeek(off_t offset, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
        new_pos = offset;
    else if (whence == SEEK_CUR)
        new_pos = count() + offset;
    else
        throw Exception("MMapReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    working_buffer = internal_buffer;
    if (new_pos < 0 || new_pos > off_t(working_buffer.size()))
        throw Exception(
            fmt::format(
                "Cannot seek through file {} because seek position ({}) is out of bounds [0, {}]",
                getFileName(),
                new_pos,
                working_buffer.size()),
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    position() = working_buffer.begin() + new_pos;
    return new_pos;
}

} // namespace DB
