#include <IO/ReadBufferFromFileProvider.h>

namespace DB {

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_SELECT;
}

ReadBufferFromFileProvider::ReadBufferFromFileProvider(
            FileProviderPtr &file_provider_,
            const std::string &file_name_,
            size_t buf_size,
            int flags,
            char *existing_memory,
            size_t alignment)
    :ReadBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment),
    file(file_provider_->NewRandomAccessFile(file_name_, flags))
{
    fd = file->getFd();
}

void ReadBufferFromFileProvider::close() {
    file->close();
}

bool ReadBufferFromFileProvider::nextImpl() {
    size_t bytes_read = file->read(internal_buffer.begin(), internal_buffer.size());

    if (bytes_read)
    {
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}

}
