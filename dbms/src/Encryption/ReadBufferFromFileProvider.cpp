#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <Encryption/ReadBufferFromFileProvider.h>


namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
}

namespace CurrentMetrics
{
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_SELECT;
} // namespace ErrorCodes

ReadBufferFromFileProvider::ReadBufferFromFileProvider(const FileProviderPtr & file_provider_, const std::string & file_name_,
    const EncryptionPath & encryption_path_, size_t buf_size, int flags, char * existing_memory, size_t alignment)
    : ReadBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment),
      file(file_provider_->newRandomAccessFile(file_name_, encryption_path_, flags))
{
    fd = file->getFd();
}

void ReadBufferFromFileProvider::close() { file->close(); }

bool ReadBufferFromFileProvider::nextImpl()
{
    size_t bytes_read = 0;
    while (!bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
            res = file->read(internal_buffer.begin(), internal_buffer.size());
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throwFromErrno("Cannot read from file " + getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }

    pos_in_file += bytes_read;

    if (bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}

off_t ReadBufferFromFileProvider::doSeekInFile(off_t offset, int whence) { return file->seek(offset, whence); }

ReadBufferFromFileProvider::~ReadBufferFromFileProvider()
{
    if (file->isClosed())
        return;

    file->close();
}

} // namespace DB
