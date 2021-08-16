#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/ReadBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event CreatedReadBufferOrdinary;
}

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(
    FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    size_t aio_threshold,
    const ReadLimiterPtr & read_limiter,
    size_t buffer_size_,
    int flags_,
    char * existing_memory_,
    size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
        return std::make_unique<ReadBufferFromFileProvider>(
            file_provider,
            filename_,
            encryption_path_,
            buffer_size_,
            read_limiter,
            flags_,
            existing_memory_,
            alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, size_t estimated_size, const ReadLimiterPtr & read_limiter,
    const DM::DMConfiguration & configuration, int flags_)
{

    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    auto file = file_provider->newRandomAccessFile(filename_, encryption_path_, read_limiter, flags_);
    auto allocation_size = std::min(estimated_size, configuration.getChecksumFrameLength());
    switch (configuration.getChecksumAlgorithm())
    {
        case ChecksumAlgo::None:
            return std::make_unique<FramedChecksumReadBuffer<Digest::None>>(file, allocation_size);
        case ChecksumAlgo::CRC32:
            return std::make_unique<FramedChecksumReadBuffer<Digest::CRC32>>(file, allocation_size);
        case ChecksumAlgo::CRC64:
            return std::make_unique<FramedChecksumReadBuffer<Digest::CRC64>>(file, allocation_size);
        case ChecksumAlgo::City128:
            return std::make_unique<FramedChecksumReadBuffer<Digest::City128>>(file, allocation_size);
        case ChecksumAlgo::XXH3:
            return std::make_unique<FramedChecksumReadBuffer<Digest::XXH3>>(file, allocation_size);
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}

} // namespace DB
