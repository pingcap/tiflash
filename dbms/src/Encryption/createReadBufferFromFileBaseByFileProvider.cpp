#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/ReadBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>


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

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, size_t estimated_size, size_t aio_threshold,
    const ReadLimiterPtr & read_limiter, size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
        return std::make_unique<ReadBufferFromFileProvider>(
            file_provider, filename_, encryption_path_, buffer_size_, read_limiter, flags_, existing_memory_, alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, const ReadLimiterPtr & read_limiter, 
    const DM::DMConfiguration & configuration, int flags_)
{

    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    auto filePtr = file_provider->newRandomAccessFile(filename_, encryption_path_, read_limiter, flags_);
    switch (configuration.getChecksumAlgorithm())
    {
        case DM::ChecksumAlgo::None:
            return std::make_unique<DM::Checksum::FramedChecksumReadBuffer<DM::Digest::None>>(filePtr);
        case DM::ChecksumAlgo::CRC32:
            return std::make_unique<DM::Checksum::FramedChecksumReadBuffer<DM::Digest::CRC32>>(filePtr);
        case DM::ChecksumAlgo::CRC64:
            return std::make_unique<DM::Checksum::FramedChecksumReadBuffer<DM::Digest::CRC64>>(filePtr);
        case DM::ChecksumAlgo::City128:
            return std::make_unique<DM::Checksum::FramedChecksumReadBuffer<DM::Digest::City128>>(filePtr);
        case DM::ChecksumAlgo::XXH3:
            return std::make_unique<DM::Checksum::FramedChecksumReadBuffer<DM::Digest::XXH3>>(filePtr);
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}

} // namespace DB
