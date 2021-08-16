#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/WriteBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
extern const Event CreatedWriteBufferOrdinary;
}

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_, size_t estimated_size, size_t aio_threshold, size_t buffer_size_, int flags_, mode_t mode,
    char * existing_memory_, size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
        return std::make_unique<WriteBufferFromFileProvider>(file_provider, filename_, encryption_path_, create_new_encryption_info_,
            write_limiter_, buffer_size_, flags_, mode, existing_memory_, alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_, const DM::DMConfiguration & configuration, int flags_, mode_t mode)
{
    ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
    auto filePtr
        = file_provider->newWritableFile(filename_, encryption_path_, true, create_new_encryption_info_, write_limiter_, flags_, mode);
    switch (configuration.getChecksumAlgorithm())
    {
        case DM::ChecksumAlgo::None:
            return std::make_unique<DM::Checksum::FramedChecksumWriteBuffer<DM::Digest::None>>(
                filePtr, configuration.getChecksumFrameLength());
        case DM::ChecksumAlgo::CRC32:
            return std::make_unique<DM::Checksum::FramedChecksumWriteBuffer<DM::Digest::CRC32>>(
                filePtr, configuration.getChecksumFrameLength());
        case DM::ChecksumAlgo::CRC64:
            return std::make_unique<DM::Checksum::FramedChecksumWriteBuffer<DM::Digest::CRC64>>(
                filePtr, configuration.getChecksumFrameLength());
        case DM::ChecksumAlgo::City128:
            return std::make_unique<DM::Checksum::FramedChecksumWriteBuffer<DM::Digest::City128>>(
                filePtr, configuration.getChecksumFrameLength());
        case DM::ChecksumAlgo::XXH3:
            return std::make_unique<DM::Checksum::FramedChecksumWriteBuffer<DM::Digest::XXH3>>(
                filePtr, configuration.getChecksumFrameLength());
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}

} // namespace DB