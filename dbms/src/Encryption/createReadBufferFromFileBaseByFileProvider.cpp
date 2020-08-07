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

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(FileProviderPtr & file_provider,
    const std::string & filename_, const EncryptionPath & encryption_path_, size_t estimated_size, size_t aio_threshold,
    size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
        return std::make_unique<ReadBufferFromFileProvider>(
            file_provider, filename_, encryption_path_, buffer_size_, flags_, existing_memory_, alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

} // namespace DB
