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

WriteBufferFromFileBase * createWriteBufferFromFileBaseByFileProvider(const FileProviderPtr & file_provider, const std::string & filename_,
    const EncryptionPath & encryption_path_, bool create_new_encryption_info_, const RateLimiterPtr & rate_limiter_, size_t estimated_size,
    size_t aio_threshold, size_t buffer_size_, int flags_, mode_t mode, char * existing_memory_, size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
        return new WriteBufferFromFileProvider(file_provider, filename_, encryption_path_, create_new_encryption_info_, rate_limiter_,
            buffer_size_, flags_, mode, existing_memory_, alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

} // namespace DB