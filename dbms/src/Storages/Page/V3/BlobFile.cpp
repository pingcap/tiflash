#include <Encryption/WriteReadableFile.h>
#include <Storages/Page/V3/BlobFile.h>

namespace DB::PS::V3
{
BlobFile::BlobFile(String path_, FileProviderPtr file_provider_, bool truncate_if_exists)
    : file_provider{file_provider_}
    , path(path_)
    , log(&Poco::Logger::get("BlobFile"))
{
    wrPtr = file_provider->newWriteReadableFile(
        getPath(),
        getEncryptionPath(),
        truncate_if_exists,
        /*create_new_encryption_info_*/ truncate_if_exists);
}

BlobFile::~BlobFile()
{
    wrPtr->close();
}


} // namespace DB::PS::V3