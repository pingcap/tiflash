#include <Encryption/FileProvider.h>
#include <Storages/Page/V3/BlobFile.h>

namespace DB::PS::V3
{
BlobFile::BlobFile(String path_, FileProviderPtr file_provider_)
    : file_provider{file_provider_}
    , path(path_)
    , log(&Poco::Logger::get("NewPageStorage"))
{
}

} // namespace DB::PS::V3