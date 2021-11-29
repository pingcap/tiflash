#include <Storages/Page/V3/BlobFile.h>


namespace DB::PS::V3
{
BlobFile::BlobFile(String path_, FileProviderPtr file_provider_)
    : file_provider{file_provider_}
    , path(path_)
    , log(&Poco::Logger::get("PageStorage V3"))
{
    smap = (struct spacemap *)calloc(1, sizeof(struct spacemap));
    if (smap == nullptr){

    }
}

BlobFile::~BlobFile()
{
    free(smap);
}





} // namespace DB::PS::V3