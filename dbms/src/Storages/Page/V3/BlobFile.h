#pragma once

#include <Poco/Logger.h>
#include <Core/Types.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>
#include <Encryption/FileProvider.h>
#include <Storages/Page/V3/spacemap/space_map.h>

namespace DB::PS::V3
{

class BlobFile;

class BlobFileWriter
{
    BlobFileWriter(BlobFile & blob_file);

    size_t req_space(size_t offset);

    size_t req_spaces(std::vector<size_t> offsets);

    void write(char * buffer, size_t offset, size_t size);
};

class BlobFile
{
public:
    BlobFile(String path_, FileProviderPtr file_provider_);

    ~BlobFile();

    String getPath();

    
    
    /**
     * RW request
     */
    void read(char * buffer, size_t offset, size_t size);



    void getWriter();

private:
    FileProviderPtr file_provider;
    String path;

    struct spacemap * smap;

    Poco::Logger * log;
};


} // namespace DB::PS::V3