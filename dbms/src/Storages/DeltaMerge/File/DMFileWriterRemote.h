#pragma once

#include "DMFile.h"
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>

namespace DB
{
namespace DM
{
class DMFileWriterRemote
{
public:
    DMFileWriterRemote(const DMFilePtr & dmfile_,
                       const FileProviderPtr & file_provider_,
                       const Remote::DMFileOID & remote_oid_,
                       const Remote::IDataStorePtr & data_store_);

    void write();

    void finalize();

private:
    DMFilePtr dmfile;
    FileProviderPtr file_provider;
    Remote::DMFileOID remote_oid;
    Remote::IDataStorePtr data_store;
};
}
}
