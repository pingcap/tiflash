#include "DMFileWriterRemote.h"

namespace DB
{
namespace DM
{
DMFileWriterRemote::DMFileWriterRemote( //
    const DMFilePtr & dmfile_,
    const FileProviderPtr & file_provider_,
    const Remote::DMFileOID & remote_oid_,
    const Remote::IDataStorePtr & data_store_)
    : dmfile(dmfile_)
    , file_provider(file_provider_)
    , remote_oid(remote_oid_)
    , data_store(data_store_)
{}

void DMFileWriterRemote::DMFileWriterRemote::write()
{
    auto path = dmfile->path();
    dmfile->setStatus(DMFile::Status::WRITING);
    data_store->copyDMFileMetaToLocalPath(remote_oid, path);
}

void DMFileWriterRemote::finalize()
{
    dmfile->finalizeForRemote(file_provider);
}
} // namespace DM
} // namespace DB