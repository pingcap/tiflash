#pragma once

#include <Interpreters/Context.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>

namespace DB::PS::V3
{

struct CheckpointInfo
{
    String checkpoint_manifest_path;
    String checkpoint_data_dir;
    UInt64 checkpoint_store_id;
};

class CheckpointPageManager
{
public:
    static UniversalPageStoragePtr createTempPageStorage(Context & context, const String & checkpoint_manifest_path, const String & data_dir);
};

} // namespace DB::PS::V3
