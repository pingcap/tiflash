// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once


#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>


namespace DB::DM
{

DMFilePtr restoreDMFileFromRemoteDataSource(
    const DMContext & dm_context,
    Remote::IDataStorePtr remote_data_store,
    UInt64 file_page_id,
    UInt64 meta_version);

DMFilePtr restoreDMFileFromLocal(const DMContext & dm_context, UInt64 file_page_id, UInt64 meta_version);

DMFilePtr restoreDMFileFromCheckpoint(
    const DMContext & dm_context,
    Remote::IDataStorePtr remote_data_store,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs,
    UInt64 file_page_id,
    UInt64 meta_version);

} // namespace DB::DM
