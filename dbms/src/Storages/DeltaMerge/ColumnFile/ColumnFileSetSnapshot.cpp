// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace DM
{
RowKeyRange ColumnFileSetSnapshot::getSquashDeleteRange() const
{
    RowKeyRange squashed_delete_range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
    for (const auto & column_file : column_files)
    {
        if (auto * f_delete = column_file->tryToDeleteRange(); f_delete)
            squashed_delete_range = squashed_delete_range.merge(f_delete->getDeleteRange());
    }
    return squashed_delete_range;
}

ColumnFileSetSnapshotPtr ColumnFileSetSnapshot::deserializeFromRemoteProtocol(
    const google::protobuf::RepeatedPtrField<dtpb::ColumnFileRemote> & proto,
    UInt64 remote_write_node_id,
    const DMContext & context, // RemoteManager, table_id, MinMaxIndex, ReadLimiter is used.
    const RowKeyRange & segment_range)
{
    auto remote_manager = context.db_context.getDMRemoteManager();
    IColumnFileSetStorageReaderPtr base_storage = std::make_shared<RemoteColumnFileSetStorage>(
        remote_manager,
        remote_write_node_id,
        context.table_id);

    auto log = Logger::get();

    auto ret = std::make_shared<ColumnFileSetSnapshot>(base_storage);
    ret->is_common_handle = segment_range.is_common_handle;
    ret->rowkey_column_size = segment_range.rowkey_column_size;
    ret->column_files.reserve(proto.size());
    for (const auto & remote_column_file : proto)
    {
        if (remote_column_file.has_tiny())
        {
            auto tiny_file = remote_column_file.tiny();
            auto page_oid = Remote::PageOID{
                .write_node_id = remote_write_node_id,
                .table_id = context.table_id,
                .page_id = tiny_file.page_id(),
            };
            ret->column_files.push_back(ColumnFileTiny::deserializeFromRemoteProtocol(
                tiny_file,
                page_oid,
                context));
        }
        else if (remote_column_file.has_in_memory())
        {
            ret->column_files.push_back(ColumnFileInMemory::deserializeFromRemoteProtocol(remote_column_file.in_memory()));
        }
        else if (remote_column_file.has_delete_range())
        {
            ret->column_files.push_back(ColumnFileDeleteRange::deserializeFromRemoteProtocol(remote_column_file.delete_range()));
        }
        else if (remote_column_file.has_big())
        {
            auto big_file = remote_column_file.big();
            auto file_oid = Remote::DMFileOID{
                .write_node_id = remote_write_node_id,
                .table_id = context.table_id,
                .file_id = big_file.file_id(),
            };
            ret->column_files.push_back(ColumnFileBig::deserializeFromRemoteProtocol(
                big_file,
                file_oid,
                context,
                segment_range));
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "Unexpected proto ColumnFile");
        }
    }
    for (const auto & column_file : ret->column_files)
    {
        ret->rows += column_file->getRows();
        ret->bytes += column_file->getBytes();
        ret->deletes += column_file->getDeletes();
    }
    return ret;
}

} // namespace DM
} // namespace DB
