// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace DM
{
RowKeyRange ColumnFileSetSnapshot::getSquashDeleteRange(bool is_common_handle, size_t rowkey_column_size) const
{
    RowKeyRange squashed_delete_range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
    for (const auto & column_file : column_files)
    {
        if (auto * f_delete = column_file->tryToDeleteRange(); f_delete)
            squashed_delete_range = squashed_delete_range.merge(f_delete->getDeleteRange());
    }
    return squashed_delete_range;
}
} // namespace DM
} // namespace DB
