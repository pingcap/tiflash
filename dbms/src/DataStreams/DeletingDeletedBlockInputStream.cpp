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

#include <DataStreams/dedupUtils.h>
#include <DataStreams/DeletingDeletedBlockInputStream.h>


namespace DB
{

Block DeletingDeletedBlockInputStream::readImpl()
{
    Block block = input->read();
    if (!block)
        return block;
    IColumn::Filter filter(block.rows());
    size_t count = setFilterByDelMarkColumn(block, filter);
    if (count > 0)
        deleteRows(block, filter);
    return block;
}

}
