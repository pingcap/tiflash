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

#include <DataStreams/AddExtraTableIDColumnInputStream.h>

namespace DB
{

AddExtraTableIDColumnInputStream::AddExtraTableIDColumnInputStream(
    BlockInputStreamPtr input,
    int extra_table_id_index,
    TableID physical_table_id_)
    : physical_table_id(physical_table_id_)
    , action(input->getHeader(), extra_table_id_index)
{
    children.push_back(input);
}

Block AddExtraTableIDColumnInputStream::read()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    auto ok = action.transform(res, physical_table_id);
    if (!ok)
        return {};

    return res;
}

} // namespace DB
