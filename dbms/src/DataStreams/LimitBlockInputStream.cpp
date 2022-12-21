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

#include <DataStreams/LimitBlockInputStream.h>

#include <algorithm>


namespace DB
{
LimitBlockInputStream::LimitBlockInputStream(
    const BlockInputStreamPtr & input,
    size_t limit_,
    const String & req_id)
    : log(Logger::get(req_id))
    , action(input->getHeader(), limit_)
{
    children.push_back(input);
}


Block LimitBlockInputStream::readImpl()
{
    Block res = children.back()->read();

    if (action.transform(res))
    {
        return res;
    }
    else
    {
        return {};
    }
}

void LimitBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", limit = {}", action.getLimit());
}
} // namespace DB
