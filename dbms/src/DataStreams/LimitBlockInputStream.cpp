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

#include <DataStreams/LimitBlockInputStream.h>

#include <algorithm>


namespace DB
{
LimitBlockInputStream::LimitBlockInputStream(
    const BlockInputStreamPtr & input,
    size_t limit_,
    size_t offset_,
    const String & req_id)
    : log(Logger::get(req_id))
    , limit(limit_)
    , offset(offset_)
{
    children.push_back(input);
}


Block LimitBlockInputStream::readImpl()
{
    Block res;
    size_t rows = 0;

    if (pos >= offset + limit)
    {
        return res;
    }

    do
    {
        res = children.back()->read();
        if (!res)
            return res;
        rows = res.rows();
        pos += rows;
    } while (pos <= offset);

    /// give away the whole block
    if (pos >= offset + rows && pos <= offset + limit)
        return res;

    /// give away a piece of the block
    UInt64 start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    UInt64 length = std::min(
        static_cast<Int64>(limit),
        std::min(
            static_cast<Int64>(pos) - static_cast<Int64>(offset),
            static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos)
                + static_cast<Int64>(rows)));

    for (size_t i = 0; i < res.columns(); ++i)
        res.safeGetByPosition(i).column = res.safeGetByPosition(i).column->cut(start, length);

    return res;
}

void LimitBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", limit = {}", limit);
}
} // namespace DB
