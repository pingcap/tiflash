// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA

#include <Columns/ColumnString.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Writer.h>

namespace DB::DM
{

namespace details
{

template <class W>
void addBlockToFTSIndexWriter(
    ::rust::Box<W> & index_writer,
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark)
{
    // Note: column may be nullable.
    const ColumnString * col_str;
    if (column.isColumnNullable())
        col_str = checkAndGetNestedColumn<ColumnString>(&column);
    else
        col_str = checkAndGetColumn<ColumnString>(&column);

    RUNTIME_CHECK(col_str != nullptr, column.getFamilyName());

    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    for (size_t i = 0, size = col_str->size(); i < size; ++i)
    {
        // add_null keeps the doc_id correct when there are missing docs.
        if (del_mark_data != nullptr && (*del_mark_data)[i])
        {
            index_writer->add_null();
            continue;
        }
        if (column.isNullAt(i))
        {
            index_writer->add_null();
            continue;
        }

        auto data = col_str->getDataAt(i);
        index_writer->add_document(rust::Str(data.data, data.size));
    }
}

} // namespace details

void FullTextIndexWriterInMemory::addBlock(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    ProceedCheckFn should_proceed)
{
    UNUSED(should_proceed);
    details::addBlockToFTSIndexWriter(inner, column, del_mark);
}

void FullTextIndexWriterOnDisk::addBlock(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    ProceedCheckFn should_proceed)
{
    UNUSED(should_proceed);
    details::addBlockToFTSIndexWriter(inner, column, del_mark);
}

} // namespace DB::DM
#endif
