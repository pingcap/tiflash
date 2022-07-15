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

#include <Columns/IColumn.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/Transaction/Collator.h>

namespace DB
{
String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & sub_column) {
        res << ", " << sub_column->dumpStructure();
    };

    const_cast<IColumn *>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

StringRef IColumn::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return serializeValueIntoArena(n, arena, begin, nullptr, TiDB::dummy_sort_key_contaner);
}
void IColumn::updateHashWithValue(size_t n, SipHash & hash) const
{
    updateHashWithValue(n, hash, nullptr, TiDB::dummy_sort_key_contaner);
}

void IColumn::updateHashWithValues(HashValues & hash_values) const
{
    updateHashWithValues(hash_values, nullptr, TiDB::dummy_sort_key_contaner);
}

void IColumn::updateWeakHash32(WeakHash32 & hash) const
{
    updateWeakHash32(hash, nullptr, TiDB::dummy_sort_key_contaner);
}

const char * IColumn::deserializeAndInsertFromArena(const char * pos)
{
    return deserializeAndInsertFromArena(pos, nullptr);
}

} // namespace DB
