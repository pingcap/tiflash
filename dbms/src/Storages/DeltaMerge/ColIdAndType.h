#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{

struct ColIdAndType
{
    UInt64      id;
    DataTypePtr type;
};

using ColIdAndTypeSet = std::set<ColIdAndType>;

// clang-format off
bool operator<(const ColIdAndType & it1, const ColIdAndType & it2){ return it1.id < it2.id; }
bool operator<(const ColIdAndType & it, UInt64 id)                { return it.id < id; }
bool operator<(UInt64 id, const ColIdAndType & it)                { return id < it.id; }
// clang-format on

void readText(ColIdAndTypeSet & colid_and_types, ReadBuffer & buf);
void writeText(const ColIdAndTypeSet & colid_and_types, WriteBuffer & buf);

} // namespace DM
} // namespace DB