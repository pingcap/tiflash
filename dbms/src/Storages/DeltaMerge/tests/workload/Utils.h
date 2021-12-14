#pragma once
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM::tests
{
// These functions are log helpers.
std::string localTime();
std::string fieldToString(const DataTypePtr & data_type, const Field & f);
std::vector<std::string> colToVec(const DataTypePtr & data_type, const ColumnPtr & col);
std::string blockToString(const Block & block);
} // namespace DB::DM::tests