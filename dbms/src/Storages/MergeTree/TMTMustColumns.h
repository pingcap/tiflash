#pragma once

namespace DB
{
// pk, version, delmark is always the first 3 columns.
// the index of column is constant after MergeTreeBlockInputStream is constructed. exception will be thrown if not found.
const size_t pk_column_index = 0, version_column_index = 1, delmark_column_index = 2;

}
