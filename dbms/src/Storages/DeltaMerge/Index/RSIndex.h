#pragma once

#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

namespace DB
{

namespace DM
{

class EqualIndex;
using EqualIndexPtr = std::shared_ptr<EqualIndex>;


class EqualIndex
{
public:
    virtual ~EqualIndex() = default;
};

struct RSIndex
{
    DataTypePtr    type;
    MinMaxIndexPtr minmax;
    EqualIndexPtr  equal;

    RSIndex(const DataTypePtr & type_, const MinMaxIndexPtr & minmax_) : type(type_), minmax(minmax_) {}

    RSIndex(const DataTypePtr & type_, const MinMaxIndexPtr & minmax_, const EqualIndexPtr & equal_)
        : type(type_), minmax(minmax_), equal(equal_)
    {
    }
};

using ColumnIndexes = std::unordered_map<ColId, RSIndex>;

} // namespace DM

} // namespace DB