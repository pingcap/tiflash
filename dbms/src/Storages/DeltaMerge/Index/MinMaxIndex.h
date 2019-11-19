#pragma once

#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/MinMax.h>

namespace DB
{

namespace DM
{

class MinMaxIndex;
using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;
using MinMaxIndexes  = std::vector<MinMaxIndexPtr>;

class MinMaxIndex
{
private:
    bool           has_null = false;
    MinMaxValuePtr minmax;

public:
    MinMaxIndex() = default;
    MinMaxIndex(bool has_null_, const MinMaxValuePtr & minmax_) : has_null(has_null_), minmax(minmax_) {}
    MinMaxIndex(const MinMaxIndex & other) : has_null(other.has_null), minmax(other.minmax->clone()) {}
    MinMaxIndex(const IDataType & type, const IColumn & column, const ColumnVector<UInt8> & del_mark, size_t offset, size_t limit);

    void merge(const MinMaxIndex & other);

    void                  write(const IDataType & type, WriteBuffer & buf);
    static MinMaxIndexPtr read(const IDataType & type, ReadBuffer & buf);

    // TODO: Use has_null and value.isNull to check.

    RSResult checkEqual(const Field & value, const DataTypePtr & type);
    RSResult checkGreater(const Field & value, const DataTypePtr & type, int nan_direction);
    RSResult checkGreaterEqual(const Field & value, const DataTypePtr & type, int nan_direction);

    String toString() const;

};


} // namespace DM

} // namespace DB