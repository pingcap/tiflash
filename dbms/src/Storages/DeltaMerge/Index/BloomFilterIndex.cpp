
#include <Storages/DeltaMerge/Index/BloomFilterIndex.h>
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeMyDate.h"
#include "DataTypes/DataTypeMyDate.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeString.h"
#include "IO/WriteHelpers.h"
#include "IO/ReadHelpers.h"
#include "Columns/ColumnNullable.h"
#include "AggregateFunctions/Helpers.h"
#include "common/types.h"
namespace DB
{
namespace DM
{
void BloomFilterIndex::write(WriteBuffer & buf){
    auto size = bloom_filter_vec.size();
    DB::writeIntBinary(size, buf);
    for (auto & bloom_filter : bloom_filter_vec){
        bloom_filter->write(buf);
    }
}

BloomFilterIndexPtr BloomFilterIndex::read(ReadBuffer & buf, size_t bytes_limit){
    UInt64 size = 0;
    if (bytes_limit != 0){
        DB::readIntBinary(size, buf);
    }
    std::vector<BloomFilterPtr> bloom_filter_vec;
    for (size_t index = 0 ; index < size; index++){
        auto bloom_filter = BloomFilter::read(buf);
        bloom_filter_vec.push_back(bloom_filter);
    }
    return std::make_shared<BloomFilterIndex>(bloom_filter_vec);
}

void BloomFilterIndex::updateBloomFilter(BloomFilterPtr & bloom_filter, const IColumn & column, size_t size, const IDataType * type){
    for (size_t i = 0; i < size; ++i){
        Field value; 
        column.get(i, value);

#define DISPATCH(TYPE)                                              \
        if (typeid_cast<const DataType##TYPE *>(type))              \
        {                                                               \
            bloom_filter->insert(value.get<TYPE>()); \
        }
        FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        if (typeid_cast<const DataTypeDate *>(type))
        {
            bloom_filter->insert(value.get<UInt16>()); 
        }
        if (typeid_cast<const DataTypeDateTime *>(type))
        {
            bloom_filter->insert(value.get<UInt32>()); 
        }
        if (typeid_cast<const DataTypeMyDateTime *>(type))
        {
            bloom_filter->insert(value.get<UInt64>()); 
        }
        if (typeid_cast<const DataTypeMyDate *>(type))
        {
            bloom_filter->insert(value.get<UInt64>()); 
        }
        if (typeid_cast<const DataTypeString *>(type))
        {
            bloom_filter->insert(value.get<String>()); 
        }
    }
}


void BloomFilterIndex::addPack(const IColumn & column, const IDataType & type){
    auto size = column.size();
    
    bloom_parameters parameters;
    parameters.projected_element_count = size;
    parameters.false_positive_probability = false_positive_probability;
    parameters.compute_optimal_parameters();

    auto bloom_filter = std::make_shared<BloomFilter>(parameters);
    if (column.isColumnNullable()){
        auto nest_column = static_cast<const ColumnNullable &>(column).getNestedColumnPtr();
        for (size_t i = 0; i < size; i++){
        }
        updateBloomFilter(bloom_filter, *nest_column, size, static_cast<const DataTypeNullable &>(type).getNestedType().get());
    } else {
        for (size_t i = 0; i < size; i++){
        }
        updateBloomFilter(bloom_filter, column, size, &type);
    }
    bloom_filter_vec.push_back(bloom_filter);
}

RSResult BloomFilterIndex::check(size_t pack_index, const Field & value, const IDataType * raw_type) const {
#define DISPATCH(TYPE)                                              \
    if (typeid_cast<const DataType##TYPE *>(raw_type))              \
    {                                                               \
        if (bloom_filter_vec[pack_index]->contains(value.get<TYPE>())){ \
            return RSResult::Some;\
        } else {\
            return RSResult::None; \
        } \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        if (bloom_filter_vec[pack_index]->contains(value.get<UInt16>()))
            return RSResult::Some;
        else 
            return RSResult::None; 
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        if (bloom_filter_vec[pack_index]->contains(value.get<UInt32>()))
            return RSResult::Some;
        else 
            return RSResult::None; 
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type))
    {
        if (bloom_filter_vec[pack_index]->contains(value.get<UInt64>()))
            return RSResult::Some;
        else 
            return RSResult::None; 
    }
    if (typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        if (bloom_filter_vec[pack_index]->contains(value.get<UInt64>()))
            return RSResult::Some;
        else 
            return RSResult::None; 
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        if (bloom_filter_vec[pack_index]->contains(value.get<String>()))
            return RSResult::Some;
        else 
            return RSResult::None; 
    }
    return RSResult::Some;
}

RSResult BloomFilterIndex::checkNullableEqual(size_t pack_index, const Field & value, const DataTypePtr & type) const{
    const auto * raw_type = type.get();
    return check(pack_index, value, raw_type);
}

RSResult BloomFilterIndex::checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type) const{
    const auto * raw_type = type.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
    {
        return checkNullableEqual(pack_index, value, removeNullable(type));
    }

    return check(pack_index, value, raw_type);
}

}
}