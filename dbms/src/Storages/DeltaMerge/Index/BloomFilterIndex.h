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

#pragma once


#include <Storages/DeltaMerge/Index/BloomFilter.h>

#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"
#include "Storages/DeltaMerge/Index/RSResult.h"

namespace DB
{
namespace DM
{
class BloomFilterIndex;
using BloomFilterIndexPtr = std::shared_ptr<BloomFilterIndex>;
using BloomFilterPtr = std::shared_ptr<BloomFilter>;
// 先不考虑 sql 中任何跟 null 有关的情况
class BloomFilterIndex
{
public:
    explicit BloomFilterIndex(double false_positive_probability_ = 0.01)
        : false_positive_probability(false_positive_probability_)
    {}
    explicit BloomFilterIndex(std::vector<BloomFilterPtr> & bloom_filter_vec_, double false_positive_probability_ = 0.01)
        : bloom_filter_vec(bloom_filter_vec_)
        , false_positive_probability(false_positive_probability_)
    {}

    void addPack(const IColumn & column, const IDataType & type);

    void write(WriteBuffer & buf);

    static BloomFilterIndexPtr read(ReadBuffer & buf, size_t bytes_limit);

    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type) const;
    RSResult checkNullableEqual(size_t pack_index, const Field & value, const DataTypePtr & type) const;

private:
    RSResult check(size_t pack_index, const Field & value, const IDataType * raw_type) const;
    void updateBloomFilter(BloomFilterPtr & bloom_filter, const IColumn & column, size_t size, const IDataType * type);
    std::vector<BloomFilterPtr> bloom_filter_vec; // 一个 dmfile 的一个 column 对应一个 bloom_filter_vec（目前先是以前支持 index 的现在也支持），vec 长度等于 pack numbers，也就是一个 pack 一个
    double false_positive_probability;
};
} // namespace DM
} // namespace DB