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

#pragma once

#include <Columns/ColumnVectorHelper.h>
#include <IO/Endian.h>

#include <cmath>


namespace DB
{
/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <typename T>
struct CompareHelper
{
    static bool less(T a, T b, int /*nan_direction_hint*/) { return a < b; }
    static bool greater(T a, T b, int /*nan_direction_hint*/) { return a > b; }

    /** Compares two numbers. Returns a number less than zero, equal to zero, or greater than zero if a < b, a == b, a > b, respectively.
      * If one of the values is NaN, then
      * - if nan_direction_hint == -1 - NaN are considered less than all numbers;
      * - if nan_direction_hint == 1 - NaN are considered to be larger than all numbers;
      * Essentially: nan_direction_hint == -1 says that the comparison is for sorting in descending order.
      */
    static int compare(T a, T b, int /*nan_direction_hint*/) { return a > b ? 1 : (a < b ? -1 : 0); }
};

template <>
struct CompareHelper<Null>
{
    static bool less(Null, Null, int) { return false; }
    static bool greater(Null, Null, int) { return false; }
    static int compare(Null, Null, int) { return 0; }
};

template <typename T>
struct FloatCompareHelper
{
    static bool less(T a, T b, int nan_direction_hint)
    {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint < 0;
        if (isnan_b)
            return nan_direction_hint > 0;

        return a < b;
    }

    static bool greater(T a, T b, int nan_direction_hint)
    {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint > 0;
        if (isnan_b)
            return nan_direction_hint < 0;

        return a > b;
    }

    static int compare(T a, T b, int nan_direction_hint)
    {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);
        if (unlikely(isnan_a || isnan_b))
        {
            if (isnan_a && isnan_b)
                return 0;

            return isnan_a ? nan_direction_hint : -nan_direction_hint;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <>
struct CompareHelper<Float32> : public FloatCompareHelper<Float32>
{
};
template <>
struct CompareHelper<Float64> : public FloatCompareHelper<Float64>
{
};


/** To implement `get64` function.
  */
template <typename T>
inline UInt64 unionCastToUInt64(T x)
{
    return x;
}

template <>
inline UInt64 unionCastToUInt64(Float64 x)
{
    union
    {
        Float64 src;
        UInt64 res{};
    };

    src = x;
    return res;
}

template <>
inline UInt64 unionCastToUInt64(Float32 x)
{
    union
    {
        Float32 src;
        UInt64 res{};
    };

    res = 0;
    src = x;
    return res;
}

template <typename TargetType, typename EncodeType>
inline TargetType decodeInt(const char * pos)
{
    if constexpr (std::is_same_v<TargetType, Null>)
    {
        return Null{};
    }
    else if constexpr (is_signed_v<TargetType>)
    {
        return static_cast<TargetType>(static_cast<std::make_signed_t<EncodeType>>(readLittleEndian<EncodeType>(pos)));
    }
    else
    {
        return static_cast<TargetType>(
            static_cast<std::make_unsigned_t<EncodeType>>(readLittleEndian<EncodeType>(pos)));
    }
}


/** A template for columns that use a simple array to store.
  */
template <typename T>
class ColumnVector final : public COWPtrHelper<ColumnVectorHelper, ColumnVector<T>>
{
    static_assert(!IsDecimal<T>);

private:
    friend class COWPtrHelper<ColumnVectorHelper, ColumnVector<T>>;

    using Self = ColumnVector<T>;

    struct less;
    struct greater;

public:
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

private:
    ColumnVector() = default;
    explicit ColumnVector(const size_t n)
        : data(n)
    {}
    ColumnVector(const size_t n, const value_type x)
        : data(n, x)
    {}
    ColumnVector(const ColumnVector & src)
        : data(src.data.begin(), src.data.end()){};

    /// Sugar constructor.
    ColumnVector(std::initializer_list<T> il)
        : data{il}
    {}

public:
    bool isNumeric() const override { return is_arithmetic_v<T>; }

    size_t size() const override { return data.size(); }

    StringRef getDataAt(size_t n) const override
    {
        return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
    }

    void insertFrom(const IColumn & src, size_t n) override
    {
        data.push_back(static_cast<const Self &>(src).getData()[n]);
    }

    void insertManyFrom(const IColumn & src, size_t position, size_t length) override
    {
        const auto & value = static_cast<const Self &>(src).getData()[position];
        data.resize_fill(data.size() + length, value);
    }

    void insertDisjunctFrom(const IColumn & src, const std::vector<size_t> & position_vec) override
    {
        const auto & src_container = static_cast<const Self &>(src).getData();
        size_t old_size = data.size();
        size_t to_add_size = position_vec.size();
        data.resize(old_size + to_add_size);
        for (size_t i = 0; i < to_add_size; ++i)
            data[i + old_size] = src_container[position_vec[i]];
    }

    void insertMany(const Field & field, size_t length) override
    {
        data.resize_fill(data.size() + length, static_cast<T>(field.get<T>()));
    }

    void insertData(const char * pos, size_t /*length*/) override { data.push_back(*reinterpret_cast<const T *>(pos)); }

    bool decodeTiDBRowV2Datum(
        size_t cursor,
        const String & raw_value,
        size_t length,
        bool force_decode [[maybe_unused]]) override
    {
        if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
        {
            if (unlikely(length != sizeof(Float64)))
            {
                throw Exception("Invalid float value length " + std::to_string(length), ErrorCodes::LOGICAL_ERROR);
            }
            constexpr UInt64 SIGN_MASK = static_cast<UInt64>(1) << 63; // NOLINT(readability-identifier-naming)
            auto num = readBigEndian<UInt64>(raw_value.c_str() + cursor);
            if (num & SIGN_MASK)
                num ^= SIGN_MASK;
            else
                num = ~num;
            Float64 res;
            memcpy(&res, &num, sizeof(UInt64));
            data.push_back(res);
        }
        else
        {
            // check overflow
            if (length > sizeof(T))
            {
                if (!force_decode)
                {
                    return false;
                }
                else
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Detected overflow when decoding integer of length {} with column type {}",
                        length,
                        this->getName());
                }
            }

            switch (length)
            {
            case sizeof(UInt8):
                data.push_back(decodeInt<T, UInt8>(raw_value.c_str() + cursor));
                break;
            case sizeof(UInt16):
                data.push_back(decodeInt<T, UInt16>(raw_value.c_str() + cursor));
                break;
            case sizeof(UInt32):
                data.push_back(decodeInt<T, UInt32>(raw_value.c_str() + cursor));
                break;
            case sizeof(UInt64):
                data.push_back(decodeInt<T, UInt64>(raw_value.c_str() + cursor));
                break;
            default:
                throw Exception("Invalid integer length " + std::to_string(length), ErrorCodes::LOGICAL_ERROR);
            }
        }
        return true;
    }

    void insertDefault() override { data.push_back(T()); }

    void insertManyDefaults(size_t length) override { data.resize_fill(data.size() + length, T()); }

    void popBack(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef serializeValueIntoArena(
        size_t n,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr &,
        String &) const override;

    inline const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &) override
    {
        data.push_back(*reinterpret_cast<const T *>(pos));
        return pos + sizeof(T);
    }

    void updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
        const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &, const BlockSelectivePtr &)
        const override;

    size_t byteSize() const override { return data.size() * sizeof(data[0]); }

    size_t byteSize(size_t /*offset*/, size_t limit) const override { return limit * sizeof(data[0]); }

    size_t allocatedBytes() const override { return data.allocated_bytes(); }

    void insert(const T value) { data.push_back(value); }

    /// This method implemented in header because it could be possibly devirtualized.
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
    {
        return CompareHelper<T>::compare(data[n], static_cast<const Self &>(rhs_).data[m], nan_direction_hint);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    void reserve(size_t n) override { data.reserve(n); }

    const char * getFamilyName() const override;

    MutableColumnPtr cloneResized(size_t size) const override;

    Field operator[](size_t n) const override { return typename NearestFieldType<T>::Type(data[n]); }

    void get(size_t n, Field & res) const override { res = typename NearestFieldType<T>::Type(data[n]); }

    UInt64 get64(size_t n) const override;

    UInt64 getUInt(size_t n) const override;

    Int64 getInt(size_t n) const override;

    void insert(const Field & x) override { data.push_back(DB::get<typename NearestFieldType<T>::Type>(x)); }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override;

    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override;

    void getExtremes(Field & min, Field & max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override
    {
        return this->template scatterImpl<Self>(num_columns, selector);
    }

    MutableColumns scatter(
        IColumn::ColumnIndex num_columns,
        const IColumn::Selector & selector,
        const BlockSelectivePtr & selective) const override
    {
        return this->template scatterImpl<Self>(num_columns, selector, selective);
    }

    void scatterTo(IColumn::ScatterColumns & columns, const IColumn::Selector & selector) const override
    {
        this->template scatterToImpl<Self>(columns, selector);
    }
    void scatterTo(
        IColumn::ScatterColumns & columns,
        const IColumn::Selector & selector,
        const BlockSelectivePtr & selective) const override
    {
        this->template scatterToImpl<Self>(columns, selector, selective);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    StringRef getRawData() const override { return StringRef(reinterpret_cast<const char *>(data.data()), byteSize()); }

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container & getData() { return data; }

    const Container & getData() const { return data; }

    const T & getElement(size_t n) const { return data[n]; }

    T & getElement(size_t n) { return data[n]; }

    template <bool selective>
    void updateWeakHash32Impl(WeakHash32 & hash, const BlockSelectivePtr & selective_ptr) const;

protected:
    Container data;
};


} // namespace DB
