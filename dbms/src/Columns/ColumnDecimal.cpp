#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsCommon.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>
#include <common/unaligned.h>


template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

template <typename T>
int ColumnDecimal<T>::compareAt(size_t n, size_t m, const IColumn & rhs_, int) const
{
    auto & other = static_cast<const Self &>(rhs_);
    const T & a = data[n];
    const T & b = other.data[m];

    return decimalLess<T>(b, a, other.scale, scale) ? 1 : (decimalLess<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
StringRef ColumnDecimal<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const TiDB::TiDBCollatorPtr &, String &) const
{
    if constexpr (is_Decimal256)
    {
        /// serialize Decimal256 in `Non-trivial, Binary` way, the serialization logical is
        /// copied from https://github.com/pingcap/boost-extra/blob/master/boost/multiprecision/cpp_int/serialize.hpp#L149
        const typename T::NativeType::backend_type & val = data[n].value.backend();
        bool s = val.sign();
        size_t limb_count = val.size();

        size_t mem_size = sizeof(bool) + sizeof(size_t) + limb_count * sizeof(boost::multiprecision::limb_type);

        auto * pos = arena.allocContinue(mem_size, begin);
        auto * current_pos = pos;
        memcpy(current_pos, &s, sizeof(bool));
        current_pos += sizeof(bool);

        memcpy(current_pos, &limb_count, sizeof(std::size_t));
        current_pos += sizeof(size_t);

        memcpy(current_pos, val.limbs(), limb_count * sizeof(boost::multiprecision::limb_type));

        return StringRef(pos, mem_size);
    }
    else
    {
        auto * pos = arena.allocContinue(sizeof(T), begin);
        memcpy(pos, &data[n], sizeof(T));
        return StringRef(pos, sizeof(T));
    }
}

template <typename T>
const char * ColumnDecimal<T>::deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &)
{
    if constexpr (is_Decimal256)
    {
        /// deserialize Decimal256 in `Non-trivial, Binary` way, the deserialization logical is
        /// copied from https://github.com/pingcap/boost-extra/blob/master/boost/multiprecision/cpp_int/serialize.hpp#L133
        T value;
        auto & val = value.value.backend();

        size_t offset = 0;
        bool s = unalignedLoad<bool>(pos + offset);
        offset += sizeof(bool);
        size_t limb_count = unalignedLoad<size_t>(pos + offset);
        offset += sizeof(size_t);

        val.resize(limb_count, limb_count);
        memcpy(val.limbs(), pos + offset, limb_count * sizeof(boost::multiprecision::limb_type));
        if (s != val.sign())
            val.negate();
        val.normalize();
        data.push_back(value);

        return pos + offset + limb_count * sizeof(boost::multiprecision::limb_type);
    }
    else
    {
        data.push_back(unalignedLoad<T>(pos));
        return pos + sizeof(T);
    }
}

template <typename T>
UInt64 ColumnDecimal<T>::get64(size_t n) const
{
    if constexpr (sizeof(T) > sizeof(UInt64))
        throw Exception(String("Method get64 is not supported for ") + getFamilyName(), ErrorCodes::NOT_IMPLEMENTED);
    return static_cast<UInt64>(static_cast<typename T::NativeType>(data[n]));
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &) const
{
    for (size_t i = 0; i < data.size(); ++i)
    {
        hash_values[i].update(data[i]);
    }
}

template <typename T>
void ColumnDecimal<T>::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    auto s = data.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) + ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        *hash_data = wideIntHashCRC32(*begin, *hash_data);

        ++begin;
        ++hash_data;
    }
}

template <typename T>
void ColumnDecimal<T>::getPermutation(bool reverse, size_t limit, int, IColumn::Permutation & res) const
{
#if 1 /// TODO: perf test
    if (data.size() <= std::numeric_limits<UInt32>::max())
    {
        PaddedPODArray<UInt32> tmp_res;
        permutation(reverse, limit, tmp_res);

        res.resize(tmp_res.size());
        for (size_t i = 0; i < tmp_res.size(); ++i)
            res[i] = tmp_res[i];
        return;
    }
#endif

    permutation(reverse, limit, res);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(size, scale);
    typename Self::Container & res_data = res->getData();

    for (size_t i = 0; i < size; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#ifndef __clang__
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
template <typename T>
MutableColumnPtr ColumnDecimal<T>::cloneResized(size_t size) const
{
    auto res = this->create(0, scale);

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);
        size_t count = std::min(this->size(), size);
        if constexpr (is_Decimal256)
        {
            for (size_t i = 0; i != count; ++i)
                new_col.data[i] = data[i];

            if (size > count)
            {
                T zero{};
                for (size_t i = count; i != size; ++i)
                    new_col.data[i] = zero;
            }
        }
        else
        {
            memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

            if (size > count)
            {
                void * tail = &new_col.data[count];
                memset(tail, 0, (size - count) * sizeof(T));
            }
        }
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::insertData(const char * src [[maybe_unused]], size_t /*length*/)
{
    if constexpr (is_Decimal256)
    {
        throw Exception("insertData is not supported for " + IColumn::getName());
    }
    else
    {
        T tmp;
        memcpy(&tmp, src, sizeof(T));
        data.emplace_back(tmp);
    }
}

template <typename T>
void ColumnDecimal<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnDecimal & src_vec = static_cast<const ColumnDecimal &>(src);

    if (start + length > src_vec.data.size())
        throw Exception("Parameters start = " + toString(start) + ", length = " + toString(length) + " are out of bound in ColumnDecimal<T>::insertRangeFrom method (data.size() = " + toString(src_vec.data.size()) + ").",
                        ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    if constexpr (is_Decimal256)
    {
        for (size_t i = 0; i != length; ++i)
            data[i + old_size] = src_vec.data[i + start];
    }
    else
    {
        memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
    }
}

#pragma GCC diagnostic pop

template <typename T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(0, scale);
    Container & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = data.data();

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(0, scale);
    if (0 == size)
        return res;

    typename Self::Container & res_data = res->getData();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_data.push_back(data[i]);
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnDecimal<T>::getExtremes(Field & min, Field & max) const
{
    if (data.size() == 0)
    {
        min = typename NearestFieldType<T>::Type(T(0), scale);
        max = typename NearestFieldType<T>::Type(T(0), scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T & x : data)
    {
        if (x.value < cur_min.value)
            cur_min = x;
        else if (x.value > cur_max.value)
            cur_max = x;
    }

    min = typename NearestFieldType<T>::Type(cur_min, scale);
    max = typename NearestFieldType<T>::Type(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
template class ColumnDecimal<Decimal256>;

} // namespace DB
