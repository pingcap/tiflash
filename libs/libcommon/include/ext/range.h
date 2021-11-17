#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <iterator>
#include <type_traits>
#include <utility>


/** \brief Numeric range iterator, used to represent a half-closed interval [begin, end).
 *    In conjunction with std::reverse_iterator allows for forward and backward iteration
 *    over corresponding interval. */
namespace ext
{
template <typename T>
using range_iterator = boost::counting_iterator<T>;

/** \brief Range-based for loop adapter for (reverse_)range_iterator.
 *  By and large should be in conjunction with ext::range and ext::reverse_range.
 */
template <typename T>
struct RangeWrapper
{
    using value_type = typename std::remove_reference<T>::type;
    using iterator = range_iterator<value_type>;

    value_type begin_t;
    value_type end_t;

    iterator begin() const { return iterator(begin_t); }
    iterator end() const { return iterator(end_t); }
};

/** \brief Constructs range_wrapper for forward-iteration over [begin, end) in range-based for loop.
 *  Usage example:
 *      for (const auto i : ext::range(0, 4)) print(i);
 *  Output:
 *      0 1 2 3
 */
template <typename T1, typename T2>
inline RangeWrapper<typename std::common_type<T1, T2>::type> range(T1 begin, T2 end)
{
    using common_type = typename std::common_type<T1, T2>::type;
    return {static_cast<common_type>(begin), static_cast<common_type>(end)};
}
} // namespace ext
