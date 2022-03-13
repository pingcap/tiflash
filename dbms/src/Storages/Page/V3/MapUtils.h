#pragma once

namespace DB::MapUtils
{
// Return an iterator to the last element whose key is less than or equal to `key`.
// If no such element is found, the past-the-end iterator is returned.
template <typename C>
typename C::const_iterator
findLessEQ(const C & c, const typename C::key_type & key)
{
    auto iter = c.upper_bound(key); // first element > `key`
    // Nothing greater than key
    if (iter == c.cbegin())
        return c.cend();
    // its prev must be less than or equal to `key`
    return --iter;
}

template <typename C>
typename C::const_iterator
findLess(const C & c, const typename C::key_type & key)
{
    auto iter = c.lower_bound(key); // first element >= `key`
    if (iter == c.cbegin())
        return c.cend(); // Nothing < `key`
    // its prev must be less than `key`
    return --iter;
}

template <typename C>
typename C::iterator
findMutLess(C & c, const typename C::key_type & key)
{
    auto iter = c.lower_bound(key); // first element >= `key`
    if (iter == c.begin())
        return c.end(); // Nothing < `key`
    // its prev must be less than `key`
    return --iter;
}

} // namespace DB::MapUtils
