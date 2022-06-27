#pragma once

#include <list>
#include <memory>

namespace DB::DM
{
// CircularScanList is a special circular list.
// It remembers the location of the last iteration and will check whether the object is expired.
template <typename T>
class CircularScanList
{
public:
    using Value = std::shared_ptr<T>;

    CircularScanList()
        : last_itr(l.end())
    {}

    void add(const Value & ptr)
    {
        l.push_back(ptr);
    }

    Value next()
    {
        last_itr = nextItr(last_itr);
        while (!l.empty())
        {
            auto ptr = *last_itr;
            if (ptr->expired())
            {
                last_itr = l.erase(last_itr);
                if (last_itr == l.end())
                {
                    last_itr = l.begin();
                }
            }
            else
            {
                return ptr;
            }
        }
        return nullptr;
    }

    // <unexpired_count, expired_count>
    std::pair<int64_t, int64_t> count() const
    {
        int64_t expired_count = 0;
        for (const auto & p : l)
        {
            expired_count += static_cast<int>(p->expired());
        }
        return {l.size() - expired_count, expired_count};
    }

    Value get(uint64_t pool_id) const
    {
        for (const auto & p : l)
        {
            if (p->getId() == pool_id)
            {
                return p;
            }
        }
        return nullptr;
    }

private:
    using Iter = typename std::list<Value>::iterator;
    Iter nextItr(Iter itr)
    {
        if (itr == l.end() || std::next(itr) == l.end())
        {
            return l.begin();
        }
        else
        {
            return std::next(itr);
        }
    }

    std::list<Value> l;
    Iter last_itr;
};

} // namespace DB::DM