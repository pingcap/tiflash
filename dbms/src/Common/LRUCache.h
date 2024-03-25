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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
namespace DB
{
template <typename K, typename T>
struct TrivialWeightFunction
{
    size_t operator()(const K &, const T &) const { return 1; }
};


/// Thread-safe cache that evicts entries which are not used for a long time.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = TrivialWeightFunction<TKey, TMapped>>
class LRUCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

public:
    /** Initialize LRUCache with max_weight and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    explicit LRUCache(size_t max_weight_, size_t max_elements_size_ = 0)
        : max_weight(std::max(static_cast<size_t>(1), max_weight_))
        , max_elements_size(max_elements_size_)
    {}

    MappedPtr get(const Key & key)
    {
        std::scoped_lock cache_lock(mutex);

        auto res = getImpl(key, cache_lock);
        if (res)
            ++hits;
        else
            ++misses;

        return res;
    }

    /// Returns whether a specific key is in the LRU cache
    /// without updating the LRU order.
    bool contains(const Key & key)
    {
        std::lock_guard cache_lock(mutex);
        return cells.find(key) != cells.end();
    }

    void set(const Key & key, const MappedPtr & mapped)
    {
        std::scoped_lock cache_lock(mutex);

        setImpl(key, mapped, cache_lock);
    }

    /// If the value for the key is in the cache, returns it. If it is not, calls load_func() to
    /// produce it, saves the result in the cache and returns it.
    /// Only one of several concurrent threads calling getOrSet() will call load_func(),
    /// others will wait for that call to complete and will use its result (this helps prevent cache stampede).
    /// Exceptions occurring in load_func will be propagated to the caller. Another thread from the
    /// set of concurrent threads will then try to call its load_func etc.
    ///
    /// Returns std::pair of the cached value and a bool indicating whether the value was produced during this call.
    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        InsertTokenHolder token_holder;
        {
            std::scoped_lock cache_lock(mutex);

            auto val = getImpl(key, cache_lock);
            if (val)
            {
                ++hits;
                return std::make_pair(val, false);
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::scoped_lock token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            return std::make_pair(token->value, false);
        }

        ++misses;
        token->value = load_func();

        std::scoped_lock cache_lock(mutex);

        /// Insert the new value only if the token is still in present in insert_tokens.
        /// (The token may be absent because of a concurrent reset() call).
        bool result = false;
        auto token_it = insert_tokens.find(key);
        if (token_it != insert_tokens.end() && token_it->second.get() == token)
        {
            setImpl(key, token->value, cache_lock);
            result = true;
        }

        if (!token->cleaned_up)
            token_holder.cleanup(token_lock, cache_lock);

        return std::make_pair(token->value, result);
    }

    void remove(const Key & key)
    {
        std::scoped_lock cache_lock(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return;

        Cell & cell = it->second;
        current_weight -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        std::scoped_lock cache_lock(mutex);
        out_hits = hits;
        out_misses = misses;
    }

    size_t weight() const
    {
        std::scoped_lock cache_lock(mutex);
        return current_weight;
    }

    size_t count() const
    {
        std::scoped_lock cache_lock(mutex);
        return cells.size();
    }

    void reset()
    {
        std::scoped_lock cache_lock(mutex);
        queue.clear();
        cells.clear();
        insert_tokens.clear();
        current_weight = 0;
        hits = 0;
        misses = 0;
    }

    bool contains(const Key & key) const
    {
        std::scoped_lock cache_lock(mutex);
        return cells.contains(key);
    }

    virtual ~LRUCache() = default;

private:
    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(LRUCache & cache_)
            : cache(cache_)
        {}

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        MappedPtr value; /// Protected by the token mutex

        LRUCache & cache;
        size_t refcount = 0; /// Protected by the cache mutex
    };

    using InsertTokenById = std::unordered_map<Key, std::shared_ptr<InsertToken>, HashFunction>;

    /// This class is responsible for removing used insert tokens from the insert_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        const Key * key = nullptr;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up = false;

        InsertTokenHolder() = default;

        void acquire(
            const Key * key_,
            const std::shared_ptr<InsertToken> & token_,
            [[maybe_unused]] std::scoped_lock<std::mutex> & cache_lock)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup(
            [[maybe_unused]] std::scoped_lock<std::mutex> & token_lock,
            [[maybe_unused]] std::scoped_lock<std::mutex> & cache_lock)
        {
            token->cache.insert_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        ~InsertTokenHolder()
        {
            if (!token)
                return;

            if (cleaned_up)
                return;

            std::scoped_lock token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::scoped_lock cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;

    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell
    {
        MappedPtr value;
        size_t size = 0;
        LRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    InsertTokenById insert_tokens;

    LRUQueue queue;
    Cells cells;

    /// Total weight of values.
    size_t current_weight = 0;
    const size_t max_weight;
    const size_t max_elements_size;

    mutable std::mutex mutex;
    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};

    const WeightFunction weight_function;

private:
    MappedPtr getImpl(const Key & key, [[maybe_unused]] std::scoped_lock<std::mutex> & cache_lock)
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return MappedPtr();
        }

        Cell & cell = it->second;
        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    void setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::scoped_lock<std::mutex> & cache_lock)
    {
        auto [it, inserted]
            = cells.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());

        Cell & cell = it->second;
        if (inserted)
        {
            try
            {
                cell.queue_iterator = queue.insert(queue.end(), key);
            }
            catch (...)
            {
                // If queue.insert() throws exception, cells and queue will be in inconsistent.
                cells.erase(it);
                tryLogCurrentException(Logger::get(), "queue.insert throw exception");
                throw;
            }
        }
        else
        {
            current_weight -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(key, *cell.value) : 0;
        current_weight += cell.size;

        removeOverflow();
    }

    void removeOverflow()
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();

        while ((current_weight > max_weight || (max_elements_size != 0 && queue_size > max_elements_size))
               && (queue_size > 1))
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            RUNTIME_ASSERT(it != cells.end(), "LRUCache became inconsistent. There must be a bug in it.");

            const auto & cell = it->second;
            current_weight -= cell.size;
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
            --queue_size;
        }

        onRemoveOverflowWeightLoss(current_weight_lost);

        // check for underflow
        RUNTIME_ASSERT(current_weight < (1ull << 63), "LRUCache became inconsistent. There must be a bug in it.");
    }

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


} // namespace DB
