#pragma once

#include <Common/TiFlashException.h>

#include <condition_variable>
#include <unordered_map>
#include <vector>

namespace DB
{

template <typename Item>
class MultiStreamRingBuffer
{
public:
    using Lock = std::unique_lock<std::mutex>;
    using ItemPtr = std::shared_ptr<Item>;

    explicit MultiStreamRingBuffer(size_t num_streams_, size_t length_)
        : num_streams(num_streams_)
        , length(length_)
        , head(0)
        , mid(0)
        , tail(lastPosition())
    {
        next.resize(num_streams);
        count.resize(length);

        items.reserve(length);
        for (size_t i = 0; i < length; ++i)
        {
            items.emplace_back(std::make_shared<Item>());
            pos[items.back()] = i;
        }
    }

    size_t getNumStreams() const
    {
        return num_streams;
    }

    size_t getLength() const
    {
        return length;
    }


    class OpBase
    {
    public:
        OpBase()
            : parent(nullptr)
            , state(OpState::INVALID)
        {}
        explicit OpBase(MultiStreamRingBuffer * parent_)
            : parent(parent_)
            , state(OpState::RUNNING)
        {}

        bool isValid() const
        {
            return state != OpState::INVALID;
        }

        bool isCommitted() const
        {
            return state == OpState::COMMITTED;
        }

    protected:
        enum class OpState
        {
            INVALID,
            RUNNING,
            COMMITTED
        };

        MultiStreamRingBuffer * parent;
        OpState state;

        void markAsCommitted()
        {
            state = OpState::COMMITTED;
        }

        void markAsInvalid()
        {
            state = OpState::INVALID;
        }
    };

    class Pop final : public OpBase
    {
    public:
        Pop() = default;
        Pop(MultiStreamRingBuffer * parent, size_t stream_id_)
            : OpBase(parent)
            , stream_id(stream_id_)
            , item(parent->items[this->parent->next[stream_id]])
        {}

        bool commit(Lock & lock)
        {
            bool result = this->parent->endPop(lock, stream_id);
            this->markAsCommitted();
            return result;
        }

        size_t stream_id;
        ItemPtr item;
    };

    friend class Pop;

    template <typename OtherCondition>
    Pop beginPop(Lock & lock, size_t stream_id, const OtherCondition & other_condition)
    {
        auto condition_1 = [&, this] {
            return next[stream_id] != head;
        };
        if (!waitWithOtherCondition(head_cv, lock, condition_1, other_condition))
            return {};

        return Pop(this, stream_id);
    }

    class Push final : public OpBase
    {
    public:
        Push() = default;
        Push(MultiStreamRingBuffer * parent, size_t index)
            : OpBase(parent)
            , item(parent->items[index])
        {
            if (parent->pos[item] != index)
                throw TiFlashException("Inconsistent index", Errors::Coprocessor::Internal);
        }

        void commit(Lock & lock [[maybe_unused]])
        {
            assert(this->isValid() && !this->isCommitted());

            auto p = this->parent;

            // i: current position of item pointer in items array
            // move item to the front of queue
            size_t index = p->pos[item];
            auto & u = p->items[index];
            auto & v = p->items[p->head];
            if (u != item)
                throw TiFlashException("Invalid item", Errors::Coprocessor::Internal);
            if (p->pos[v] != p->head)
                throw TiFlashException("Invalid head", Errors::Coprocessor::Internal);

            std::swap(u, v);

            // update position mapping
            p->pos[u] = index;
            p->pos[v] = p->head;

            // move head pointer and notify others
            p->count[p->head] = p->num_streams;
            p->forward(p->head);
            p->head_cv.notify_all();

            this->markAsCommitted();
        }

        void cancel(Lock & lock [[maybe_unused]])
        {
            // originally I want to implement this as dtor of class Push
            // but I realized that cancel needs to hold the lock
            // this is not elegant

            if (!this->isValid())
                return;

            auto p = this->parent;
            p->back(p->mid);

            size_t index = p->pos[item];
            auto & u = p->items[index];
            auto & v = p->items[p->mid];
            if (u != item)
                throw TiFlashException("Invalid item", Errors::Coprocessor::Internal);
            if (p->pos[v] != p->mid)
                throw TiFlashException("Invalid mid", Errors::Coprocessor::Internal);

            std::swap(u, v);

            p->pos[u] = index;
            p->pos[v] = p->mid;

            p->tail_cv.notify_all();

            this->markAsInvalid();
        }

        ItemPtr item;
    };

    friend class Push;

    template <typename OtherCondition>
    Push beginPush(Lock & lock, const OtherCondition & other_condition)
    {
        auto condition = [this] {
            return mid != tail;
        };
        if (!waitWithOtherCondition(tail_cv, lock, condition, other_condition))
            return {};
        size_t index = mid;
        forward(mid);
        return Push(this, index);
    }

    void notify()
    {
        head_cv.notify_all();
        tail_cv.notify_all();
    }

private:
    // `endPop` returns true if it is the last stream
    bool endPop(Lock & lock [[maybe_unused]], size_t stream_id)
    {
        size_t index = next[stream_id];
        count[index]--;
        forward(next[stream_id]);

        if (count[index] == 0)
        {
            tail = index;
            tail_cv.notify_all();
            return true;
        }

        return false;
    }

    template <typename MainCondition, typename OtherCondition>
    static bool waitWithOtherCondition(
        std::condition_variable & cv,
        Lock & lock,
        const MainCondition & main_condition,
        const OtherCondition & other_condition)
    {
        cv.wait(lock, [&] {
            return main_condition() || other_condition();
        });
        return main_condition();
    }

    size_t lastPosition() const
    {
        return length - 1;
    }

    void forward(size_t & position)
    {
        position = position >= lastPosition() ? 0 : position + 1;
    }

    void back(size_t & position)
    {
        position = position <= 0 ? lastPosition() : position - 1;
    }


    size_t num_streams, length;
    size_t head, mid, tail;
    std::condition_variable head_cv, tail_cv;
    std::unordered_map<ItemPtr, size_t> pos;
    std::vector<size_t> next; // size = num_streams
    std::vector<size_t> count; // size = length
    std::vector<ItemPtr> items; // size = length
};

} // namespace DB
