#pragma once

#include <condition_variable>
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
        , tail(lastPosition())
    {
        next.resize(num_streams);
        count.resize(length);
        items.reserve(length);
        for (size_t i = 0; i < length; ++i)
        {
            items.emplace_back(std::make_shared<Item>());
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

    template <typename OtherCondition>
    ItemPtr beginPop(Lock & lock, size_t stream_id, const OtherCondition & other_condition)
    {
        auto condition_1 = [&, this] {
            return next[stream_id] != head;
        };
        if (!waitWithOtherCondition(head_cv, lock, condition_1, other_condition))
            return nullptr;

        return items[next[stream_id]];
    }

    // `endPop` returns true if it is the last stream
    bool endPop(Lock & lock [[maybe_unused]], size_t stream_id)
    {
        size_t index = next[stream_id];
        count[index]--;
        advance(next[stream_id]);

        if (count[index] == 0)
        {
            tail = index;
            tail_cv.notify_all();
            return true;
        }

        return false;
    }

    template <typename OtherCondition>
    ItemPtr beginPush(Lock & lock, const OtherCondition & other_condition)
    {
        auto condition = [this] {
            return head != tail;
        };
        if (!waitWithOtherCondition(tail_cv, lock, condition, other_condition))
            return nullptr;
        return items[head];
    }

    void endPush(Lock & lock [[maybe_unused]])
    {
        count[head] = num_streams;
        advance(head);
        head_cv.notify_all();
    }

    void notify()
    {
        head_cv.notify_all();
        tail_cv.notify_all();
    }

private:
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

    void advance(size_t & position)
    {
        position = position >= lastPosition() ? 0 : position + 1;
    }


    size_t num_streams, length;
    size_t head, tail;
    std::condition_variable head_cv, tail_cv;
    std::vector<size_t> next; // size = num_streams
    std::vector<size_t> count; // size = length
    std::vector<ItemPtr> items; // size = length
};

} // namespace DB
