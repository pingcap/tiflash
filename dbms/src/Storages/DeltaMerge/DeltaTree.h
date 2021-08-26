#pragma once

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Tuple.h>
#include <common/logger_useful.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <queue>

namespace DB
{
namespace DM
{
struct DTMutation;
template <size_t M, size_t F, size_t S>
struct DTLeaf;
template <size_t M, size_t F, size_t S>
struct DTIntern;

using TupleRefs = std::vector<size_t>;

#define as(T, p) (reinterpret_cast<T *>(p))
#define asNode(p) (reinterpret_cast<void *>(p))
#define isLeaf(p) (((*reinterpret_cast<size_t *>(p)) & 0x01) != 0)
#define nodeName(p) (isLeaf(p) ? "leaf" : "intern")

inline std::string addrToHex(const void * addr)
{
    if (!addr)
        return "null";
    std::stringstream ss;
    ss << addr;
    return ss.str();
}


using DT_TypeCount = UInt32;
using DT_Id = UInt32;
using DT_Delta = Int32;

inline UInt64 checkId(UInt64 id)
{
    if (unlikely(id >= std::numeric_limits<DT_Id>::max()))
        throw Exception("Illegal id: " + DB::toString(id));
    return id;
}

inline Int64 checkDelta(Int64 delta)
{
    if (unlikely(delta < std::numeric_limits<DT_Delta>::min() || delta >= std::numeric_limits<DT_Delta>::max()))
        throw Exception("Illegal delta: " + DB::toString(delta));
    return delta;
}

namespace DTType
{
static constexpr DT_TypeCount TYPE_MASK = 1;

inline std::string DTTypeString(bool is_insert)
{
    return is_insert ? "INS" : "DEL";
}

inline DT_TypeCount getTypeCount(bool is_insert, UInt32 count)
{
    return (count << 1) | (DT_TypeCount)is_insert;
}

inline UInt32 getCount(DT_TypeCount type_count)
{
    return type_count >> 1;
}

inline bool isInsert(DT_TypeCount type_count)
{
    return type_count & TYPE_MASK;
}

inline bool isDelete(DT_TypeCount type_count)
{
    return !(type_count & TYPE_MASK);
}

inline DT_TypeCount updateCount(DT_TypeCount type_count, UInt32 count)
{
    return (count << 1) | (DT_TypeCount)isInsert(type_count);
}

} // namespace DTType


struct DTMutation
{
    DTMutation() = default;
    DTMutation(bool is_insert, UInt32 count, UInt64 value_)
        : type_count(DTType::getTypeCount(is_insert, count))
        , value(value_)
    {}

    /// The lowest bit of type_count indicates whether this is a insert or not (delete).
    /// And the rest bits represent the inserted or deleted rows.
    DT_TypeCount type_count = 0;
    /// For DT_INS, "value" is the value index (tuple_id) in value space;
    DT_Id value = 0;

    bool isInsert() const { return DTType::isInsert(type_count); }
    bool isDelete() const { return DTType::isDelete(type_count); }
    UInt32 count() const { return DTType::getCount(type_count); }
    void setCount(UInt32 v) { type_count = DTType::updateCount(type_count, v); }
};

/// Note that we allocate one more slot for entries in DTIntern and DTLeaf, to simplify entry insert operation.

template <size_t M, size_t F, size_t S>
struct DTLeaf
{
    using NodePtr = void *;
    using Leaf = DTLeaf<M, F, S>;
    using Intern = DTIntern<M, F, S>;
    using LeafPtr = Leaf *;
    using InternPtr = Intern *;

    DTLeaf() = default;

    const size_t mark = 1; // <-- This mark MUST be declared at first place!

    DT_Id sids[M * S + 1];
    DTMutation mutations[M * S + 1];
    size_t count = 0; // mutations count

    LeafPtr prev = nullptr;
    LeafPtr next = nullptr;
    InternPtr parent = nullptr;

    DTLeaf(const Leaf & o) = default;

    std::string toString()
    {
        return "{count:" + DB::toString(count) + ",prev:" + addrToHex(prev) + ",next:" + addrToHex(next) + ",parent:" + addrToHex(parent)
            + "}";
    }

    inline UInt64 sid(size_t pos) const { return sids[pos]; }
    inline UInt64 rid(size_t pos, Int64 delta) const { return sids[pos] + delta; }
    inline UInt16 isInsert(size_t pos) const { return mutations[pos].isInsert(); }
    inline UInt32 mut_count(size_t pos) const { return mutations[pos].count(); }
    inline UInt64 value(size_t pos) const { return mutations[pos].value; }

    static inline bool overflow(size_t count) { return count > M * S; }
    static inline bool underflow(size_t count) { return count < M; }
    inline bool legal() { return !overflow(count) && !underflow(count); }
    inline std::string state() { return overflow(count) ? "overflow" : (underflow(count) ? "underflow" : "legal"); }

    /// shift entries from pos with n steps.
    inline void shiftEntries(size_t pos, int n)
    {
        if (n == 0)
        {
            return;
        }
        if (n > 0)
        {
            std::move_backward(std::begin(sids) + pos, std::begin(sids) + count, std::begin(sids) + count + n);
            std::move_backward(std::begin(mutations) + pos, std::begin(mutations) + count, std::begin(mutations) + count + n);
        }
        else
        {
            std::move(std::begin(sids) + pos, std::begin(sids) + count, std::begin(sids) + pos + n);
            std::move(std::begin(mutations) + pos, std::begin(mutations) + count, std::begin(mutations) + pos + n);
        }
    }

    /// calculate the delta this leaf node generated.
    inline Int64 getDelta() const
    {
        Int64 delta = 0;
        for (size_t i = 0; i < count; ++i)
        {
            const auto & m = mutations[i];
            if (m.isInsert())
                delta += 1;
            else
                delta -= m.count();
        }
        return delta;
    }

    /// Search the first pos with equal or greater id.
    /// Returns <pos_in_node, delta>.
    template <bool isRid>
    inline std::pair<size_t, Int64> search(const UInt64 id, Int64 delta) const
    {
        size_t i = 0;
        for (; i < count; ++i)
        {
            if constexpr (isRid)
            {
                if (id <= rid(i, delta))
                    return {i, delta};
            }
            else
            {
                if (id <= sid(i))
                    return {i, delta};
            }
            if (isInsert(i))
                delta += 1;
            else
                delta -= mut_count(i);
        }
        return {i, delta};
    }

    template <bool isRid>
    inline bool exists(const UInt64 id, Int64 delta) const
    {
        return search<isRid>(id, delta).first < count;
    }

    inline std::pair<size_t, Int64> searchRid(const UInt64 id, Int64 delta) const { return search<true>(id, delta); }
    inline std::pair<size_t, Int64> searchSid(const UInt64 id, Int64 delta) const { return search<false>(id, delta); }

    inline bool existsRid(const UInt64 id, Int64 delta) const { return exists<true>(id, delta); }
    inline bool existsSid(const UInt64 id) const { return exists<false>(id, 0); }

    /// Split into two nodes.
    /// This mehod only handle the pre/next/parent link, but won't handle the updates in parent node.
    /// Returns the new splited sid.
    inline UInt64 split(LeafPtr right_n)
    {
        size_t split = M * S / 2;

        right_n->prev = this;
        right_n->next = this->next;
        right_n->parent = this->parent;

        if (this->next)
            this->next->prev = right_n;
        this->next = right_n;

        std::move(std::begin(this->sids) + split, std::begin(this->sids) + this->count, std::begin(right_n->sids));
        std::move(std::begin(this->mutations) + split, std::begin(this->mutations) + this->count, std::begin(right_n->mutations));

        right_n->count = this->count - split;
        this->count = split;

        return right_n->sids[0];
    }

    /// Merge this node and the sibling.
    /// Note that sibling should be deleted outside.
    inline void merge(LeafPtr sibling, bool left, size_t node_pos)
    {
        adopt(sibling, left, sibling->count, node_pos);
        if (left)
        {
            this->prev = sibling->prev;
            if (this->prev)
                this->prev->next = this;
        }
        else
        {
            this->next = sibling->next;
            if (this->next)
                this->next->prev = this;
        }
    }

    /// Adopt one entry from sibling, whether sibling is from left or right are handled in different way.
    /// Returns new separator sid.
    inline UInt64 adopt(LeafPtr sibling, bool left, size_t adopt_count, size_t /*node_pos*/)
    {
        if (left)
        {
            this->shiftEntries(0, adopt_count);

            auto sibling_cut = sibling->count - adopt_count;
            std::move(std::begin(sibling->sids) + sibling_cut, std::begin(sibling->sids) + sibling->count, std::begin(this->sids));
            std::move(
                std::begin(sibling->mutations) + sibling_cut,
                std::begin(sibling->mutations) + sibling->count,
                std::begin(this->mutations));

            sibling->count -= adopt_count;
            this->count += adopt_count;

            return this->sids[0];
        }
        else
        {
            std::move(std::begin(sibling->sids), std::begin(sibling->sids) + adopt_count, std::begin(this->sids) + this->count);
            std::move(
                std::begin(sibling->mutations),
                std::begin(sibling->mutations) + adopt_count,
                std::begin(this->mutations) + this->count);

            sibling->shiftEntries(adopt_count, -adopt_count);

            sibling->count -= adopt_count;
            this->count += adopt_count;

            return sibling->sids[0];
        }
    }
};

template <size_t M, size_t F, size_t S>
struct DTIntern
{
    using NodePtr = void *;
    using Leaf = DTLeaf<M, F, S>;
    using Intern = DTIntern<M, F, S>;
    using LeafPtr = Leaf *;
    using InternPtr = Intern *;

    DTIntern() = default;

    const size_t mark = 0; // <-- This mark MUST be declared at first place!

    DT_Id sids[F * S + 1];
    DT_Delta deltas[F * S + 1];
    NodePtr children[F * S + 1];
    size_t count = 0; // deltas / children count, and the number of sids is "count - 1"

    InternPtr parent = nullptr;

    DTIntern(const Intern & o) = default;

    std::string toString() { return "{count:" + DB::toString(count) + ",parent:" + addrToHex(parent) + "}"; }

    inline UInt64 sid(size_t pos) const { return sids[pos]; }
    inline UInt64 rid(size_t pos, Int64 delta) const { return sids[pos] + delta; }

    static inline bool overflow(size_t count) { return count > F * S; }
    static inline bool underflow(size_t count) { return count < F; }
    inline bool legal() { return !overflow(count) && !underflow(count); }
    inline std::string state() { return overflow(count) ? "overflow" : (underflow(count) ? "underflow" : "legal"); }

    /// shift entries from pos with n steps.
    inline void shiftEntries(size_t child_pos, int n)
    {
        if (n == 0)
        {
            return;
        }
        else if (n > 0)
        {
            if (child_pos != count)
            {
                if (count + n > F * S + 1)
                    throw Exception("Overflow");
            }

            std::move_backward(std::begin(sids) + child_pos, std::begin(sids) + count, std::begin(sids) + count + n);
            std::move_backward(std::begin(deltas) + child_pos, std::begin(deltas) + count, std::begin(deltas) + count + n);
            std::move_backward(std::begin(children) + child_pos, std::begin(children) + count, std::begin(children) + count + n);
            if (((int)child_pos) - 1 >= 0)
                sids[child_pos - 1 + n] = sids[child_pos - 1];

            return;
        }
        else
        {
            if (child_pos != count)
            {
                if ((Int64)child_pos < -n)
                    throw Exception("Underflow");
            }

            if (((int)child_pos) - 1 + n >= 0)
                sids[child_pos - 1 + n] = sids[child_pos - 1];
            std::move(std::begin(sids) + child_pos, std::begin(sids) + count, std::begin(sids) + child_pos + n);
            std::move(std::begin(deltas) + child_pos, std::begin(deltas) + count, std::begin(deltas) + child_pos + n);
            std::move(std::begin(children) + child_pos, std::begin(children) + count, std::begin(children) + child_pos + n);
            return;
        }
    }

    inline size_t searchChild(NodePtr child)
    {
        size_t i = 0;
        for (; i < count && children[i] != child; ++i) {}
        return i;
    }

    inline Int64 getDelta()
    {
        Int64 delta = 0;
        for (size_t i = 0; i < count; ++i)
        {
            delta += deltas[i];
        }
        return delta;
    }

    inline void refreshChildParent()
    {
        if (isLeaf(children[0]))
            for (size_t i = 0; i < count; ++i)
                as(Leaf, children[i])->parent = this;
        else
            for (size_t i = 0; i < count; ++i)
                as(Intern, children[i])->parent = this;
    }

    /// Split into two nodes.
    /// Returns the new separator sid.
    inline UInt64 split(InternPtr right_n)
    {
        size_t split = F * S / 2;

        right_n->parent = this->parent;

        std::move(std::begin(this->sids) + split, std::begin(this->sids) + this->count, std::begin(right_n->sids));
        std::move(std::begin(this->deltas) + split, std::begin(this->deltas) + this->count, std::begin(right_n->deltas));
        std::move(std::begin(this->children) + split, std::begin(this->children) + this->count, std::begin(right_n->children));

        right_n->count = this->count - split;
        this->count = split;

        this->refreshChildParent();
        right_n->refreshChildParent();

        return this->sids[this->count - 1];
    }

    /// Merge this node and the sibling, node_pos is the position of currently node in parent.
    /// Note that sibling should be deleted outside.
    inline void merge(InternPtr sibling, bool left, size_t node_pos) { adopt(sibling, left, sibling->count, node_pos); }

    /// Adopt entries from sibling, whether sibling is from left or right are handled in different way.
    /// node_pos is the position of currently node in parent.
    /// Returns the new separator sid.
    inline UInt64 adopt(InternPtr sibling, bool left, size_t adopt_count, size_t node_pos)
    {
        if (left)
        {
            this->shiftEntries(0, adopt_count);

            auto sibling_cut = sibling->count - adopt_count;
            // if adopt_count equals to sibling->count, new_sep_sid is meaningless.
            auto new_sep_sid = !sibling_cut ? 0 : sibling->sids[sibling_cut - 1];

            std::move(std::begin(sibling->sids) + sibling_cut, std::begin(sibling->sids) + sibling->count - 1, std::begin(this->sids));
            std::move(std::begin(sibling->deltas) + sibling_cut, std::begin(sibling->deltas) + sibling->count, std::begin(this->deltas));
            std::move(
                std::begin(sibling->children) + sibling_cut,
                std::begin(sibling->children) + sibling->count,
                std::begin(this->children));

            this->sids[adopt_count - 1] = parent->sids[node_pos - 1];

            sibling->count -= adopt_count;
            this->count += adopt_count;

            this->refreshChildParent();

            return new_sep_sid;
        }
        else
        {
            auto new_sep_sid = adopt_count == sibling->count ? 0 : sibling->sids[adopt_count - 1];

            std::move(std::begin(sibling->sids), std::begin(sibling->sids) + adopt_count, std::begin(this->sids) + this->count);
            std::move(std::begin(sibling->deltas), std::begin(sibling->deltas) + adopt_count, std::begin(this->deltas) + this->count);
            std::move(std::begin(sibling->children), std::begin(sibling->children) + adopt_count, std::begin(this->children) + this->count);

            sibling->shiftEntries(adopt_count, -adopt_count);

            this->sids[this->count - 1] = parent->sids[node_pos];

            sibling->count -= adopt_count;
            this->count += adopt_count;

            this->refreshChildParent();

            return new_sep_sid;
        }
    }
};

template <size_t M, size_t F, size_t S>
class DTEntryIterator
{
    using LeafPtr = DTLeaf<M, F, S> *;

    LeafPtr leaf;
    size_t pos;
    Int64 delta;

public:
    DTEntryIterator() = default;
    DTEntryIterator(LeafPtr leaf_, size_t pos_, Int64 delta_)
        : leaf(leaf_)
        , pos(pos_)
        , delta(delta_)
    {}

    std::string toString() { return "{leaf:" + addrToHex(leaf) + ",pos:" + DB::toString(pos) + ",delta:" + DB::toString(delta) + "}"; }

    bool operator==(const DTEntryIterator & rhs) const { return leaf == rhs.leaf && pos == rhs.pos; }
    bool operator!=(const DTEntryIterator & rhs) const { return !(*this == rhs); }

    DTEntryIterator & operator++()
    {
        if (unlikely(pos >= leaf->count))
            throw Exception("Illegal ++ operation on " + toString());

        if (leaf->isInsert(pos))
            delta += 1;
        else
            delta -= leaf->mut_count(pos);

        if (++pos >= leaf->count && leaf->next)
        {
            leaf = leaf->next;
            pos = 0;
        }

        return *this;
    }

    DTEntryIterator & operator--()
    {
        if (pos > 0)
        {
            --pos;
        }
        else
        {
            leaf = leaf->prev;
            pos = leaf->count - 1;
        }

        if (leaf->isInsert(pos))
            delta -= 1;
        else
            delta += leaf->mut_count(pos);

        return *this;
    }

    DTMutation getMutation() const { return leaf->mutations[pos]; }
    LeafPtr getLeaf() const { return leaf; }
    size_t getPos() const { return pos; }
    Int64 getDelta() const { return delta; }
    bool isInsert() const { return leaf->mutations[pos].isInsert(); }
    bool isDelete() const { return leaf->mutations[pos].isDelete(); }
    UInt32 getCount() const { return leaf->mutations[pos].count(); }
    UInt64 getValue() const { return leaf->mutations[pos].value; }
    UInt64 getSid() const { return leaf->sids[pos]; }
    UInt64 getRid() const { return leaf->sids[pos] + delta; }

    void setValue(UInt64 value) { leaf->mutations[pos].value = checkId(value); }
};

template <size_t M, size_t F, size_t S, typename Allocator>
class DTEntriesCopy : Allocator
{
    using LeafPtr = DTLeaf<M, F, S> *;

    const size_t entry_count;
    const Int64 delta;
    UInt64 * const sids;
    DTMutation * const mutations;

public:
    DTEntriesCopy(LeafPtr left_leaf, size_t entry_count_, Int64 delta_)
        : entry_count(entry_count_)
        , delta(delta_)
        , sids(reinterpret_cast<UInt64 *>(this->alloc(sizeof(UInt64) * entry_count)))
        , mutations(reinterpret_cast<DTMutation *>(this->alloc(sizeof(DTMutation) * entry_count)))
    {
        size_t offset = 0;
        while (left_leaf)
        {
            std::move(left_leaf->sids, left_leaf->sids + left_leaf->count, sids + offset);
            std::move(left_leaf->mutations, left_leaf->mutations + left_leaf->count, mutations + offset);

            offset += left_leaf->count;
            left_leaf = left_leaf->next;
        }
    }

    size_t entryCount() { return entry_count; }

    ~DTEntriesCopy()
    {
        this->free(sids, sizeof(UInt64) * entry_count);
        this->free(mutations, sizeof(DTMutation) * entry_count);
    }

    class Iterator
    {
    private:
        std::shared_ptr<DTEntriesCopy> entries_holder; // Holds a reference, stop being freed.
        DTEntriesCopy * entries;

        size_t index = 0;
        Int64 delta;

    public:
        Iterator(const std::shared_ptr<DTEntriesCopy> & entries_, size_t index_, Int64 delta_)
            : entries_holder(entries_)
            , entries(entries_.get())
            , index(index_)
            , delta(delta_)
        {
        }

        bool operator==(const Iterator & rhs) const { return index == rhs.index; }
        bool operator!=(const Iterator & rhs) const { return index != rhs.index; }

        Iterator & operator++()
        {
            if (entries->mutations[index].isInsert())
                delta += 1;
            else
                delta -= entries->mutations[index].count();

            ++index;

            return *this;
        }

        Iterator & operator--()
        {
            --index;

            if (entries->mutations[index].isInsert())
                delta -= 1;
            else
                delta += entries->mutations[index].count();

            return *this;
        }

        Int64 getDelta() const { return delta; }
        bool isInsert() const { return entries->mutations[index].isInsert(); }
        bool isDelete() const { return entries->mutations[index].isDelete(); }
        UInt32 getCount() const { return entries->mutations[index].count(); }
        UInt64 getValue() const { return entries->mutations[index].value; }
        UInt64 getSid() const { return entries->sids[index]; }
        UInt64 getRid() const { return entries->sids[index] + delta; }
    };

    static Iterator begin(const std::shared_ptr<DTEntriesCopy> & entries) { return {entries, 0, 0}; }
    static Iterator end(const std::shared_ptr<DTEntriesCopy> & entries) { return {entries, entries->entry_count, entries->delta}; }
};

/// Compact the continuing inserts.
template <size_t M, size_t F, size_t S>
class DTCompactedEntries
{
    using EntryIterator = DTEntryIterator<M, F, S>;

public:
    struct Entry
    {
        UInt64 sid;
        bool is_insert;
        UInt32 count;
        UInt64 value;
    };
    using Entries = std::vector<Entry>;

    struct Iterator
    {
        typename Entries::iterator it;

        Iterator(typename Entries::iterator it_)
            : it(it_)
        {}
        bool operator==(const Iterator & rhs) const { return it == rhs.it; }
        bool operator!=(const Iterator & rhs) const { return it != rhs.it; }
        Iterator & operator++()
        {
            ++it;
            return *this;
        }

        UInt64 getSid() const { return it->sid; }
        bool isInsert() const { return it->is_insert; }
        bool isDelete() const { return !it->is_insert; }
        UInt32 getCount() const { return it->count; }
        UInt64 getValue() const { return it->value; }
    };

private:
    Entries entries;

public:
    DTCompactedEntries(const EntryIterator & begin, const EntryIterator & end, size_t entry_count)
    {
        entries.reserve(entry_count);

        for (auto it = begin; it != end; ++it)
        {
            if (!entries.empty() && it.isInsert())
            {
                auto & prev_index = entries.back();
                if (prev_index.is_insert //
                    && prev_index.sid == it.getSid() //
                    && prev_index.value + prev_index.count == it.getValue())
                {
                    // Merge current insert entry into previous one.
                    prev_index.count += it.getCount();
                    continue;
                }
            }
            Entry entry = {.sid = it.getSid(), .is_insert = it.isInsert(), .count = it.getCount(), .value = it.getValue()};
            entries.emplace_back(entry);
        }
    }

    auto begin() { return Iterator(entries.begin()); }
    auto end() { return Iterator(entries.end()); }
};

template <class ValueSpace, size_t M, size_t F, size_t S, typename Allocator>
class DeltaTree
{
public:
    using Self = DeltaTree<ValueSpace, M, F, S, Allocator>;
    using NodePtr = void *;
    using Leaf = DTLeaf<M, F, S>;
    using Intern = DTIntern<M, F, S>;
    using LeafPtr = Leaf *;
    using InternPtr = Intern *;
    using EntryIterator = DTEntryIterator<M, F, S>;
    using ValueSpacePtr = std::shared_ptr<ValueSpace>;

    using CompactedEntries = DTCompactedEntries<M, F, S>;
    using CompactedEntriesPtr = std::shared_ptr<CompactedEntries>;

    static_assert(M >= 2);
    static_assert(F >= 2);
    static_assert(S >= 2);
    /// We rely on the standard layout to determine whether a node is Leaf or Intern.
    static_assert(std::is_standard_layout_v<Leaf>);
    static_assert(std::is_standard_layout_v<Intern>);

private:
    NodePtr root;
    LeafPtr left_leaf, right_leaf;
    size_t height = 1;

    size_t num_inserts = 0;
    size_t num_deletes = 0;
    size_t num_entries = 0;

    Allocator * allocator;
    size_t bytes = 0;

    Poco::Logger * log;

public:
    // For test cases only.
    ValueSpacePtr insert_value_space;

private:
    inline bool isRootOnly() const { return height == 1; }

    void check(NodePtr node, bool recursive) const;

    template <bool is_rid, bool is_left>
    EntryIterator findLeaf(const UInt64 id) const;

    /// Find the leaf which could contains id.
    template <bool is_rid>
    EntryIterator findRightLeaf(const UInt64 id) const
    {
        return findLeaf<is_rid, false>(id);
    }

    template <bool is_rid, bool is_left>
    void searchId(EntryIterator & it, const UInt64 id) const;

    /// Go to first entry that has greater or equal id.
    template <bool is_rid>
    void searchLeftId(EntryIterator & it, const UInt64 id) const
    {
        return searchId<is_rid, true>(it, id);
    }

    using InterAndSid = std::pair<InternPtr, UInt64>;
    template <typename T>
    InterAndSid submitMinSid(T * node, UInt64 subtree_min_sid);

    template <typename T>
    InternPtr afterNodeUpdated(T * node);

    inline void afterLeafUpdated(LeafPtr leaf)
    {
        if (leaf->count == 0 && isRootOnly())
            return;

        InternPtr next;
        UInt64 subtree_min_sid = 0;
        std::tie(next, subtree_min_sid) = submitMinSid(leaf, subtree_min_sid);
        while (next)
        {
            std::tie(next, subtree_min_sid) = submitMinSid(next, subtree_min_sid);
        }

        next = afterNodeUpdated(leaf);
        while (next)
        {
            next = afterNodeUpdated(next);
        }
    }

    template <typename T>
    void freeNode(T * node)
    {
        allocator->free(reinterpret_cast<char *>(node), sizeof(T));

        bytes -= sizeof(T);
    }

    template <typename T>
    T * createNode()
    {
        T * n = reinterpret_cast<T *>(allocator->alloc(sizeof(T)));
        new (n) T();

        bytes += sizeof(T);

        return n;
    }

    template <typename T>
    void freeTree(T * node)
    {
        constexpr bool is_leaf = std::is_same<Leaf, T>::value;
        if constexpr (!is_leaf)
        {
            InternPtr intern = static_cast<InternPtr>(node);
            if (intern->count)
            {
                if (isLeaf(intern->children[0]))
                    for (size_t i = 0; i < intern->count; ++i)
                        freeTree<Leaf>(as(Leaf, intern->children[i]));
                else
                    for (size_t i = 0; i < intern->count; ++i)
                        freeTree<Intern>(as(Intern, intern->children[i]));
            }
        }
        freeNode<T>(node);
    }

    void init(const ValueSpacePtr & insert_value_space_)
    {
        allocator = new Allocator();

        log = &Poco::Logger::get("DeltaTree");

        insert_value_space = insert_value_space_;

        root = createNode<Leaf>();
        left_leaf = right_leaf = as(Leaf, root);

        LOG_TRACE(log, "create");
    }

public:
    DeltaTree() { init(std::make_shared<ValueSpace>()); }
    DeltaTree(const ValueSpacePtr & insert_value_space_) { init(insert_value_space_); }
    DeltaTree(const Self & o);

    DeltaTree & operator=(const Self & o)
    {
        Self tmp(o);
        this->swap(tmp);
        return *this;
    }

    DeltaTree & operator=(Self && o) noexcept
    {
        this->swap(o);
        return *this;
    }

    void swap(Self & other)
    {
        std::swap(root, other.root);

        std::swap(left_leaf, other.left_leaf);
        std::swap(right_leaf, other.right_leaf);
        std::swap(height, other.height);

        std::swap(num_inserts, other.num_inserts);
        std::swap(num_deletes, other.num_deletes);
        std::swap(num_entries, other.num_entries);

        std::swap(log, other.log);

        std::swap(allocator, allocator);

        insert_value_space.swap(other.insert_value_space);
    }

    ~DeltaTree()
    {
        if (isLeaf(root))
            freeTree<Leaf>((LeafPtr)root);
        else
            freeTree<Intern>((InternPtr)root);

        delete allocator;

        LOG_TRACE(log, "free");
    }

    void checkAll() const
    {
        LeafPtr p = left_leaf;
        size_t count = 0;
        for (; p != right_leaf; p = p->next)
        {
            count += p->count;
        }
        count += right_leaf->count;
        if (count != num_entries)
            throw Exception("entries count not match");

        check(root, true);
    }

    size_t getBytes() { return bytes; }

    size_t getHeight() const { return height; }
    EntryIterator begin() const { return EntryIterator(left_leaf, 0, 0); }
    EntryIterator end() const
    {
        Int64 delta = isLeaf(root) ? as(Leaf, root)->getDelta() : as(Intern, root)->getDelta();
        return EntryIterator(right_leaf, right_leaf->count, delta);
    }

    template <typename CopyAllocator>
    std::shared_ptr<DTEntriesCopy<M, F, S, CopyAllocator>> getEntriesCopy()
    {
        Int64 delta = isLeaf(root) ? as(Leaf, root)->getDelta() : as(Intern, root)->getDelta();
        return std::make_shared<DTEntriesCopy<M, F, S, CopyAllocator>>(left_leaf, num_entries, delta);
    }

    CompactedEntriesPtr getCompactedEntries() { return std::make_shared<CompactedEntries>(begin(), end(), num_entries); }

    size_t numEntries() const { return num_entries; }
    size_t numInserts() const { return num_inserts; }
    size_t numDeletes() const { return num_deletes; }

    void addDelete(const UInt64 rid);
    void addInsert(const UInt64 rid, const UInt64 tuple_id);
    void removeInsertsStartFrom(UInt64 tuple_id_start);
    void updateTupleId(const TupleRefs & tuple_refs, size_t offset);
};

#define DT_TEMPLATE template <class ValueSpace, size_t M, size_t F, size_t S, typename Allocator>
#define DT_CLASS DeltaTree<ValueSpace, M, F, S, Allocator>

DT_TEMPLATE
DT_CLASS::DeltaTree(const DT_CLASS::Self & o)
    : height(o.height)
    , num_inserts(o.num_inserts)
    , num_deletes(o.num_deletes)
    , num_entries(o.num_entries)
    , allocator(new Allocator())
    , log(&Poco::Logger::get("DeltaTree"))
{
    NodePtr my_root;
    if (isLeaf(o.root))
        my_root = new (createNode<Leaf>()) Leaf(*as(Leaf, o.root));
    else
        my_root = new (createNode<Intern>()) Intern(*as(Intern, o.root));

    std::queue<NodePtr> nodes;
    nodes.push(my_root);

    LeafPtr first_leaf = nullptr;
    LeafPtr last_leaf = nullptr;
    while (!nodes.empty())
    {
        auto node = nodes.front();
        nodes.pop();

        if (isLeaf(node))
        {
            auto leaf = as(Leaf, node);

            leaf->prev = last_leaf;
            if (last_leaf)
                last_leaf->next = leaf;
            if (!first_leaf)
                first_leaf = leaf;

            last_leaf = leaf;
        }
        else
        {
            auto intern = as(Intern, node);
            if (unlikely(!intern->count))
                throw Exception("Unexpected internal node which count = 0");
            if (isLeaf(intern->children[0]))
            {
                for (size_t i = 0; i < intern->count; ++i)
                {
                    auto child = new (createNode<Leaf>()) Leaf(*as(Leaf, intern->children[i]));
                    nodes.push(child);
                    intern->children[i] = child;

                    child->parent = intern;
                }
            }
            else
            {
                for (size_t i = 0; i < intern->count; ++i)
                {
                    auto child = new (createNode<Intern>()) Intern(*as(Intern, intern->children[i]));
                    nodes.push(child);
                    intern->children[i] = child;

                    child->parent = intern;
                }
            }
        }
    }

    this->root = my_root;
    this->left_leaf = first_leaf;
    this->right_leaf = last_leaf;
}

DT_TEMPLATE
void DT_CLASS::check(NodePtr node, bool recursive) const
{
    if (isLeaf(node))
    {
        LeafPtr p = as(Leaf, node);
        if (p->mark > 1 || ((node != root) && (Leaf::overflow(p->count) || Leaf::underflow(p->count))))
            throw Exception("illegal node");
        InternPtr parent = p->parent;
        if (parent)
        {
            auto pos = parent->searchChild(p);
            if (pos >= parent->count)
                throw Exception("illegal node");
            if (parent->deltas[pos] != p->getDelta())
            {
                throw Exception("illegal node");
            }
            if (pos > 0 && parent->sids[pos - 1] != p->sids[0])
            {
                throw Exception("illegal node");
            }
        }
    }
    else
    {
        InternPtr p = as(Intern, node);
        if (p->mark > 1 || ((node != root) && (Intern::overflow(p->count) || Intern::underflow(p->count))))
            throw Exception("illegal node");

        InternPtr parent = p->parent;
        if (parent)
        {
            auto pos = parent->searchChild(p);
            if (pos >= parent->count)
                throw Exception("illegal node");
            if (parent->deltas[pos] != p->getDelta())
            {
                throw Exception("illegal node");
            }
        }
        if (recursive)
        {
            for (size_t i = 0; i < p->count; ++i)
            {
                check(p->children[i], recursive);
            }
        }
    }
}

DT_TEMPLATE
void DT_CLASS::addDelete(const UInt64 rid)
{
    checkId(rid);

    EntryIterator leaf_end(this->end());
    auto it = findRightLeaf<true>(rid);
    searchLeftId<true>(it, rid);

    bool has_delete = false;
    while (it != leaf_end && it.getRid() == rid && it.isDelete())
    {
        has_delete = true;
        ++it;
    }

    bool has_insert = it != leaf_end && it.getRid() == rid && it.isInsert();
    if (has_insert)
    {
        /// Remove existing insert entry.

        --num_entries;
        --num_inserts;

        auto leaf = it.getLeaf();
        auto pos = it.getPos();
        auto value = it.getValue();

        insert_value_space->removeFromInsert(value);
        leaf->shiftEntries(pos + 1, -1);
        --(leaf->count);
    }
    else if (has_delete)
    {
        /// Simply increase delete count at the last one of delete chain.

        ++num_deletes;

        --it; // <-- Go to last delete entry.

        auto leaf = it.getLeaf();
        auto pos = it.getPos();

        leaf->mutations[pos].setCount(leaf->mutations[pos].count() + 1);
    }
    else
    {
        /// Insert a new delete entry.
        ++num_deletes;
        ++num_entries;

        auto leaf = it.getLeaf();
        auto pos = it.getPos();
        auto delta = it.getDelta();

        leaf->shiftEntries(pos, 1);
        leaf->sids[pos] = checkId(rid - delta);
        leaf->mutations[pos] = DTMutation(/* is_insert */ false, /*count*/ 1, /*value*/ 0);
        ++(leaf->count);
    }

    afterLeafUpdated(it.getLeaf());

    if (unlikely(!isRootOnly() && !it.getLeaf()->legal()))
        throw Exception("Illegal leaf state: " + it.getLeaf()->state());
}

DT_TEMPLATE
void DT_CLASS::addInsert(const UInt64 rid, const UInt64 tuple_id)
{
    checkId(rid);
    checkId(tuple_id);

    EntryIterator leaf_end(this->end());
    auto it = findRightLeaf<true>(rid);
    searchLeftId<true>(it, rid);

    /// Skip DT_DEL entries.
    while (it != leaf_end && it.getRid() == rid && it.isDelete())
    {
        ++it;
    }

    ++num_inserts;
    ++num_entries;

    auto leaf = it.getLeaf();
    auto pos = it.getPos();
    auto delta = it.getDelta();
    auto sid = checkId(rid - delta);

#ifndef NDEBUG
    if (it != leaf_end && sid > it.getSid())
        throw Exception("Unexpected insertion, sid is bigger than current pos");
#endif

    leaf->shiftEntries(pos, 1);
    leaf->sids[pos] = sid;
    leaf->mutations[pos] = DTMutation(/* is_insert */ true, /*count*/ 1, tuple_id);
    ++(leaf->count);

    afterLeafUpdated(leaf);

    if (unlikely(!isRootOnly() && !leaf->legal()))
        throw Exception("Illegal leaf state: " + leaf->state());
}

DT_TEMPLATE
void DT_CLASS::removeInsertsStartFrom(UInt64 tuple_id_start)
{
    std::vector<UInt64> rids;
    for (EntryIterator entry_it(this->begin()), entry_end(this->end()); entry_it != entry_end; ++entry_it)
    {
        if (entry_it.isInsert() && entry_it.getValue() >= tuple_id_start)
            rids.push_back(entry_it.getRid());
    }
    // Must remove the bigger rids first. Because after a rid got removed, the later value of rids changed.
    for (auto it = rids.rbegin(); it != rids.rend(); ++it)
        addDelete(*it);
}

DT_TEMPLATE
void DT_CLASS::updateTupleId(const TupleRefs & tuple_refs, size_t offset)
{
    size_t tuple_id_end = offset + tuple_refs.size();
    for (EntryIterator entry_it(this->begin()), entry_end(this->end()); entry_it != entry_end; ++entry_it)
    {
        auto id = entry_it.getValue();
        if (entry_it.isInsert() && id >= offset && id < tuple_id_end)
            entry_it.setValue(tuple_refs[id - offset] + offset);
    }
}

DT_TEMPLATE
template <bool is_rid, bool is_left>
typename DT_CLASS::EntryIterator DT_CLASS::findLeaf(const UInt64 id) const
{
    NodePtr node = root;
    Int64 delta = 0;
    while (!isLeaf(node))
    {
        InternPtr intern = as(Intern, node);
        size_t i = 0;
        for (; i < intern->count - 1; ++i)
        {
            delta += intern->deltas[i];
            bool ok;
            if constexpr (is_rid)
            {
                if constexpr (is_left)
                {
                    ok = id <= intern->rid(i, delta);
                }
                else
                {
                    ok = id < intern->rid(i, delta);
                }
            }
            else
            {
                if constexpr (is_left)
                {
                    ok = id <= intern->sid(i);
                }
                else
                {
                    ok = id < intern->sid(i);
                }
            }
            if (ok)
            {
                delta -= intern->deltas[i];
                break;
            }
        }
        node = intern->children[i];
    }
    return EntryIterator{as(Leaf, node), 0, delta};
}

DT_TEMPLATE
template <bool is_rid, bool is_left>
void DT_CLASS::searchId(EntryIterator & it, const UInt64 id) const
{
    EntryIterator leaf_end(this->end());
    while (it != leaf_end)
    {
        if constexpr (is_rid)
        {
            if constexpr (is_left)
            {
                if (id <= it.getRid())
                    break;
            }
            else
            {
                if (id < it.getRid())
                    break;
            }
        }
        else
        {
            if constexpr (is_left)
            {
                if (id <= it.getSid())
                    break;
            }
            else
            {
                if (id < it.getSid())
                    break;
            }
        }
        ++it;
    }
}

DT_TEMPLATE
template <class T>
typename DT_CLASS::InterAndSid DT_CLASS::submitMinSid(T * node, UInt64 subtree_min_sid)
{
    if (!node)
        return {};

    auto parent = node->parent;
    if (!parent)
        return {};

    if constexpr (std::is_same<Leaf, T>::value)
        subtree_min_sid = as(Leaf, node)->sids[0];

    auto pos = parent->searchChild(asNode(node));
    if (pos != 0)
    {
        parent->sids[pos - 1] = subtree_min_sid;
        return {};
    }
    else
    {
        return {parent, subtree_min_sid};
    }
}

DT_TEMPLATE
template <class T>
typename DT_CLASS::InternPtr DT_CLASS::afterNodeUpdated(T * node)
{
    if (!node)
        return {};

    constexpr bool is_leaf = std::is_same<Leaf, T>::value;

    if (root == asNode(node) && !isLeaf(root) && node->count == 1)
    {
        /// Decrease tree height.
        root = as(Intern, root)->children[0];

        --(node->count);
        freeNode<T>(node);

        if (isLeaf(root))
            as(Leaf, root)->parent = nullptr;
        else
            as(Intern, root)->parent = nullptr;
        --height;

        LOG_TRACE(log, "height " << DB::toString(height + 1) << " -> " << DB::toString(height));

        return {};
    }

    auto parent = node->parent;
    bool parent_updated = false;

    if (T::overflow(node->count)) // split
    {
        if (!parent)
        {
            /// Increase tree height.
            parent = createNode<Intern>();
            root = asNode(parent);

            parent->deltas[0] = checkDelta(node->getDelta());
            parent->children[0] = asNode(node);
            ++(parent->count);
            parent->refreshChildParent();

            ++height;

            LOG_TRACE(log, "height " << DB::toString(height - 1) << " -> " << DB::toString(height));
        }

        auto pos = parent->searchChild(asNode(node));

        T * next_n = createNode<T>();

        UInt64 sep_sid = node->split(next_n);

        // handle parent update
        parent->shiftEntries(pos + 1, 1);
        // for current node
        parent->deltas[pos] = checkDelta(node->getDelta());
        // for next node
        parent->sids[pos] = sep_sid;
        parent->deltas[pos + 1] = checkDelta(next_n->getDelta());
        parent->children[pos + 1] = asNode(next_n);

        ++(parent->count);

        if constexpr (is_leaf)
        {
            if (as(Leaf, node) == right_leaf)
                right_leaf = as(Leaf, next_n);
        }

        parent_updated = true;

        // LOG_TRACE(log, nodeName(node) << " split");
    }
    else if (T::underflow(node->count) && root != asNode(node)) // adopt or merge
    {
        auto pos = parent->searchChild(asNode(node));

        // currently we always adopt from the right one if possible
        bool is_sibling_left;
        size_t sibling_pos;
        T * sibling;

        if (unlikely(parent->count <= 1))
            throw Exception("Unexpected parent entry count: " + DB::toString(parent->count));

        if (pos == parent->count - 1)
        {
            is_sibling_left = true;
            sibling_pos = pos - 1;
            sibling = as(T, parent->children[sibling_pos]);
        }
        else
        {
            is_sibling_left = false;
            sibling_pos = pos + 1;
            sibling = as(T, parent->children[sibling_pos]);
        }

        if (unlikely(sibling->parent != node->parent))
            throw Exception("parent not the same");

        auto after_adopt = (node->count + sibling->count) / 2;
        if (T::underflow(after_adopt))
        {
            // Do merge.
            // adoption won't work because the sibling doesn't have enough entries.

            node->merge(sibling, is_sibling_left, pos);
            freeNode<T>(sibling);

            pos = std::min(pos, sibling_pos);
            parent->deltas[pos] = checkDelta(node->getDelta());
            parent->children[pos] = asNode(node);
            parent->shiftEntries(pos + 2, -1);

            if constexpr (is_leaf)
            {
                if (is_sibling_left && (as(Leaf, sibling) == left_leaf))
                    left_leaf = as(Leaf, node);
                else if (!is_sibling_left && as(Leaf, sibling) == right_leaf)
                    right_leaf = as(Leaf, node);
            }
            --(parent->count);

            // LOG_TRACE(log, nodeName(node) << " merge");
        }
        else
        {
            // Do adoption.

            auto adopt_count = after_adopt - node->count;
            auto new_sep_sid = node->adopt(sibling, is_sibling_left, adopt_count, pos);

            parent->sids[std::min(pos, sibling_pos)] = new_sep_sid;
            parent->deltas[pos] = checkDelta(node->getDelta());
            parent->deltas[sibling_pos] = checkDelta(sibling->getDelta());

            // LOG_TRACE(log, nodeName(node) << " adoption");
        }

        parent_updated = true;
    }
    else if (parent)
    {
        auto pos = parent->searchChild(asNode(node));
        auto delta = node->getDelta();
        parent_updated = parent->deltas[pos] != delta;
        parent->deltas[pos] = checkDelta(delta);
    }

    if (parent_updated)
        return parent;
    else
        return {};
}

#undef as
#undef asNode
#undef isLeaf
#undef nodeName

#undef DT_TEMPLATE
#undef DT_CLASS

} // namespace DM
} // namespace DB