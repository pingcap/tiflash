#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Tuple.h>

#include <common/logger_useful.h>

namespace DB
{

struct DTMutation;
struct DTModify;
template <size_t M, size_t F, size_t S>
struct DTLeaf;
template <size_t M, size_t F, size_t S>
struct DTIntern;

extern const size_t INVALID_ID;

using DTModifies    = std::vector<DTModify>;
using DTModifiesPtr = DTModifies *;
static_assert(sizeof(UInt64) >= sizeof(DTModifiesPtr));

#define as(T, p) (reinterpret_cast<T *>(p))
#define asNode(p) (reinterpret_cast<void *>(p))
#define isLeaf(p) (((*reinterpret_cast<size_t *>(p)) & 0x01) != 0)
#define nodeName(p) (isLeaf(p) ? "leaf" : "intern")

/// DTMutation type available values.
static constexpr UInt16 DT_INS           = 65535;
static constexpr UInt16 DT_DEL           = 65534;
static constexpr UInt16 DT_MULTI_MOD     = 65533;
static constexpr UInt16 DT_MAX_COLUMN_ID = 65500;

inline std::string DTTypeString(UInt16 type)
{
    String type_s;
    switch (type)
    {
    case DT_INS:
        type_s = "INS";
        break;
    case DT_DEL:
        type_s = "DEL";
        break;
    case DT_MULTI_MOD:
        type_s = "MMD";
        break;
    default:
        type_s = ::DB::toString(type);
        break;
    }
    return type_s;
}

struct DTMutation
{
    DTMutation() = default;
    DTMutation(UInt16 type_, UInt64 value_) : type(type_), value(value_) {}

    /// DT_INS : Insert
    /// DT_DEL : Delete
    /// DT_MULTI_MOD : modify chain
    /// otherwise, mutation is in MOD mode, "type" is modify columnId.
    UInt16 type = 0;
    /// for DT_INS and MOD, "value" is the value index in value space;
    /// for DT_MULTI_MOD, "value" represents the chain pointer;
    /// for DT_DEL, "value" is the consecutive deleting number, e.g. 5 means 5 tuples deleted from current position.
    UInt64 value = 0;

    inline bool isModify() const { return type != DT_INS && type != DT_DEL; }
};

struct DTModify
{
    DTModify() = default;

    DTModify(size_t column_id_, UInt64 value_) : column_id(column_id_), value(value_) {}

    size_t column_id = 0;
    UInt64 value     = 0;
};


/// Note that we allocate one more slot for entries in DTIntern and DTLeaf, to simplify entry insert operation.

template <size_t M, size_t F, size_t S>
struct DTLeaf
{
    using NodePtr   = void *;
    using Leaf      = DTLeaf<M, F, S>;
    using Intern    = DTIntern<M, F, S>;
    using LeafPtr   = Leaf *;
    using InternPtr = Intern *;

    DTLeaf() = default;

    const size_t mark = 1; // <-- This mark MUST be declared at first place!

    UInt64     sids[M * S + 1];
    DTMutation mutations[M * S + 1];
    size_t     count = 0; // mutations number count

    LeafPtr   prev   = nullptr;
    LeafPtr   next   = nullptr;
    InternPtr parent = nullptr;

    inline UInt64 sid(size_t pos) const { return sids[pos]; }
    inline UInt64 rid(size_t pos, Int64 delta) const { return sids[pos] + delta; }
    inline UInt16 type(size_t pos) const { return mutations[pos].type; }
    inline UInt16 value(size_t pos) const { return mutations[pos].value; }
    inline bool   isModify(size_t pos) const { return mutations[pos].isModify(); }

    static inline bool overflow(size_t count) { return count > M * S; }
    static inline bool underflow(size_t count) { return count < M; }
    inline bool        legal() { return !overflow(count) && !underflow(count); }
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
            if (m.type == DT_INS)
                delta += 1;
            else if (m.type == DT_DEL)
                delta -= m.value;
        }
        return delta;
    }

    /// Search the first pos with equal or greater id.
    /// Returns the mutations count if the id is not found in this leaf.
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
            if (type(i) == DT_INS)
                delta += 1;
            else if (type(i) == DT_DEL)
                delta -= value(i);
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

        right_n->prev   = this;
        right_n->next   = this->next;
        right_n->parent = this->parent;

        if (this->next)
            this->next->prev = right_n;
        this->next = right_n;

        std::move(std::begin(this->sids) + split, std::begin(this->sids) + this->count, std::begin(right_n->sids));
        std::move(std::begin(this->mutations) + split, std::begin(this->mutations) + this->count, std::begin(right_n->mutations));

        right_n->count = this->count - split;
        this->count    = split;

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
                std::begin(sibling->mutations) + sibling_cut, std::begin(sibling->mutations) + sibling->count, std::begin(this->mutations));

            sibling->count -= adopt_count;
            this->count += adopt_count;

            return this->sids[0];
        }
        else
        {
            std::move(std::begin(sibling->sids), std::begin(sibling->sids) + adopt_count, std::begin(this->sids) + this->count);
            std::move(
                std::begin(sibling->mutations), std::begin(sibling->mutations) + adopt_count, std::begin(this->mutations) + this->count);

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
    using NodePtr   = void *;
    using Leaf      = DTLeaf<M, F, S>;
    using Intern    = DTIntern<M, F, S>;
    using LeafPtr   = Leaf *;
    using InternPtr = Intern *;

    DTIntern() = default;

    const size_t mark = 0; // <-- This mark MUST be declared at first place!

    UInt64  sids[F * S + 1];
    Int64   deltas[F * S + 1];
    NodePtr children[F * S + 1];
    size_t  count = 0; // children number count, and sids' is "count - 1"

    InternPtr parent = nullptr;

    inline UInt64 sid(size_t pos) const { return sids[pos]; }
    inline UInt64 rid(size_t pos, Int64 delta) const { return sids[pos] + delta; }

    static inline bool overflow(size_t count) { return count > F * S; }
    static inline bool underflow(size_t count) { return count < F; }
    inline bool        legal() { return !overflow(count) && !underflow(count); }
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
    /// Returns the newly created node, and new separator sid.
    inline UInt64 split(InternPtr right_n)
    {
        size_t split = F * S / 2;

        right_n->parent = this->parent;

        std::move(std::begin(this->sids) + split, std::begin(this->sids) + this->count, std::begin(right_n->sids));
        std::move(std::begin(this->deltas) + split, std::begin(this->deltas) + this->count, std::begin(right_n->deltas));
        std::move(std::begin(this->children) + split, std::begin(this->children) + this->count, std::begin(right_n->children));

        right_n->count = this->count - split;
        this->count    = split;

        this->refreshChildParent();
        right_n->refreshChildParent();

        return this->sids[this->count - 1];
    }

    /// Merge this node and the sibling, node_pos is the position of currently node in parent.
    /// Note that sibling should be deleted outside.
    inline void merge(InternPtr sibling, bool left, size_t node_pos) { adopt(sibling, left, sibling->count, node_pos); }

    /// Adopt one entry from sibling, whether sibling is from left or right are handled in different way.
    /// node_pos is the position of currently node in parent.
    /// Returns new separator sid.
    inline UInt64 adopt(InternPtr sibling, bool left, size_t adopt_count, size_t node_pos)
    {
        if (left)
        {
            this->shiftEntries(0, adopt_count);

            auto sibling_cut = sibling->count - adopt_count;
            // if adopt_count equals to sibling->count, new_sep_sid is meaningless.
            auto new_sep_sid = !sibling_cut ? INVALID_ID : sibling->sids[sibling_cut - 1];

            std::move(std::begin(sibling->sids) + sibling_cut, std::begin(sibling->sids) + sibling->count - 1, std::begin(this->sids));
            std::move(std::begin(sibling->deltas) + sibling_cut, std::begin(sibling->deltas) + sibling->count, std::begin(this->deltas));
            std::move(
                std::begin(sibling->children) + sibling_cut, std::begin(sibling->children) + sibling->count, std::begin(this->children));

            this->sids[adopt_count - 1] = parent->sids[node_pos - 1];

            sibling->count -= adopt_count;
            this->count += adopt_count;

            this->refreshChildParent();

            return new_sep_sid;
        }
        else
        {
            auto new_sep_sid = adopt_count == sibling->count ? INVALID_ID : sibling->sids[adopt_count - 1];

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
    size_t  pos;
    Int64   delta;

public:
    DTEntryIterator(LeafPtr leaf_, size_t pos_, Int64 delta_) : leaf(leaf_), pos(pos_), delta(delta_) {}

    bool operator==(const DTEntryIterator & rhs) const { return leaf == rhs.leaf && pos == rhs.pos; }
    bool operator!=(const DTEntryIterator & rhs) const { return !(*this == rhs); }

    DTEntryIterator & operator++()
    {
        if (leaf->type(pos) == DT_INS)
            delta += 1;
        else if (leaf->type(pos) == DT_DEL)
            delta -= leaf->value(pos);

        if (++pos >= leaf->count && leaf->next)
        {
            leaf = leaf->next;
            pos  = 0;
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
            pos  = leaf->count - 1;
        }

        if (leaf->type(pos) == DT_INS)
            delta -= 1;
        else if (leaf->type(pos) == DT_DEL)
            delta += leaf->value(pos);

        return *this;
    }

    DTMutation getMutation() const { return leaf->mutations[pos]; }
    LeafPtr    getLeaf() const { return leaf; }
    size_t     getPos() const { return pos; }
    Int64      getDelta() const { return delta; }
    UInt16     getType() const { return leaf->mutations[pos].type; }
    UInt64     getValue() const { return leaf->mutations[pos].value; }
    UInt64     getSid() const { return leaf->sids[pos]; }
    UInt64     getRid() const { return leaf->sids[pos] + delta; }
};

template <class ValueSpace, size_t M, size_t F, size_t S, typename Allocator>
class DeltaTree : private Allocator
{
public:
    using NodePtr       = void *;
    using Leaf          = DTLeaf<M, F, S>;
    using Intern        = DTIntern<M, F, S>;
    using LeafPtr       = Leaf *;
    using InternPtr     = Intern *;
    using EntryIterator = DTEntryIterator<M, F, S>;
    using ValueSpacePtr = std::shared_ptr<ValueSpace>;

    static_assert(M >= 2);
    static_assert(F >= 2);
    static_assert(S >= 2);
    /// We rely on the standard layout to determine whether a node is Leaf or Intern.
    static_assert(std::is_standard_layout_v<Leaf>);
    static_assert(std::is_standard_layout_v<Intern>);

private:
    NodePtr root;
    LeafPtr left_leaf, right_leaf;
    size_t  height = 1;

    size_t insert_count = 0;
    size_t delete_count = 0;
    size_t modify_count = 0;

    size_t num_entries = 0;

    Logger * log;

public:
    ValueSpacePtr insert_value_space;
    ValueSpacePtr modify_value_space;

private:
    inline bool isRootOnly() const { return height == 1; }

    void check(NodePtr node) const;

    using LeafAndDeltaPtr = std::pair<LeafPtr, Int64>;
    /// Find right most leaf this id (rid/sid) could exists/insert.
    template <bool isRid>
    LeafAndDeltaPtr findRightLeaf(const UInt64 id) const;

    /// Find left most leaf this id (rid/sid) could exists.
    template <bool isRid>
    LeafAndDeltaPtr findLeftLeaf(const UInt64 id) const;

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
        UInt64    subtree_min_sid       = INVALID_ID;
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

        //#ifndef NDEBUG
        //        checkAll();
        //#endif
    }

    template <typename T>
    void freeNode(T * node)
    {
        Allocator::free(reinterpret_cast<char *>(node), sizeof(T));
    }

    template <typename T>
    T * createNode()
    {
        T * n = reinterpret_cast<T *>(Allocator::alloc(sizeof(T)));
        new (n) T();
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

public:
    DeltaTree(const ValueSpacePtr & insert_value_space_, const ValueSpacePtr & modify_value_space_)
        : log(&Logger::get("DeltaTree")), //
          insert_value_space(insert_value_space_),
          modify_value_space(modify_value_space_)
    {
        root      = createNode<Leaf>();
        left_leaf = right_leaf = as(Leaf, root);

        LOG_TRACE(log, "MM create");
    }


    ~DeltaTree()
    {
        if (isLeaf(root))
            freeTree<Leaf>((LeafPtr)root);
        else
            freeTree<Intern>((InternPtr)root);

        LOG_TRACE(log, "MM free");
    }

    void checkAll() const
    {
        LeafPtr p     = left_leaf;
        size_t  count = 0;
        for (; p != right_leaf; p = p->next)
        {
            count += p->count;
        }
        count += right_leaf->count;
        if (count != num_entries)
            throw Exception("WTF");

        check(root);
    }

    size_t        getHeight() const { return height; }
    EntryIterator begin() const { return EntryIterator(left_leaf, 0, 0); }
    EntryIterator end() const
    {
        Int64 delta = isLeaf(root) ? as(Leaf, root)->getDelta() : as(Intern, root)->getDelta();
        return EntryIterator(right_leaf, right_leaf->count, delta);
    }

    size_t entries() const { return num_entries; }

    void addModify(const UInt64 rid, const UInt16 column_id, const UInt64 new_value_id);
    void addModify(const UInt64 rid, const RefTuple & tuple);
    void addDelete(const UInt64 rid);
    void addInsert(const UInt64 rid, const UInt64 tuple_id);
};

#define DT_TEMPLATE template <class ValueSpace, size_t M, size_t F, size_t S, typename Allocator>
#define DT_CLASS DeltaTree<ValueSpace, M, F, S, Allocator>

DT_TEMPLATE
void DT_CLASS::check(NodePtr node) const
{
    if (isLeaf(node))
    {
        LeafPtr p = as(Leaf, node);
        if (p->mark > 1 || ((node != root) && (Leaf::overflow(p->count) || Leaf::underflow(p->count))))
            throw Exception("WTF");
        InternPtr parent = p->parent;
        if (parent)
        {
            auto pos = parent->searchChild(p);
            if (pos >= parent->count)
                throw Exception("WTF");
            if (parent->deltas[pos] != p->getDelta())
            {
                throw Exception("WTF");
            }
            if (pos > 0 && parent->sids[pos - 1] != p->sids[0])
            {
                throw Exception("WTF");
            }
        }
    }
    else
    {
        InternPtr p = as(Intern, node);
        if (p->mark > 1 || ((node != root) && (Intern::overflow(p->count) || Intern::underflow(p->count))))
            throw Exception("WTF");

        InternPtr parent = p->parent;
        if (parent)
        {
            auto pos = parent->searchChild(p);
            if (pos >= parent->count)
                throw Exception("WTF");
            if (parent->deltas[pos] != p->getDelta())
            {
                throw Exception("WTF");
            }
        }
        for (size_t i = 0; i < p->count; ++i)
        {
            check(p->children[i]);
        }
    }
}

DT_TEMPLATE
void DT_CLASS::addModify(const UInt64 rid, const UInt16 column_id, const UInt64 new_value_id)
{
    addModify(rid, RefTuple(column_id, new_value_id));
}


DT_TEMPLATE
void DT_CLASS::addModify(const UInt64 rid, const RefTuple & tuple)
{
    if (tuple.values.empty())
        return;

    ++modify_count;

    size_t  pos;
    LeafPtr leaf;
    Int64   delta;
    std::tie(leaf, delta) = findRightLeaf<true>(rid);
    std::tie(pos, delta)  = leaf->searchRid(rid, delta);

    bool exists = pos != leaf->count && leaf->rid(pos, delta) == rid;
    if (exists && leaf->type(pos) == DT_DEL)
    {
        /// Skip DT_DEL entries.
        EntryIterator leaf_it(leaf, pos, delta);
        EntryIterator leaf_end(this->end());
        while (leaf_it != leaf_end && leaf_it.getRid() == rid && leaf_it.getType() == DT_DEL)
        {
            ++leaf_it;
        }
        leaf  = leaf_it.getLeaf();
        pos   = leaf_it.getPos();
        delta = leaf_it.getDelta();

        exists = leaf_it != leaf_end && leaf->rid(pos, delta) == rid;
    }

    if (unlikely(!isRootOnly() && !leaf->legal()))
        throw Exception("Illegal leaf state: " + leaf->state());

    auto & modify_values = tuple.values;
    if (!exists)
    {
        /// Either rid does not exists in this leaf, or all entries with same rid are DT_DEL, simply append an new entry.

        ++num_entries;

        leaf->shiftEntries(pos, 1);
        leaf->sids[pos]      = rid - delta;
        leaf->mutations[pos] = DTMutation();
        ++(leaf->count);

        auto & mutation = leaf->mutations[pos];
        if (modify_values.size() == 1)
        {
            mutation.type  = modify_values[0].column;
            mutation.value = modify_values[0].value;
        }
        else
        {
            DTModifiesPtr modifies = new DTModifies(modify_values.size());
            for (size_t i = 0; i < modify_values.size(); ++i)
            {
                (*modifies)[i] = DTModify(modify_values[i].column, modify_values[i].value);
            }
            mutation.type  = DT_MULTI_MOD;
            mutation.value = reinterpret_cast<UInt64>(modifies);
        }
    }
    else
    {
        /// In-place update for DT_INS.
        if (leaf->mutations[pos].type == DT_INS)
        {
            leaf->mutations[pos].value = insert_value_space->withModify(leaf->value(pos), *modify_value_space, tuple);
            // No leaf entry update.
            return;
        }

        /// Deal with existing modifies.
        DTModifiesPtr modifies;
        {
            auto & m = leaf->mutations[pos];
            if (m.type != DT_MULTI_MOD)
            {
                if (modify_values.size() == 1 && m.type == modify_values[0].column)
                {
                    modify_value_space->removeFromModify(m.value, m.type);
                    m.value = modify_values[0].value;
                    // No leaf entry update.
                    return;
                }
                /// Create modify chain and move the value of current entry into.
                modifies = new DTModifies();
                modifies->emplace_back(m.type, m.value);
                m.type  = DT_MULTI_MOD;
                m.value = reinterpret_cast<UInt64>(modifies);
            }
            else
            {
                modifies = reinterpret_cast<DTModifiesPtr>(m.value);
            }
        }

        /// TODO improve algorithm here
        for (const auto & value : modify_values)
        {
            auto it  = modifies->begin();
            auto end = modifies->end();
            for (; it != end; ++it)
            {
                auto & old_modify = *it;
                if (value.column == old_modify.column_id)
                {
                    modify_value_space->removeFromModify(old_modify.value, old_modify.column_id);
                    old_modify.value = value.value;
                    break;
                }
                else if (value.column < old_modify.column_id)
                {
                    modifies->insert(it, DTModify(value.column, value.value));
                    break;
                }
            }
            if (it == end)
                modifies->emplace_back(value.column, value.value);
        }
    }
    afterLeafUpdated(leaf);
}

DT_TEMPLATE
void DT_CLASS::addDelete(const UInt64 rid)
{
    ++delete_count;

    size_t  pos;
    LeafPtr leaf;
    Int64   delta;
    std::tie(leaf, delta) = findLeftLeaf<true>(rid);
    std::tie(pos, delta)  = leaf->searchRid(rid, delta);

    bool   merge = false;
    size_t merge_pos;

    bool exists = pos != leaf->count && leaf->rid(pos, delta) == rid;
    if (exists && leaf->type(pos) == DT_DEL)
    {
        /// Skip DT_DEL entries.
        EntryIterator leaf_it(leaf, pos, delta);
        EntryIterator leaf_end(this->end());
        while (leaf_it != leaf_end && leaf_it.getRid() == rid && leaf_it.getType() == DT_DEL)
        {
            merge     = true;
            merge_pos = leaf_it.getPos();

            ++leaf_it;
        }
        leaf  = leaf_it.getLeaf();
        pos   = leaf_it.getPos();
        delta = leaf_it.getDelta();

        exists = leaf_it != leaf_end && leaf->rid(pos, delta) == rid;
    }

    if (unlikely(!isRootOnly() && !leaf->legal()))
        throw Exception("Illegal leaf state: " + leaf->state());

    if (exists)
    {
        /// Delete existing insert entry.
        if (leaf->mutations[pos].type == DT_INS)
        {
            --num_entries;

            insert_value_space->removeFromInsert(leaf->mutations[pos].value);
            leaf->shiftEntries(pos + 1, -1);
            --(leaf->count);

            afterLeafUpdated(leaf);

            return;
        }
        else
        {
            // Modify
            --num_entries;

            auto & m = leaf->mutations[pos];
            if (m.type == DT_MULTI_MOD)
            {
                DTModifiesPtr modifies = reinterpret_cast<DTModifiesPtr>(m.value);
                for (const auto & md : *modifies)
                    modify_value_space->removeFromModify(md.value, md.column_id);
                delete modifies;
            }
            else
            {
                modify_value_space->removeFromModify(m.value, m.type);
            }

            leaf->shiftEntries(pos + 1, -1);
            --(leaf->count);
        }
    }

    if (merge)
    {
        /// Simply increase delete count at the last one of delete chain.
        ++(leaf->mutations[merge_pos].value);
    }
    else
    {
        ++num_entries;

        leaf->shiftEntries(pos, 1);
        leaf->sids[pos]      = rid - delta;
        leaf->mutations[pos] = DTMutation(DT_DEL, 1);
        ++(leaf->count);
    }

    afterLeafUpdated(leaf);
}

DT_TEMPLATE
void DT_CLASS::addInsert(const UInt64 rid, const UInt64 tuple_id)
{
    ++insert_count;

    size_t  pos;
    LeafPtr leaf;
    Int64   delta;
    std::tie(leaf, delta) = findRightLeaf<true>(rid);
    std::tie(pos, delta)  = leaf->searchRid(rid, delta);

    bool exists = pos != leaf->count && leaf->rid(pos, delta) == rid;
    if (exists && leaf->type(pos) == DT_DEL)
    {
        /// Skip DT_DEL entries.
        EntryIterator leaf_it(leaf, pos, delta);
        EntryIterator leaf_end(this->end());
        while (leaf_it != leaf_end && leaf_it.getRid() == rid && leaf_it.getType() == DT_DEL)
        {
            ++leaf_it;
        }
        leaf  = leaf_it.getLeaf();
        pos   = leaf_it.getPos();
        delta = leaf_it.getDelta();
    }

    if (unlikely(!isRootOnly() && !leaf->legal()))
        throw Exception("Illegal leaf state: " + leaf->state());

    ++num_entries;

    leaf->shiftEntries(pos, 1);
    leaf->sids[pos]            = rid - delta;
    leaf->mutations[pos].type  = DT_INS;
    leaf->mutations[pos].value = tuple_id;
    ++(leaf->count);

    afterLeafUpdated(leaf);
}

DT_TEMPLATE
template <bool isRid>
typename DT_CLASS::LeafAndDeltaPtr DT_CLASS::findRightLeaf(const UInt64 id) const
{
    NodePtr node  = root;
    Int64   delta = 0;
    while (!isLeaf(node))
    {
        InternPtr intern = as(Intern, node);
        size_t    i      = 0;
        for (; i < intern->count - 1; ++i)
        {
            delta += intern->deltas[i];
            if constexpr (isRid)
            {
                if (id < (intern->rid(i, delta)))
                {
                    delta -= intern->deltas[i];
                    break;
                }
            }
            else
            {
                if (id < intern->sid(i))
                {
                    delta -= intern->deltas[i];
                    break;
                }
            }
        }
        node = intern->children[i];
    }
    return {as(Leaf, node), delta};
}

DT_TEMPLATE
template <bool isRid>
typename DT_CLASS::LeafAndDeltaPtr DT_CLASS::findLeftLeaf(const UInt64 id) const
{
    NodePtr node      = root;
    Int64   delta     = 0;
    bool    checkLeaf = false;
    while (!isLeaf(node))
    {
        InternPtr intern = as(Intern, node);
        size_t    i      = 0;
        for (; i < intern->count - 1; ++i)
        {
            delta += intern->deltas[i];
            bool less;
            bool equal;
            if constexpr (isRid)
            {
                less  = id < (intern->rid(i, delta));
                equal = id == (intern->rid(i, delta));
            }
            else
            {
                less  = id < intern->sid(i);
                equal = id == intern->sid(i);
            }
            if (less || equal)
            {
                delta -= intern->deltas[i];
                if (equal)
                {
                    checkLeaf = true;
                }
                break;
            }
        }
        node = intern->children[i];
    }
    LeafPtr leaf = as(Leaf, node);
    if (checkLeaf)
    {
        // We can't simply call leaf->exists<isRid>(id, delta) because of bug (likely) of C++ compliler, both Clang(6.0.0) and GCC.
        // They think '<' is an operator and complain like:
        //  invalid operands of types '<unresolved overloaded function type>' and 'bool' to binary 'operator<'

        bool exists;
        if constexpr (isRid)
            exists = leaf->existsRid(id, delta);
        else
            exists = leaf->existsSid(id);
        if (!exists)
        {
            delta += leaf->getDelta();
            leaf = leaf->next;
        }
    }
    return {leaf, delta};
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

        LOG_TRACE(log, "height " + toString(height + 1) + " -> " + toString(height));

        return {};
    }

    auto parent         = node->parent;
    bool parent_updated = false;

    if (T::overflow(node->count)) // split
    {
        if (!parent)
        {
            /// Increase tree height.
            parent = createNode<Intern>();
            root   = asNode(parent);

            parent->deltas[0]   = node->getDelta();
            parent->children[0] = asNode(node);
            ++(parent->count);
            parent->refreshChildParent();

            ++height;

            LOG_TRACE(log, "height " + toString(height - 1) + " -> " + toString(height));
        }

        auto pos = parent->searchChild(asNode(node));

        T * next_n = createNode<T>();

        UInt64 sep_sid = node->split(next_n);

        // handle parent update
        parent->shiftEntries(pos + 1, 1);
        // for current node
        parent->deltas[pos] = node->getDelta();
        // for next node
        parent->sids[pos]         = sep_sid;
        parent->deltas[pos + 1]   = next_n->getDelta();
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
        bool   is_sibling_left;
        size_t sibling_pos;
        T *    sibling;

        if (unlikely(parent->count <= 1))
            throw Exception("Unexpected parent entry count: " + toString(parent->count));

        if (pos == parent->count - 1)
        {
            is_sibling_left = true;
            sibling_pos     = pos - 1;
            sibling         = as(T, parent->children[sibling_pos]);
        }
        else
        {
            is_sibling_left = false;
            sibling_pos     = pos + 1;
            sibling         = as(T, parent->children[sibling_pos]);
        }

        auto after_adopt = (node->count + sibling->count) / 2;
        if (T::underflow(after_adopt))
        {
            // Do merge.
            // adoption won't work because the sibling doesn't have enough entries.

            node->merge(sibling, is_sibling_left, pos);
            freeNode<T>(sibling);

            pos                   = std::min(pos, sibling_pos);
            parent->deltas[pos]   = node->getDelta();
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
            parent->deltas[pos]                      = node->getDelta();
            parent->deltas[sibling_pos]              = sibling->getDelta();

            // LOG_TRACE(log, nodeName(node) << " adoption");
        }

        parent_updated = true;
    }
    else if (parent)
    {
        auto pos            = parent->searchChild(asNode(node));
        auto delta          = node->getDelta();
        parent_updated      = parent->deltas[pos] != delta;
        parent->deltas[pos] = delta;
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

} // namespace DB