#include <Common/HashTable/HashTable.h>
#include <Common/phmap/phmap.h>

#include <tuple>

template <typename K, typename V>
struct MapSlotWithSavedHashType
{
    // variable name obeys the style of phmap.
    static constexpr bool with_saved_hash = true;
    MapSlotWithSavedHashType()
        : hashval(0)
    {}
    ~MapSlotWithSavedHashType() = delete;
    MapSlotWithSavedHashType(const MapSlotWithSavedHashType &) = delete;
    MapSlotWithSavedHashType & operator=(const MapSlotWithSavedHashType &) = delete;

    template <typename Container>
    size_t getHash(const Container &) const
    {
        return hashval;
    }

    const K & getKey() const { return value.first; }
    V & getMapped() { return value.second; }
    const V & getMapped() const { return value.second; }
    const std::pair<K, V> & getValue() { return value; }
    void setHash(size_t hashval_) { hashval = hashval_; }

    using value_type = std::pair<const K, V>;
    using mutable_value_type = std::pair<K, V>;

    template <typename T>
    static K & getKey(const T & x)
    {
        static_assert(
            std::is_same_v<std::decay_t<T>, value_type> || std::is_same_v<std::decay_t<T>, mutable_value_type>);
        return x.second;
    }

    union
    {
        std::pair<const K, V> value;
        std::pair<K, V> mutable_value;
        K key;
    };
    size_t hashval;
};

template <typename K, typename V>
using MapSlotWithSavedHashPolicy = phmap::priv::map_slot_policy<K, V, MapSlotWithSavedHashType<K, V>>;

template <typename K, typename V>
using FlatHashMapWithSavedHashPolicy = phmap::priv::FlatHashMapPolicy<K, V, MapSlotWithSavedHashPolicy<K, V>>;

template <typename TKey, typename TMapped, typename Hash, typename Base>
class PhHashTableTemplate : public Base
{
public:
    PhHashTableTemplate() { this->reserve(256); }

    static constexpr bool is_phmap = true;
    static constexpr bool is_string_hash_map = false;

    using Self = PhHashTableTemplate;

    using Cell = typename Base::slot_type;
    using cell_type = Cell;
    using Key = typename Base::key_type;
    using mapped_type = TMapped;
    using typename Base::key_type;
    using typename Base::value_type;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    using Base::begin;
    using Base::capacity;
    using Base::clear;
    using Base::empty;
    using Base::end;
    using Base::find_impl;
    using Base::hash;
    using Base::lazy_emplace;
    using Base::lazy_emplace_with_hash;
    using Base::prefetch;
    using Base::prefetch_hash;
    using Base::size;
    using Base::slot_at;

    template <typename KeyHolder>
    ALWAYS_INLINE inline void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        const auto & key = keyHolderGetKey(key_holder);
        const auto hashval = this->hash(key);
        emplaceWithHash(key, key_holder, it, inserted, hashval);
    }

    template <typename KeyHolder>
    ALWAYS_INLINE inline void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hashval)
    {
        const auto & key = keyHolderGetKey(key_holder);
        emplaceWithHash(key, key_holder, it, inserted, hashval);
    }

    ALWAYS_INLINE inline LookupResult find(const TKey & key)
    {
        const auto hashval = this->hash(key);
        return find(key, hashval);
    }

    ALWAYS_INLINE inline ConstLookupResult find(const TKey & key, size_t hashval) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key, hashval);
    }

    ALWAYS_INLINE inline ConstLookupResult find(const TKey & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key);
    }

    ALWAYS_INLINE inline LookupResult find(const TKey & key, size_t hashval)
    {
        size_t offset;
        if (find_impl(key, hashval, offset))
            return slot_at(offset);
        else
            return nullptr;
    }

    template <typename Func>
    ALWAYS_INLINE inline void forEachValue(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            func(iter->first, iter->second);
        }
    }

    template <typename Func>
    ALWAYS_INLINE inline void forEachMapped(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            func(iter->second);
        }
    }

    ALWAYS_INLINE inline typename Base::mapped_type & operator[](const Key & key)
    {
        LookupResult it = nullptr;
        bool inserted = false;
        emplace(key, it, inserted);
        return it->getMapped();
    }

    ALWAYS_INLINE inline size_t getBufferSizeInBytes() const
    {
        return capacity() * (sizeof(typename Base::slot_type) + sizeof(typename phmap::priv::ctrl_t));
    }

    ALWAYS_INLINE inline size_t getBufferSizeInCells() const { return capacity(); }

    ALWAYS_INLINE inline void clearAndShrink() { clear(); }

    void write(DB::WriteBuffer &) const
    {
        // TODO
        // DB::writeBinary(value.first, wb);
        // DB::writeBinary(value.second, wb);
    }

    void writeText(DB::WriteBuffer &) const
    {
        // DB::writeDoubleQuoted(value.first, wb);
        // DB::writeChar(',', wb);
        // DB::writeDoubleQuoted(value.second, wb);
    }

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer &)
    {
        // DB::readBinary(value.first, rb);
        // DB::readBinary(value.second, rb);
    }

    void readText(DB::ReadBuffer &)
    {
        // DB::readDoubleQuoted(value.first, rb);
        // DB::assertChar(',', rb);
        // DB::readDoubleQuoted(value.second, rb);
    }

    void setResizeCallback(const ResizeCallback &)
    {
        // TODO
    }

    template <typename Func>
    ALWAYS_INLINE inline void mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = begin(), end = this->end(); it != end; ++it)
        {
            typename Self::LookupResult res_it = nullptr;
            bool inserted = false;
            that.emplace(it->first, res_it, inserted);
            func(res_it->getMapped(), it->second, inserted);
        }
    }

    template <typename Func>
    ALWAYS_INLINE inline void mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = begin(), end = this->end(); it != end; ++it)
        {
            auto res_it = that.find(it->first);
            if (!res_it)
                func(it->second, it->second, false);
            else
                func(res_it->getMapped(), it->second, true);
        }
    }

private:
    template <typename Key, typename KeyHolder>
    ALWAYS_INLINE inline void emplaceWithHash(
        Key && key,
        KeyHolder && key_holder,
        LookupResult & it,
        bool & inserted,
        size_t hashval)
    {
        inserted = false;
        auto iter = lazy_emplace_with_hash(key, hashval, [&](const auto & ctor) {
            inserted = true;
            // NOTE: remember to persiste key before call ctor, because ArenaKehHolder will change key.data when calling.
            keyHolderPersistKey(key_holder);
            ctor(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
        });
        it = iter.getPtr();
        if (inserted)
            it->setHash(hashval);
        else
            keyHolderDiscardKey(key_holder);
    }
};

template <typename Key, typename Mapped, typename Hash>
using PhHashTableBase = phmap::flat_hash_map<Key, Mapped, Hash>;

template <typename Key, typename Mapped, typename Hash>
using PhHashTableWithSavedHashBase = phmap::priv::raw_hash_map<
    FlatHashMapWithSavedHashPolicy<Key, Mapped>,
    Hash,
    phmap::priv::hash_default_eq<Key>,
    phmap::priv::Allocator<typename FlatHashMapWithSavedHashPolicy<Key, Mapped>::slot_type>>;

template <typename Key, typename Mapped, typename Hash>
using PhHashTable = PhHashTableTemplate<Key, Mapped, Hash, PhHashTableBase<Key, Mapped, Hash>>;

template <typename Key, typename Mapped, typename Hash>
using PhHashTableWithSavedHash
    = PhHashTableTemplate<Key, Mapped, Hash, PhHashTableWithSavedHashBase<Key, Mapped, Hash>>;
