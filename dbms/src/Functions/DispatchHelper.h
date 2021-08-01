#pragma once

#include <Common/TiFlashException.h>
#include <fmt/format.h>

#include <typeindex>

namespace DB
{

namespace DispatchHelper
{

namespace impl
{

// ordered type list.
template <typename... Ts>
struct List
{
    static constexpr size_t length = sizeof...(Ts);
};

// ListDestructor extracts the first item of list.
// it does not work for empty list.
template <typename T>
struct ListDestructor;

template <typename T, typename... Ts>
struct ListDestructor<List<T, Ts...>>
{
    using First = T;
    using Remain = List<Ts...>;
};

// EntryHelper is used to determine the type of entry in jump table.
template <typename Signature, typename Lists>
struct EntryHelper;

// specialization for last level jump table.
template <typename Signature>
struct EntryHelper<Signature, List<>>
{
    using type = Signature;
};

template <typename Signature, typename First, typename... Others>
struct EntryHelper<Signature, List<First, Others...>>
{
    using NextType = typename EntryHelper<Signature, List<Others...>>::type;
    using NextRef = std::decay_t<NextType>;

    using type = const NextRef[First::length];
};

// how to write a custom checker:
// 1. create a new struct/class.
//    > struct MyChecker { ... };
// 2. (optional) add a template constexpr static member function `check`, which accepts
//    a fixed number of template arguments. `check` returns true if template arguments
//    are acceptable.
//    > template <typename T>
//    > static constexpr bool check() {
//    >     return sizeof(T) == 4;
//    > }
// 3. (optional) you can add more overloaded `check`s that accept different numbers of
//    template arugments.
//    > template <typename A, typename B>
//    > static constexpr bool check() {
//    >     return sizeof(A) == sizeof(B);
//    > }
//
// how does checker work:
// - jump table is constructed step by step.
//   - already resolved types are placed in `Visited...`.
// - before jump table goes the to next level, it will call `Check::check<Visited...>()`
//   to see whether current entry should be constructed.
//   - if your `Check::check` does not have specialization for `check<Visited...>`,
//     jump table will simply assume you allow the construction of current entry.

// DefaultChecker does no check.
struct DefaultChecker
{
};

// canExistImpl uses SFINAE to detect whether `Check` contains expected specialization
// for static constexpr member function `check`.
// if it does not exist, just return true. Otherwise, call `check` to get the result.

template <typename>
struct Type
{
};

constexpr bool canExistImpl(...) { return true; }

template <typename Check, typename... Ts>
constexpr auto canExistImpl(Check, Type<Ts>...) -> std::bool_constant<Check::template check<Ts...>()>
{
    return {}; // return value depends on return type (std::true_type/std::false_type)
}

// canExist checks whether `Check` allows `Apply<Ts..., ...>` to exist.
template <typename Check, typename... Ts>
constexpr bool canExist()
{
    return canExistImpl(Check(), Type<Ts>()...);
}

// jump table is a multi-level dispatch tree. For example, suppose we have E<A, B>, where A can be
// A1, A2 and B can be B1, B2, then the structure of dispatch tree is:
//
// >                      E<>
// >                       |
// >           +-----------+-----------+             first level jump table (1 table)
// >           |                       |
// >         E<A1>                   E<A2>
// >           |                       |
// >     +-----+-----+           +-----+-----+       second (last) level jump table (2 tables)
// >     |           |           |           |
// > E<A1, B1>   E<A1, B2>   E<A2, B1>   E<A2, B2>
//
// jump table is static and constexpr.
template <template <typename...> class Apply, typename Signature, typename Check, typename Lists, typename... Visited>
struct JumpTable
{
    using Destructor = ListDestructor<Lists>;

    // Proxy destructs the first list and constructs jump table for this level.
    template <typename T>
    struct Proxy;

    template <typename... Ts>
    struct Proxy<List<Ts...>>
    {
        static constexpr size_t length = sizeof...(Ts);

        using Entry = typename EntryHelper<Signature, Lists>::type;

        template <typename T>
        static constexpr auto getNextEntry()
        {
            if constexpr (canExist<Check, Visited..., T>())
                return JumpTable<Apply, Signature, Check, typename Destructor::Remain, Visited..., T>::entry;
            else
                return nullptr;
        }

        static constexpr Entry entry{getNextEntry<Ts>()...};
    };

    static constexpr auto entry = Proxy<typename Destructor::First>::entry;
};

// specialization for last level jump table.
template <template <typename...> class Apply, typename Signature, typename Check, typename... Visited>
struct JumpTable<Apply, Signature, Check, List<>, Visited...>
{
    static constexpr auto entry = Apply<Visited...>::apply;
};

// Index represents integer index.
// it support operator/ with const array.
// > (a / Index(0)) / Index(1) => a[0][1]
//
// it helps Dispatcher::lookup to write fold expressions.
struct Index
{
    static constexpr size_t invalid_index = std::numeric_limits<size_t>::max();

    Index(size_t index_) : index(index_) {}
    operator size_t() const { return index; }

    size_t index;
};

// if array is nullptr or index is Index::invalid_index, returns nullptr.
// NOTE: it does not check array bound.
template <typename T>
auto operator/(const T * array, const Index & index)
{
    std::decay_t<decltype(array[0])> result = nullptr;
    if (array && index != Index::invalid_index)
        result = array[index];
    return result;
}

// base class of TypeVar.
class ITypeVar
{
public:
    virtual ~ITypeVar() = default;

    virtual std::type_index getTypeIndex() const = 0;
    virtual std::string getName() const = 0;
};

using TypeVarPtr = const ITypeVar *;

// TypeVar wraps a concrete type. It is constructable and copyable no matter what type is inside.
// it is used to pass type in arguments, especially useful for template lambdas.
template <typename T>
class TypeVar final : public ITypeVar
{
public:
    using type = T;

    std::type_index getTypeIndex() const override { return std::type_index(typeid(T)); }
    std::string getName() const override { return typeid(T).name(); }

    static TypeVar & instance()
    {
        static TypeVar static_instance;
        return static_instance;
    }
};

template <typename T>
TypeVarPtr getTypeVar()
{
    return &TypeVar<T>::instance();
}

// extract type out of TypeVar.
// > unwrap_t<decltype(typevar)>
template <typename T>
using unwrap_t = typename std::decay_t<T>::type;

// Dispatcher is a wrapper of jump table and provides lookup method.
// Fn is the function pointer of Signature of jump table.
// Entry is the type of the first level jump table's entry.
template <typename Fn, typename Entry, typename... Lists>
class Dispatcher
{
public:
    static constexpr size_t depth = sizeof...(Lists);
    static constexpr auto shape = std::make_tuple(Lists::length...);

    std::vector<TypeVarPtr> createTypeVector() const
    {
        std::vector<TypeVarPtr> vector;
        vector.resize(depth);
        return vector;
    }

    // if no viable function is found, lookup will return nullptr.
    template <typename... Arguments>
    Fn lookup(const Arguments &... arguments) const
    {
        static_assert(sizeof...(Arguments) == depth);
        return lookup(std::make_index_sequence<depth>(), arguments...);
    }

    template <typename Argument>
    Fn lookup(const std::initializer_list<Argument> & arguments) const
    {
        return lookup(std::vector<Argument>(arguments));
    }

    template <typename Argument>
    Fn lookup(const std::vector<Argument> & arguments) const
    {
        if (arguments.size() != depth)
            return nullptr;
        return lookup(std::make_index_sequence<depth>(), arguments);
    }

protected:
    template <typename Signature, typename...>
    friend struct Builder;

    Dispatcher(const Entry & entry_) : entry(entry_) {}

    template <size_t... index, typename... Arguments>
    Fn lookup(std::index_sequence<index...>, const Arguments &... arguments) const
    {
        return (entry / ... / getIndex<index, Lists>(arguments));
    }

    template <typename Argument, size_t... index>
    Fn lookup(std::index_sequence<index...>, const std::vector<Argument> & arguments) const
    {
        return (entry / ... / getIndex<index, Lists>(arguments[index]));
    }

    template <size_t index, typename Current, typename Argument>
    static Index getIndex(const Argument & argument)
    {
        size_t result = 0;

        if constexpr (std::is_same_v<Argument, TypeVarPtr>)
            getIndexInList(argument, result, Current(), std::make_index_sequence<Current::length>());
        else
            result = static_cast<size_t>(argument);

        // out-of-bound check
        if (result >= std::get<index>(shape))
            return Index::invalid_index;
        else
            return result;
    }

    template <typename... Ts, size_t... index>
    static void getIndexInList(TypeVarPtr typevar, size_t & output, List<Ts...>, std::index_sequence<index...>)
    {
        if (!((testTypeEqual<Ts>(typevar, index, output)) || ...))
            output = Index::invalid_index;
    }

    template <typename T>
    static bool testTypeEqual(TypeVarPtr typevar, size_t index, size_t & output)
    {
        if (typevar && typevar->getTypeIndex() == std::type_index(typeid(T)))
        {
            output = index;
            return true;
        }
        return false;
    }

private:
    Entry entry;
};

// Builder incrementally builds the configuration of a Dispatcher.
template <typename Signature = void(), typename... Lists>
class Builder
{
public:
    template <typename... Ts>
    constexpr auto append() const
    {
        return Builder<Signature, Lists..., List<Ts...>>();
    }

    template <typename NewSignature>
    constexpr auto setSignature()
    {
        return Builder<NewSignature, Lists...>();
    }

    template <template <typename...> class Apply, typename Check = DefaultChecker>
    auto buildFor()
    {
        auto entry = JumpTable<Apply, Signature, Check, List<Lists...>>::entry;
        return Dispatcher<std::decay_t<Signature>, decltype(entry), Lists...>{entry};
    }
};

constexpr auto newBuilder() { return Builder<>(); }

// Selector is a helper class to select from a list of types.
// > selectFrom<int *, char *, short *>()
// >     .where([&](auto v) {
// >         return sizeof(std::remove_pointer_t<unwrap_t<decltype(v)>>) == int_size;
// >     })
// >     .onError([] { std::cerr << "???" << std::endl; })
// >     .transform<std::remove_pointer>()
// >     .toTypeVar() => int, char, short or report an error
template <typename Predicate, typename ErrorHandler, template <typename> class Transformer, typename... Ts>
class Selector
{
public:
    Selector() = default;

    template <typename NewPredicate>
    constexpr auto where(const NewPredicate & new_predicate) const
    {
        return Selector<NewPredicate, ErrorHandler, Transformer, Ts...>{new_predicate, on_error};
    }

    template <typename NewErrorHandler>
    constexpr auto onError(const NewErrorHandler & new_handler) const
    {
        return Selector<Predicate, NewErrorHandler, Transformer, Ts...>{predicate, new_handler};
    }

    template <template <typename> class NewTransformer>
    constexpr auto transform() const
    {
        return Selector<Predicate, ErrorHandler, NewTransformer, Ts...>{predicate, on_error};
    }

    TypeVarPtr toTypeVar() const
    {
        TypeVarPtr result = nullptr;

        bool found = ((test<Ts>(result)) || ...);
        if (!found)
            on_error();

        return result;
    }

protected:
    template <typename, typename, template <typename> class, typename...>
    friend class Selector;

    Selector(const Predicate & predicate_, const ErrorHandler & handler_) : predicate(predicate_), on_error(handler_) {}

    template <typename T>
    bool test(TypeVarPtr & output) const
    {
        if (predicate(TypeVar<T>()))
        {
            output = getTypeVar<unwrap_t<Transformer<T>>>();
            return true;
        }

        return false;
    }

private:
    Predicate predicate;
    ErrorHandler on_error;
};

// DefaultPredicate always selects the first type in list.
struct DefaultPredicate
{
    template <typename T>
    constexpr bool operator()(T) const
    {
        return true;
    }
};

struct DefaultErrorHandler
{
    void operator()() const { throw TiFlashException("No type selected by predicate", Errors::Coprocessor::Internal); }
};

// DefaultTransformer is identity transformation.
template <typename T>
struct DefaultTransformer
{
    using type = T;
};

template <typename... Ts>
constexpr auto selectFrom()
{
    return Selector<DefaultPredicate, DefaultErrorHandler, DefaultTransformer, Ts...>{};
}

} // namespace impl

// export public functions.

template <typename T>
constexpr auto getTypeVar = impl::getTypeVar<T>;

template <typename T>
using unwrap_t = impl::unwrap_t<T>;

constexpr auto newBuilder = impl::newBuilder;

template <typename... Ts>
constexpr auto selectFrom = impl::selectFrom<Ts...>;

} // namespace DispatchHelper

} // namespace DB
