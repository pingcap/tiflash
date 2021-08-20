#pragma once

#include <Common/TiFlashException.h>
#include <fmt/core.h>

#include <cassert>

namespace DB
{

namespace DispatchHelper
{

constexpr size_t invalid_index = std::numeric_limits<size_t>::max();

template <typename T>
struct Type;
template <typename... Ts>
struct Types;

struct InvalidSelector
{
    using Candidates = Types<>;
};

struct InvalidVisitor
{
    using Selectors = Types<InvalidSelector>;
};

namespace Traits
{

// TODO: C++20 concepts are better than traits + `static_assert`s.
// TODO: add tests.

template <typename T>
struct is_types : std::false_type
{
};
template <typename... Ts>
struct is_types<Types<Ts...>> : std::true_type
{
};

template <typename T>
static constexpr bool is_types_v = is_types<T>::value;

template <typename T, typename = void>
struct is_selector_impl : std::false_type
{
};

template <>
struct is_selector_impl<InvalidSelector> : std::true_type
{
};

template <typename T>
struct is_selector_impl<T, std::void_t<typename T::Candidates>>
    : std::bool_constant<is_types_v<typename T::Candidates> && (T::Candidates::size > 0)>
{
};

template <typename T>
struct is_selector : is_selector_impl<T>
{
};

template <typename T>
static constexpr bool is_selector_v = is_selector<T>::value;

template <typename Context, typename T, typename = void>
struct has_select_func_impl : std::false_type
{
};

template <typename Context, typename T>
struct has_select_func_impl<Context, T,
    std::void_t<decltype(static_cast<size_t>(T::select(static_cast<const Context &>(std::declval<Context>()))))>> : std::true_type
{
};

template <typename Context, typename T>
struct has_select_func : has_select_func_impl<Context, T>
{
};

template <typename Context, typename T>
static constexpr bool has_select_func_v = has_select_func<Context, T>::value;

template <typename T, typename = void>
struct is_visitor_impl : std::false_type
{
};

template <>
struct is_visitor_impl<InvalidVisitor> : std::true_type
{
};

template <typename T>
struct is_visitor_impl<T, std::void_t<typename T::Selectors>>
    : std::bool_constant<is_types_v<typename T::Selectors> && (T::Selectors::size > 0) && T::Selectors::template testEach<is_selector>>
{
};

template <typename T>
struct is_visitor : is_visitor_impl<T>
{
};

template <typename T>
static constexpr bool is_visitor_v = is_visitor<T>::value;

} // namespace Traits

template <typename T>
struct Type
{
    static constexpr T unwrap() { return std::declval<T>(); }

    using Candidates = Types<T>;

    template <typename Context>
    static Candidates select(const Context &)
    {
        return Type<T>();
    }
};

template <typename... Ts>
struct Then
{
    static_assert(sizeof...(Ts) > 0, "You should provide at least one selector");
    static_assert((Traits::is_selector_v<Ts> && ...), "Ts... must be selectors");

    using Selectors = Types<Ts...>;
};

template <typename... Ts>
struct Types
{
    static constexpr size_t size = sizeof...(Ts);

    template <template <typename> class Predicate>
    static constexpr bool testEach = (Predicate<Ts>::value && ...);

    template <size_t i>
    using Get = std::tuple_element_t<i, std::tuple<Ts...>>;

    template <typename... NewTs>
    using Append = Types<Ts..., NewTs...>;

    template <template <typename> class Transformation>
    using Transform = Types<typename Transformation<Ts>::type...>;

    const size_t index;

    constexpr Types() : index(invalid_index) {}
    constexpr Types(size_t index_) : index((index_ < size) ? index_ : invalid_index) {}
    constexpr operator size_t() const { return index; }

    template <typename T>
    constexpr Types(Type<T>) : index(find<T>())
    {}

    template <typename Op>
    static constexpr auto getApplyFunc(Type<Op>)
    {
        return Op::template Impl<Ts...>::apply;
    }

    template <typename Predicate>
    static constexpr Types find(const Predicate & predicate)
    {
        static_assert((std::is_invocable_v<Predicate, Type<Ts>> && ...), "Predicate must accept a type variable");

        size_t pos = 0;

        // warning: possible misuse of comma operator here
        bool found = ((predicate(Type<Ts>()) || (static_cast<void>(++pos), false)) || ...);

        if (found)
        {
            assert(pos < size);
            return pos;
        }
        else
            return invalid_index;
    }

    template <typename T>
    static constexpr Types find()
    {
        return find([](auto v) { return std::is_same_v<T, decltype(v.unwrap())>; });
    }

    bool valid() const { return index != invalid_index; }

    template <typename Func>
    Types invokeIf(bool condition, const Func & func) const
    {
        static_assert(std::is_invocable_v<Func>, "Func must be callable");

        if (condition)
        {
            if constexpr (std::is_same_v<decltype(func()), void>)
                func();
            else
                return static_cast<Types>(func());
        }

        return *this;
    }

    template <typename Func>
    Types onValid(const Func & func) const
    {
        return invokeIf(valid(), func);
    }

    template <typename Func>
    Types onInvalid(const Func & func) const
    {
        return invokeIf(!valid(), func);
    }

    template <typename Func>
    auto transform(const Func & func) const
    {
        static_assert(std::is_invocable_v<Func, Types>, "Func must accept an instance of struct Types");

        return func(*this);
    }
};

template <typename Context>
struct ApplyNode
{
    using NodePtr = const ApplyNode *;
    using SelectFunc = size_t (*)(const Context &);
    using ApplyFunc = void (*)(const Context &);

    template <typename Selector>
    constexpr ApplyNode(Type<Selector>, const NodePtr * children_) : children(children_), func(wrappedSelect<Selector>)
    {
        static_assert(Traits::is_selector_v<Selector>, "Selector is not valid");
    }
    constexpr ApplyNode(ApplyFunc apply) : children(nullptr), func(apply) {}

    void apply(const Context & ctx) const
    {
        if (isLeaf())
            func.apply(ctx);
        else
        {
            // out-of-bound is checked in wrappedSelect.
            size_t index = func.select(ctx);
            children[index]->apply(ctx);
        }
    }

private:
    union Func
    {
        SelectFunc select;
        ApplyFunc apply;

        constexpr Func(SelectFunc select_) : select(select_) {}
        constexpr Func(ApplyFunc apply_) : apply(apply_) {}
    };

    const NodePtr * children;
    Func func;

    template <typename Selector>
    static size_t wrappedSelect(const Context & ctx)
    {
        static_assert(Traits::has_select_func_v<Context, Selector>, "Selector does not have select func");

        size_t index = static_cast<size_t>(Selector::select(ctx));
        if (index >= Selector::Candidates::size)
            throw TiFlashException(
                fmt::format("Index out of range: index = {}, size = {}", index, Selector::Candidates::size), Errors::Coprocessor::Internal);

        return index;
    }

    bool isLeaf() const { return children == nullptr; }
};

template <typename Op, typename Tour = Types<>, typename Selection = Types<>>
struct ApplyTree
{
    using Context = typename Op::Context;

    static void apply(const Context & ctx) { root.apply(ctx); }

private:
    template <typename, typename, typename>
    friend struct ApplyTree;

    static_assert(Traits::is_types_v<Tour>, "Tour should be a type list");
    static_assert(Traits::is_types_v<Selection>, "Selection should be a type list");

    using Node = ApplyNode<Context>;
    using NodePtr = const Node *;

    template <typename List, typename... Ts>
    using Append = typename List::template Append<Ts...>;

    template <typename... Ts>
    struct Get
    {
        using Visitor = typename Op::template Visitor<Append<Tour, Ts...>>;

        static_assert(Traits::is_visitor_v<Visitor>, "Visitor is invalid");
    };

    template <typename... Ts>
    using GetSelectors = typename Get<Ts...>::Visitor::Selectors;

    template <typename List, bool ended = (List::size == GetSelectors<>::size)>
    struct Resolve;

    template <typename... Selected>
    struct Resolve<Types<Selected...>, true>
    {
        using Tree = ApplyTree<Op, Append<Tour, Selected...>, Types<>>;
        using Selectors = GetSelectors<Selected...>;
        static_assert(Selectors::size > 0, "No selector available");

        using Selector = typename Selectors::template Get<0>;
    };

    template <typename List>
    struct Resolve<List, false>
    {
        static_assert(Traits::is_types_v<List>, "List should be a type list");

        static constexpr size_t size = List::size;

        using Selectors = GetSelectors<>;
        static_assert(size < Selectors::size, "Size of list is larger than the number of selectors");

        using Tree = ApplyTree<Op, Tour, List>;
        using Selector = typename Selectors::template Get<size>;
    };

    template <typename Selector, typename>
    struct Proxy
    {
        static_assert(Traits::is_selector_v<Selector>, "Selector is not valid");

        template <typename>
        struct Explore;

        template <typename... Candidates>
        struct Explore<Types<Candidates...>>
        {
            static constexpr size_t size = sizeof...(Candidates);
            static_assert(size > 0, "Selector provides no candidate");

            static constexpr NodePtr children[size]{&Resolve<Append<Selection, Candidates>>::Tree::root...};
        };

        using Candidates = typename Selector::Candidates;
        static constexpr Node root{Type<Selector>(), Explore<Candidates>::children};
    };

    template <typename... Selected>
    struct Proxy<InvalidSelector, Types<Selected...>>
    {
        static constexpr Node root{Append<Tour, Selected...>::getApplyFunc(Type<Op>())};
    };

    using Selector = typename Resolve<Selection>::Selector;
    static constexpr Node root = Proxy<Selector, Selection>::root;
};

} // namespace DispatchHelper

#define DISPATCHER_NAMESPACE ::DB::DispatchHelper
#define USING_DISPATCH_HELPER                           \
    template <typename T__>                             \
    using Type = DISPATCHER_NAMESPACE::Type<T__>;       \
    template <typename... Ts__>                         \
    using Types = DISPATCHER_NAMESPACE::Types<Ts__...>; \
    template <typename... Ts__>                         \
    using Then = DISPATCHER_NAMESPACE::Then<Ts__...>

#define CREATE_BINDER_WITH_PREFIX(prefix, member) \
    struct prefix##member                         \
    {                                             \
        static constexpr auto name = #member;     \
        template <typename Context>               \
        static auto value(const Context & ctx)    \
        {                                         \
            return ctx.member;                    \
        }                                         \
        template <typename Context>               \
        static auto & ref(const Context & ctx)    \
        {                                         \
            return ctx.member;                    \
        }                                         \
    }
#define CREATE_BINDER(member) CREATE_BINDER_WITH_PREFIX(bind_, member)

#define CREATE_VISITOR                                    \
    template <typename>                                   \
    struct Visitor : DISPATCHER_NAMESPACE::InvalidVisitor \
    {                                                     \
    }
#define VISITOR_CASE(name, ...) struct name::Visitor<DISPATCHER_NAMESPACE::Types __VA_ARGS__>

} // namespace DB
