#pragma once

#include <Common/TiFlashException.h>
#include <Core/Defines.h>

/**
 * the problem: many function implementations need to call into template classes according to
 * IDataType, column types, etc., which are only given at runtime. As a result, a very deep
 * and large conditional if structure is required to find out (dispatch to) the correct template
 * specialization.
 *
 * for example:
 *
 * > if (a.IsColumnConst())
 * >     if (b.IsColumnConst())
 * >         { ... }
 * >     else
 * >         { ... }
 * > else
 * >     if (b.IsColumnConst())
 * >         { ... }
 * >     else
 * >         { ... }
 *
 * it causes a lot of duplication.
 *
 * the attempt: here we present a builder that incrementally builds such recursive structures
 * as shown above. We leave a direct example here. For details, please refer to comments in the
 * namespace DB::DispatchHelper.
 *
 * example code:
 *
 * > template <typename ... Ts>
 * > struct Print {
 * >     static void apply(int number) {
 * >         std::cout << __PRETTY_FUNCTION__ << std::endl;
 * >         std::cout << "number = " << number << std::endl;
 * >     }
 * > };
 * >
 * > std::cin >> int_size;
 * >
 * > using DispatchHelper::Unwrap;
 * >
 * > DispatchHelper::New()
 * >     .Select<char>()
 * >     .SelectFrom<int, unsigned>().Where([](auto v) {
 * >         return std::is_unsigned_v<Unwrap<decltype(v)>>;
 * >     })
 * >     .SelectFrom<short, int>().Where([&](auto v) {
 * >         return sizeof(Unwrap<decltype(v)>) == int_size;
 * >     }).OnError([] { std::cerr << "woo~~~" << std::endl; })
 * >     .Build()
 * >     .Apply<Print>(233);
 *
 * equivalent C++ code:
 *
 * > if (std::is_unsigned_v<int>) {
 * >     if (sizeof(short) == int_size)
 * >         Print<char, int, short>::apply(233);
 * >     else if (sizeof(int) == int_size)
 * >         Print<char, int, int>::apply(233);
 * >     else
 * >         std::cerr << "woo~~~" << std::endl;
 * > } else if (std::is_unsigned_v<unsigned>) {
 * >     if (sizeof(short) == int_size)
 * >         Print<char, unsigned, short>::apply(233);
 * >     else if (sizeof(int) == int_size)
 * >         Print<char, unsigned, int>::apply(233);
 * >     else
 * >         std::cerr << "woo~~~" << std::endl;
 * > } else
 * >     throw Exception("...");
 *
 * note that selection between int and unsigned is constexpr, and hopefully it will be optimized
 * by compilers.
 *
 * example stdin:
 *
 * > 4
 *
 * example stdout:
 *
 * > static void Print<char, unsigned int, int>::apply(int) [Ts = <char, unsigned int, int>]
 * > number = 233
 *
 * performance: we expect most dispatch plans can be fully inlined in Release mode.
 * we should be careful about builder's implementation. It's easy to introduce temporary
 * object construction/destruction, complex recursive calls, etc., which may hinder
 * function inlining.
 *
 * here's a demo that validates DispatchHelper can generate compact machine code on GCC 7.5:
 * <https://gcc.godbolt.org/z/9dc76PW3e>
 *
 * TODO:
 * - currently assume all conditional statements are independent to each other.
 *   - all `Where` clauses do not know about others' resolution.
 *   - we can pass already resolved type list, as a std::tuple, to predicates.
 *     - not implemented yet.
 */

namespace DB
{

namespace DispatchHelper
{

/**
 * key points:
 * - ApplyFn: template <typename A, typename B, ...> struct ApplyFn
 *   - e.g. ModuloImpl<A, B>
 *   - the goal of DispatchHelper is to determine A, B, ... at runtime.
 * - type list: List<Ts...>
 *   - considering A, A may be chosen from some candidate types. These types
 *     form a type list.
 *   - list implies types are considered in order.
 * - draft/plan: a series of type lists. Each list comes with a predicate
 *   and an error handler.
 *   - auto a = DispathHelper::New(); auto b = a.Build()
 *     - `a` is a draft, and `b` is a plan.
 *     - technically they are of the same type.
 *     - at `Build`, draft is transferred to an executor, and becomes a plan.
 *   - predicate is a lambda.
 *   - predicate accepts a type and determines whether this type is selected.
 *   - if no type is selected by predicate, error handler will be called.
 *     - dispatching does not backtrack. Once failed, the entire plan dispatching
 *       also fails.
 * - executor: accepts ApplyFn.
 *   - execute plan to get A, B, ...
 *   - if no error occurs:
 *     - ApplyFn<A, B, ...>::apply will be called.
 */

constexpr size_t InvalidIndex = std::numeric_limits<size_t>::max();

// we wish to use lambdas in `Where` clauses, but C++17 does not have fully
// templated lambdas. All lambda template arguments should be inferrable
// from lambda arguments. Therefore, we pass type wrapped in TypeVar, so that
// lambdas like `[] (auto v) {}` can receive it in v, and use Unwrap<decltype(v)>
// to get the corresponding type.
//
// it also carries the index in the type list. Index starts from zero.
template <typename T>
struct TypeVar
{
    using Type = T;

    const size_t index = InvalidIndex;
};

template <typename T>
using Unwrap = typename T::Type;

// a dummy struct composes some types.
// it is different from std::tuple that construct a type list does not constructor any Ts object.
template <typename... Ts>
struct List
{
    static constexpr size_t Size = sizeof...(Ts);
};

using EmptyList = List<>;

template <typename T>
struct IsList
{
    static constexpr bool Value = false;
};

template <typename... Ts>
struct IsList<List<Ts...>>
{
    static constexpr bool Value = true;
};

// default predicate always selects the first type in list.
struct DefaultPredicate
{
    template <typename T>
    constexpr bool operator()(T) const
    {
        return true;
    }
};

// indexer returns an integer index (std::size_t) instead of true/false.
// ByIndex wraps an indexer into a predicate, so the indexer can be passed to Where.
// > .Where(ByIndex([&](size_t i) { return i; }, index))
template <typename Indexer, typename... Args>
inline constexpr auto ByIndex(const Indexer & index, Args &&... args)
{
    return [&](auto typevar) { return typevar.index == static_cast<size_t>(index(std::forward<Args>(args)...)); };
}

// error handler will be called when predicate failed to select a type.
// it usually throws an exception to inform callers.
struct DefaultErrorHandler
{
    void operator()() const { throw TiFlashException("DispathHelper: unexpected case", Errors::Coprocessor::Internal); }
};

// Pack binds a predicate and an error handler to a type list.
// to avoid name conflicts, we will call it "Item" in Draft.
//
// as a special case, if T is EmptyList, it can still binds a predicate, which will
// be used as prune condition. See `Draft.PruneIf`.
template <typename T, typename P = DefaultPredicate, typename E = DefaultErrorHandler>
struct Pack
{
    using TypeList = std::conditional_t<IsList<T>::Value, T, List<T>>;
    using Predicate = P;
    using ErrorHandler = E;

    Predicate predicate;
    ErrorHandler on_error;
};

// forward declarations.
template <typename Plan, typename... Ts>
class Executor;
template <typename... Items>
class Draft;

using EmptyDraft = Draft<>;

// Draft is the builder of itself.
template <>
class Draft<>
{
public:
    // see Draft<Items...> for explanations of Select/SelectFrom.

    template <typename T>
    constexpr auto Select() const
    {
        return Draft<Pack<T>>(*this);
    }

    template <typename... Ts>
    constexpr auto SelectFrom() const
    {
        static_assert(sizeof...(Ts) > 1);

        return Select<List<Ts...>>();
    }

    // NOTE: we actually disallow empty plan. It's useless.
};

// internally, Draft is a singly linked list.
template <typename... Items>
class Draft
{
protected:
    // DraftHelper can extract the first pack (item).
    template <typename Item, typename... Others>
    struct DraftHelper
    {
        using ItemType = Item;

        using TypeList = typename Item::TypeList;
        using Predicate = typename Item::Predicate;
        using ErrorHandler = typename Item::ErrorHandler;

        using Next = Draft<Others...>;

        template <typename NewPredicate>
        using ReplacePredicate = Draft<Pack<TypeList, NewPredicate, ErrorHandler>, Others...>;

        template <typename NewErrorHandler>
        using ReplaceErrorHandler = Draft<Pack<TypeList, Predicate, NewErrorHandler>, Others...>;
    };

    using Helper = DraftHelper<Items...>;

public:
    // Select directly places a new type onto ApplyFn's template arguments.
    // > .Select<A>().Select<B>().Select<C>() => ApplyFn<A, B, C>
    template <typename T>
    constexpr auto Select() const
    {
        return Draft<Pack<T>, Items...>(*this);
    }

    // SelectFrom creates a new type list with default predicate and error handler.
    // we use a different name from Select to make plan construction more readable.
    // > .SelectFrom<A, B, C>().Where(...)
    template <typename... Ts>
    constexpr auto SelectFrom() const
    {
        static_assert(sizeof...(Ts) > 1);

        return Select<List<Ts...>>();
    }

    // replace default predicate with new_predicate.
    template <typename NewPredicate>
    constexpr auto Where(const NewPredicate & new_predicate) const
    {
        // if there's only one type in list, predicate is usually not needed.
        static_assert(Helper::TypeList::Size > 1);
        // prevent .Where(...).Where(...)
        static_assert(std::is_same_v<typename Helper::Predicate, DefaultPredicate>);

        return typename Helper::template ReplacePredicate<NewPredicate>({new_predicate, item.on_error}, next);
    }

    // replace original error handler with new_handler.
    template <typename NewErrorHandler>
    constexpr auto OnError(const NewErrorHandler & new_handler) const
    {
        // prevent .OnError(...).OnError(...)
        static_assert(std::is_same_v<typename Helper::ErrorHandler, DefaultErrorHandler>);

        return typename Helper::template ReplaceErrorHandler<NewErrorHandler>({item.predicate, new_handler}, next);
    }

    // PruneIf decides whether current branch can be pruned out.
    // during execution, all resolved Ts will be passed `can_prune`, in TypeVar, as arguments.
    // you should guarantee that pruned branches are never reached. Otherwise an internal exception
    // will be thrown.
    //
    // if `can_prune` can be inlined in Release mode and is constexpr, hopefully useless branches
    // will be removed by compilers. this is useful to reduce compiled binary size.
    //
    // the following plan will never dispatch to any ApplyFn, and will be optimized out in
    // Release mode (which only reports an error at runtime):
    // > .Select<int>().Select<int>().PruneIf([](auto u, auto v) {
    // >     return std::is_same_v<Unwrap<decltype(u)>, Unwrap<decltype(v)>>;
    // > })
    //
    // PruneIf will push a new item at the front, and therefore can be followed by OnError.
    template <typename PruneCondition>
    constexpr auto PruneIf(const PruneCondition & can_prune) const
    {
        return Draft<Pack<EmptyList, PruneCondition>, Items...>(can_prune, *this);
    }

    // build the plan.
    // during construction, draft records the reversed order of A, B, ...
    // therefore, we have to reverse it back here.
    //
    // the original Draft is reversed because Select/SelectFrom push new item at the front:
    // > .Select<A>().Select<B>().Select<C>() => Draft<C, B, A>
    constexpr auto Build() const { return Executor(this->Reverse(EmptyDraft())); }

protected:
    template <typename...>
    friend class Draft;
    template <typename, typename...>
    friend class Executor;

    using TypeList = typename Helper::TypeList;
    static constexpr size_t ListSize = Helper::TypeList::Size;

    using NextType = typename Helper::Next;

    constexpr Draft(const typename Helper::Next & _next) : next(_next) {}
    constexpr Draft(const typename Helper::Predicate & _predicate, const typename Helper::Next & _next)
        : item({_predicate, DefaultErrorHandler()}), next(_next)
    {}
    constexpr Draft(const typename Helper::ItemType & _item, const typename Helper::Next & _next) : item(_item), next(_next) {}

    // reverse singly linked list.
    //
    // i.e., turn
    // > Draft<C, B, A> -> Draft<B, A> -> Draft<A> -> Draft<>
    // into
    // > Draft<> <- Draft<C> <- Draft<B, C> <- Draft<A, B, C>
    template <typename... Visited>
    constexpr auto Reverse(const Draft<Visited...> & prev) const
    {
        using Item = typename Helper::ItemType;

        auto current = Draft<Item, Visited...>(item, prev);

        if constexpr (std::is_same_v<NextType, EmptyDraft>)
            return current;
        else
            return next.template Reverse<Item, Visited...>(current);
    }

    // invoke predicate on type T.
    // T will be converted into a TypeVar and passed as the first argument.
    template <typename T>
    constexpr bool Test(size_t index = InvalidIndex) const
    {
        return item.predicate(TypeVar<T>{index});
    }

    // treat predicate as prune condition and call it.
    template <typename... Ts>
    constexpr bool CanPrune() const
    {
        return item.predicate(TypeVar<Ts>{}...);
    }

    // invoke error handler.
    void Error() const { item.on_error(); }

    constexpr auto GetNext() const { return next; }

private:
    const typename Helper::ItemType item;
    const typename Helper::Next next;
};

// Executor stores a plan, and can be executed multiple times.
// TODO: we can't pass extra arguments to predicates, which limits the reusability of executors.
template <typename Plan, typename... Ts>
class Executor
{
public:
    // Apply returns true if no error occurred.
    // if any error has occurred, but no one throws an exception, it returns false.
    //
    // NO_INLINE: prevent Apply being inlined on every call. ApplyImpl will be inlined anyway.
    template <template <typename...> class ApplyFn, typename... Args>
    void NO_INLINE Apply(Args &&... args) const
    {
        ApplyImpl<ApplyFn>(std::forward<Args>(args)...);
    }

protected:
    template <typename...>
    friend class Draft;
    template <typename, typename...>
    friend class Executor;

    constexpr Executor(const Plan & _plan) : plan(_plan) {}

    template <template <typename...> class ApplyFn, typename... Args>
    void ApplyImpl(Args &&... args) const
    {
        if constexpr (std::is_same_v<Plan, EmptyDraft>)
        {
            // if all template arguments have been determined, it's ready to call ApplyFn.
            ApplyFn<Ts...>::apply(std::forward<Args>(args)...);
        }
        else if constexpr (std::is_same_v<typename Plan::TypeList, EmptyList>)
        {
            // if there's no candidate, we use predicate as a prune condition.
            // constexpr: see below.
            /*constexpr*/ bool pruned = plan.template CanPrune<Ts...>();

            // sadly this is not constexpr if, since the plan may contain lambdas that capture non-constexpr objects,
            // which makes the entire plan not constexpr. Since plan.CanPrune references plan (by `this` pointer),
            // `pruned` is not constexpr as well.
            //
            // TODO: make it constexpr to speed up compilation.
            if /*constexpr*/ (pruned)
            {
                // this branch has been pruned, so we should not reach here.
                plan.Error();
            }
            else
            {
                // if current branch has not been pruned, just continue applying.
                // note that there's no new type pushed at the back of Ts.
                auto next_executor = Executor<typename Plan::NextType, Ts...>(plan.GetNext());
                next_executor.template ApplyImpl<ApplyFn>(std::forward<Args>(args)...);
            }
        }
        else
        {
            if (!CheckAndApply<ApplyFn>(
                    typename Plan::TypeList(), std::make_index_sequence<Plan::ListSize>(), plan, std::forward<Args>(args)...))
                plan.Error();
        }
    }

    // the first argument is meant to extract candidate types from type list.
    // the second argument is used to get indexes in the type list.
    template <template <typename...> class ApplyFn, typename... Candidates, size_t... Indexes, typename... Args>
    constexpr bool CheckAndApply(
        const List<Candidates...> &, const std::integer_sequence<size_t, Indexes...> &, const Plan & plan, Args &&... args) const
    {
        static_assert(sizeof...(Candidates) > 0);

        // unary fold expression `(E op ...)` expands to `(E(1) op (... op (E(n-1) op E(n))))`.
        return ((CheckAndApplyImpl<ApplyFn, Candidates>()(plan, Indexes, std::forward<Args>(args)...)) || ...);
    }

    template <template <typename...> class ApplyFn, typename T>
    struct CheckAndApplyImpl
    {
        template <typename... Args>
        constexpr bool operator()(const Plan & plan, size_t index, Args &&... args) const
        {
            if (plan.template Test<T>(index))
            {
                // if the test succeeds, push T at the back of Ts, and execute the next plan.
                auto next_executor = Executor<typename Plan::NextType, Ts..., T>(plan.GetNext());
                next_executor.template ApplyImpl<ApplyFn>(std::forward<Args>(args)...);
                return true;
            }
            else
                return false;
        }
    };

private:
    const Plan plan;
};

// create a new empty draft.
inline constexpr auto New() { return EmptyDraft(); }

} // namespace DispatchHelper

} // namespace DB
