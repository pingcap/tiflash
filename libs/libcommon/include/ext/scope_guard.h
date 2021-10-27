#pragma once

#include <utility>

namespace ext
{

template <class F> class ScopeGuard {
    const F function;

public:
    constexpr explicit ScopeGuard(const F & function) : function{function} {}
    constexpr explicit ScopeGuard(F && function) : function{std::move(function)} {}
    ~ScopeGuard() { function(); }
};

template <class F>
inline ScopeGuard<F> make_scope_guard(F && function) { return ScopeGuard<F>(std::forward<F>(function)); }

}

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = ext::make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

