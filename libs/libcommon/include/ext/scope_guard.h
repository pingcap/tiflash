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

#include <utility>

namespace ext
{
template <class F>
class ScopeGuard
{
    const F function;

public:
    constexpr explicit ScopeGuard(const F & function)
        : function{function}
    {}
    constexpr explicit ScopeGuard(F && function)
        : function{std::move(function)}
    {}
    ~ScopeGuard() { function(); }
};

template <class F>
inline ScopeGuard<F> make_scope_guard(F && function)
{
    return ScopeGuard<F>(std::forward<F>(function));
}

} // namespace ext

#define SCOPE_EXIT_CONCAT(n, ...) const auto scope_exit##n = ext::make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)
