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

#include <functional>
#include <type_traits>
#include <utility>


template <typename T, typename Tag>
struct StrongTypedef
{
private:
    using Self = StrongTypedef;
    T t;

public:
    using UnderlyingType = T;
    template <class Enable = typename std::is_copy_constructible<T>::type>
    explicit StrongTypedef(const T & t_)
        : t(t_)
    {}
    template <class Enable = typename std::is_move_constructible<T>::type>
    explicit StrongTypedef(T && t_)
        : t(std::move(t_))
    {}

    template <class Enable = typename std::is_default_constructible<T>::type>
    StrongTypedef()
        : t()
    {}

    StrongTypedef(const Self &) = default;
    StrongTypedef(Self &&) = default;

    Self & operator=(const Self &) = default;
    Self & operator=(Self &&) = default;

    template <class Enable = typename std::is_copy_assignable<T>::type>
    Self & operator=(const T & rhs)
    {
        t = rhs;
        return *this;
    }

    template <class Enable = typename std::is_move_assignable<T>::type>
    Self & operator=(T && rhs)
    {
        t = std::move(rhs);
        return *this;
    }

    operator const T &() const { return t; }
    operator T &() { return t; }

    bool operator==(const Self & rhs) const { return t == rhs.t; }
    bool operator<(const Self & rhs) const { return t < rhs.t; }
    bool operator>(const Self & rhs) const { return t > rhs.t; }
    bool operator<=(const Self & rhs) const { return !(*this > rhs); }

    T & toUnderType() { return t; }
    const T & toUnderType() const { return t; }
};


namespace std
{
template <typename T, typename Tag>
struct hash<StrongTypedef<T, Tag>>
{
    size_t operator()(const StrongTypedef<T, Tag> & x) const { return std::hash<T>()(x.toUnderType()); }
};
} // namespace std

#define STRONG_TYPEDEF(T, D) \
    struct D##Tag            \
    {                        \
    };                       \
    using D = StrongTypedef<T, D##Tag>;
