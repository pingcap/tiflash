// Copyright 2024 PingCAP, Inc.
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

#include <Common/Exception.h>

#include <memory>
#include <not_null.hpp>

namespace DB
{

template <typename T>
using NotNull = cpp::bitwizeshift::not_null<T>;

// It involves an extra check when constructing,
// if we can't tell if `ptr` is nullptr at compile time.
template <class T>
auto newNotNull(T && ptr)
{
    return cpp::bitwizeshift::check_not_null(std::move(ptr));
}

template <class T, class... Args>
auto makeNotNullShared(Args &&... args)
{
    return cpp::bitwizeshift::assume_not_null(std::make_shared<T>(std::forward<Args>(args)...));
}

template <class T, class... Args>
auto makeNotNullUnique(Args &&... args)
{
    return cpp::bitwizeshift::assume_not_null(std::make_unique<T>(std::forward<Args>(args)...));
}

template <typename T>
using NotNullShared = cpp::bitwizeshift::not_null<std::shared_ptr<T>>;

template <typename T>
using NotNullUnique = cpp::bitwizeshift::not_null<std::unique_ptr<T>>;

}; // namespace DB