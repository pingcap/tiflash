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

#include <Common/Exception.h>
#include <common/types.h>

#include <exception>

namespace DB
{
struct ExecutionResult
{
    bool is_success;
    std::exception_ptr exception;

    void verify()
    {
        if (unlikely(!is_success))
            std::rethrow_exception(exception);
    }

    static ExecutionResult success() { return {true, nullptr}; }

    static ExecutionResult fail(const std::exception_ptr & exception)
    {
        RUNTIME_CHECK(exception != nullptr);
        return {false, exception};
    }
};
} // namespace DB
