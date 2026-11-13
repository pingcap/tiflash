// Copyright 2026 PingCAP, Inc.
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

#include <atomic>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}
// Separate extension ABI: do not change the existing proxy helper layout.
// Rust owns every token/scope and clones tokens for its background workers.
struct ColumnarCancellationInterfaces
{
    uint32_t version;
    uint32_t size;
    void * (*create)();
    void (*cancel)(void *);
    void (*destroy)(void *);
    void * (*enter)(void *);
    void (*leave)(void *);
    void * (*child)(void *);
};

extern "C" const ColumnarCancellationInterfaces * tiflash_columnar_get_cancellation_interfaces();

class ColumnarCancellation
{
public:
    ColumnarCancellation()
    {
        interfaces = tiflash_columnar_get_cancellation_interfaces();
        RUNTIME_CHECK(interfaces && interfaces->version == 1 && interfaces->size >= sizeof(*interfaces));
        token = interfaces->create();
    }
    explicit ColumnarCancellation(std::shared_ptr<ColumnarCancellation> parent_)
        : parent(std::move(parent_))
        , interfaces(parent->interfaces)
        , token(interfaces->child(parent->token))
    {}
    ~ColumnarCancellation() { interfaces->destroy(token); }
    ColumnarCancellation(const ColumnarCancellation &) = delete;
    ColumnarCancellation & operator=(const ColumnarCancellation &) = delete;

    void cancel()
    {
        cancelled.store(true, std::memory_order_release);
        interfaces->cancel(token);
    }
    bool isCancelled() const { return cancelled.load(std::memory_order_acquire) || (parent && parent->isCancelled()); }
    void check() const
    {
        if (isCancelled())
            throw Exception("columnar query cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    }

    class Scope
    {
    public:
        explicit Scope(const ColumnarCancellation & cancellation)
            : interfaces(cancellation.interfaces)
            , scope(interfaces->enter(cancellation.token))
        {}
        ~Scope() { interfaces->leave(scope); }
        Scope(const Scope &) = delete;
        Scope & operator=(const Scope &) = delete;

    private:
        const ColumnarCancellationInterfaces * interfaces;
        void * scope;
    };

private:
    std::shared_ptr<ColumnarCancellation> parent;
    const ColumnarCancellationInterfaces * interfaces;
    void * token;
    std::atomic_bool cancelled{false};
};
using ColumnarCancellationPtr = std::shared_ptr<ColumnarCancellation>;
} // namespace DB
