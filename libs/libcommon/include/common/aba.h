// Copyright 2022 PingCAP, Ltd.
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

// Inspired by https://github.com/microsoft/snmalloc/blob/0.6.0/src/snmalloc/ds/aba.h.
// MIT License: https://github.com/microsoft/snmalloc/blob/0.6.0/LICENSE
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
/**
 * This file contains an abstraction of ABA protection. This API should be
 * implementable with double-word compare and exchange or with load-link
 * store conditional.
 *
 * We provide a lock based implementation as a backup for other platforms
 * without appropriate intrinsics.
 */
namespace common
{
union alignas(2 * sizeof(std::size_t)) Linked
{
    struct
    {
        void * ptr;
        uintptr_t aba;
    };
    __int128 whole{};
};

/// Perform hardware 16byte compare and exchange operation.
bool aba_xchg16b(
    std::atomic<Linked> & src,
    Linked & cmp,
    Linked with);

/// Relax CPU IO pipeline and speculation.
void aba_generic_cpu_relax();

/// Detect if hardware operations are supported.
bool aba_runtime_has_xchg16b();

template <typename T>
class ABAFast
{
public:
    struct Independent
    {
        std::atomic<T *> ptr{nullptr};
        std::atomic<uintptr_t> aba{0};
    };

    static_assert(
        sizeof(Linked) == sizeof(Independent),
        "Expecting identical struct sizes in union");
    static_assert(
        sizeof(Linked) == (2 * sizeof(std::size_t)),
        "Expecting ABA to be the size of two pointers");

private:
    union
    {
        alignas(2 * sizeof(std::size_t)) std::atomic<Linked> linked{};
        Independent independent;
    };

public:
    constexpr ABAFast()
        : independent()
    {}

    void init(T * x)
    {
        independent.ptr.store(x, std::memory_order_relaxed);
        independent.aba.store(0, std::memory_order_relaxed);
    }

    struct Cmp;

    Cmp read()
    {
        return Cmp{{{independent.ptr.load(std::memory_order_relaxed),
                     independent.aba.load(std::memory_order_relaxed)}},
                   this};
    }

    struct Cmp
    {
        ABAFast * parent;
        Linked old;

        /*
       * MSVC apparently does not like the implicit constructor it creates when
       * asked to interpret its input as C++20; it rejects the construction up
       * in read(), above.  Help it out by making the constructor explicit.
       */
        Cmp(Linked old, ABAFast * parent)
            : parent(parent)
            , old(old)
        {}

        T * ptr()
        {
            return static_cast<T *>(old.ptr);
        }

        bool storeConditional(T * value)
        {
            Linked xchg{{value, old.aba + 1}};
            std::atomic<Linked> & addr = parent->linked;
            auto result = aba_xchg16b(addr, old, xchg);
            return result;
        }

        void reset(){};
    };
};

template <typename T>
class ABAGeneric
{
    std::atomic<T *> ptr = nullptr;
    std::atomic<size_t> lock = ATOMIC_FLAG_INIT;

public:
    void init(T * x)
    {
        ptr.store(x, std::memory_order_relaxed);
    }

    struct Cmp;

    Cmp read()
    {
        while (true)
        {
            while (lock.load(std::memory_order_acquire))
            {
                ::common::aba_generic_cpu_relax();
            }
            if (!lock.exchange(1, std::memory_order_acq_rel))
            {
                break;
            }
        }
        return Cmp{this};
    }

    struct Cmp
    {
        ABAGeneric * parent;

    public:
        T * ptr()
        {
            return parent->ptr;
        }

        bool storeConditional(T * t)
        {
            parent->ptr = t;
            return true;
        }

        void reset()
        {
            parent->lock.store(0, std::memory_order_release);
        }
    };
};

template <typename T>
struct ABA
{
    bool is_fast{};

    union
    {
        ABAFast<T> fast;
        ABAGeneric<T> generic;
    };

    struct Cmp
    {
        union
        {
            typename ABAFast<T>::Cmp fast;
            typename ABAGeneric<T>::Cmp generic;
        };

        bool is_fast;

        T * ptr()
        {
            if (is_fast)
            {
                return fast.ptr();
            }
            else
            {
                return generic.ptr();
            }
        }
        bool storeConditional(T * value)
        {
            if (is_fast)
            {
                return fast.storeConditional(value);
            }
            else
            {
                return generic.storeConditional(value);
            }
        }
        ~Cmp()
        {
            if (is_fast)
            {
                fast.reset();
            }
            else
            {
                generic.reset();
            }
        }
        Cmp(){}; // NOLINT(modernize-use-equals-default,cppcoreguidelines-pro-type-member-init)
        Cmp(const Cmp &) = delete;
        Cmp(Cmp &&) noexcept = default;
    };

    explicit ABA(bool force_generic = false)
    {
        init(nullptr, force_generic);
    }

    void init(T * x, bool force_generic)
    {
        is_fast = !force_generic && aba_runtime_has_xchg16b();
        if (is_fast)
        {
            fast.init(x);
        }
        else
        {
            generic.init(x);
        }
    }

    Cmp read()
    {
        Cmp result;

        result.is_fast = is_fast;

        if (is_fast)
        {
            result.fast = fast.read();
        }
        else
        {
            result.generic = generic.read();
        }

        return result;
    }
};

} // namespace common