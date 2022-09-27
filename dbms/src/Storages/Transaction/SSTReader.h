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

#pragma once

#include <Common/nocopyable.h>
#include <Storages/Transaction/ProxyFFI.h>

namespace DB
{
struct SSTReader
{
    bool remained() const;
    BaseBuffView key() const;
    BaseBuffView value() const;
    void next();

    DISALLOW_COPY_AND_MOVE(SSTReader);
    SSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view);
    ~SSTReader();

private:
    const TiFlashRaftProxyHelper * proxy_helper;
    SSTReaderPtr inner;
    ColumnFamilyType type;
};

template <typename R, typename E>
struct MultiSSTReader: SSTReader
{
    using Initer = std::function<std::unique_ptr<R>(const E &)>;

    DISALLOW_COPY_AND_MOVE(MultiSSTReader);

    bool remained() const
    {
        this->maybe_next_reader();
        return proxy_helper->sst_reader_interfaces.fn_remained(inner, type);
    }
    BaseBuffView key() const
    {
        return proxy_helper->sst_reader_interfaces.fn_key(inner, type);
    }
    BaseBuffView value() const
    {
        return proxy_helper->sst_reader_interfaces.fn_value(inner, type);
    }
    void next()
    {
        this->maybe_next_reader();
        return proxy_helper->sst_reader_interfaces.fn_next(inner, type);
    }
    void maybe_next_reader()
    {
        if (!proxy_helper->sst_reader_interfaces.fn_remained(inner, type))
        {
            current++;
            if (current < args.size())
            {
                proxy_helper->sst_reader_interfaces.fn_gc(inner, type);
                inner = initer(args[current]);
            }
        }
    }

    MultiSSTReader(const TiFlashRaftProxyHelper * proxy_helper_, ColumnFamilyType type_, Initer initer_, std::vector<E> && args_)
        : proxy_helper(proxy_helper_)
        , type(type_)
        , initer(initer_)
        , args(args_)
        , current(0)
    {
        assert(args.size() > 0);
        inner = initer(args[current]);
    }

    ~MultiSSTReader()
    {
        proxy_helper->sst_reader_interfaces.fn_gc(inner, type);
    }

private:
    mutable std::unique_ptr<R> inner;
    const TiFlashRaftProxyHelper * proxy_helper;
    ColumnFamilyType type;
    Initer initer;
    std::vector<E> args;
    mutable int current;
};

} // namespace DB
