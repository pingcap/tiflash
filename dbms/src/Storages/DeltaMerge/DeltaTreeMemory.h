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
#include <Common/NumaAwareMemoryHierarchy.h>

namespace DB::DM
{

NumaAwareMemoryHierarchy::Client getClient(size_t size, size_t alignment);

template <class Leaf, class Intern>
class MultiLevelAllocator
{
private:
    NumaAwareMemoryHierarchy::Client leaf_client;
    NumaAwareMemoryHierarchy::Client intern_client;

public:
    MultiLevelAllocator()
        : leaf_client(getClient(sizeof(Leaf), alignof(Leaf)))
        , intern_client(getClient(sizeof(Intern), alignof(Intern)))
    {}

    void * allocateLeaf()
    {
        return leaf_client.allocate();
    }

    void * allocateIntern()
    {
        return intern_client.allocate();
    }

    void deallocateLeaf(void * p)
    {
        return leaf_client.deallocate(p);
    }

    void deallocateIntern(void * p)
    {
        return intern_client.deallocate(p);
    }
};

template <class Base, class Leaf, class Intern>
class ProxyAllocator
{
private:
    Base base;

public:
    ProxyAllocator()
        : base()
    {}

    void * allocateLeaf()
    {
        return base.alloc(sizeof(Leaf), alignof(Leaf));
    }

    void * allocateIntern()
    {
        return base.alloc(sizeof(Intern), alignof(Intern));
    }

    void deallocateLeaf(void * p)
    {
        return base.free(p, sizeof(Leaf));
    }

    void deallocateIntern(void * p)
    {
        return base.free(p, sizeof(Intern));
    }
};

} // namespace DB::DM
