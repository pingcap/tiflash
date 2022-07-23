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

#include <Common/SyncPoint/Ctl.h>

#include <functional>

namespace DB
{

namespace SyncPointOps
{

class BaseOp
{
public:
    virtual ~BaseOp() = default;
    virtual void enable(){};
    virtual void disable(){};
    virtual void operator()() = 0;
};

class WaitAndPause : public BaseOp
{
public:
    /**
     * When executed, it waits for the specified sync point.
     */
    explicit WaitAndPause(const char * name_)
        : name(name_)
    {}

    void enable() override
    {
        SyncPointCtl::enable(name.c_str());
    };
    void disable() override
    {
        SyncPointCtl::disable(name.c_str());
    };
    void operator()() override
    {
        SyncPointCtl::waitAndPause(name.c_str());
    };

private:
    std::string name;
};

class Next : public BaseOp
{
public:
    /**
     * When executed, it continues the execution of the specified sync point.
     * It must be placed after corresponding `WaitAndPause()`.
     */
    explicit Next(const char * name_)
        : name(name_)
    {}

    void operator()() override
    {
        SyncPointCtl::next(name.c_str());
    };

private:
    std::string name;
};

class WaitAndNext : public BaseOp
{
public:
    explicit WaitAndNext(const char * name_)
        : name(name_)
    {}

    void enable() override
    {
        SyncPointCtl::enable(name.c_str());
    };
    void disable() override
    {
        SyncPointCtl::disable(name.c_str());
    };
    void operator()() override
    {
        SyncPointCtl::waitAndPause(name.c_str());
        SyncPointCtl::next(name.c_str());
    };

private:
    std::string name;
};

class Call : public BaseOp
{
public:
    /**
     * When executed, it invokes the specified function.
     */
    explicit Call(std::function<void()> fn_)
        : fn(fn_)
    {}

    void operator()() override
    {
        if (fn)
            fn();
    };

private:
    std::function<void()> fn;
};

} // namespace SyncPointOps

} // namespace DB
