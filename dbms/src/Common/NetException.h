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


namespace DB
{
class NetException : public DB::Exception
{
public:
    explicit NetException(const std::string & msg, int code = 0)
        : DB::Exception(msg, code)
    {}
    NetException(const std::string & msg, const std::string & arg, int code = 0)
        : DB::Exception(msg, arg, code)
    {}
    NetException(const std::string & msg, const DB::Exception & exc, int code = 0)
        : DB::Exception(msg, exc, code)
    {}

    explicit NetException(const DB::Exception & exc)
        : DB::Exception(exc)
    {}
    explicit NetException(const Poco::Exception & exc)
        : DB::Exception(exc.displayText())
    {}

    const char * name() const throw() override { return "DB::NetException"; }
    const char * className() const throw() override { return "DB::NetException"; }
    DB::NetException * clone() const override { return new DB::NetException(*this); }
    void rethrow() const override { throw *this; }
};

} // namespace DB
