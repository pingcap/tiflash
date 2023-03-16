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

#include <atomic>
#include <ostream>

namespace DB
{
class FieldVisitorToDebugString;
}

class Redact
{
public:
    static void setRedactLog(bool v);

    static std::string handleToDebugString(int64_t handle);
    static std::string keyToDebugString(const char * key, size_t size);

    static std::string keyToHexString(const char * key, size_t size);

    static void keyToDebugString(const char * key, size_t size, std::ostream & oss);
    static std::string hexStringToKey(const char * start, size_t len);

    friend class DB::FieldVisitorToDebugString;

protected:
    Redact() = default;

private:
    // Log user data to log only when this flag is set to false.
    static std::atomic<bool> REDACT_LOG;
};
