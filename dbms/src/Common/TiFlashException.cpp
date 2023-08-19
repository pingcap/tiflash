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

#include <Common/TiFlashException.h>

#include <set>

namespace DB
{
void TiFlashErrorRegistry::initialize()
{
    // Used to check uniqueness of classes
    std::set<std::string> all_classes;

    using namespace Errors;

/// Register error classes, and check their uniqueness
#define C(class_name, ...)                                                                          \
    {                                                                                               \
        using namespace class_name;                                                                 \
        if (auto [_, took_place] = all_classes.insert(#class_name); !took_place)                    \
        {                                                                                           \
            (void)_;                                                                                \
            throw Exception("Error Class " #class_name " is duplicate, please check related code"); \
        }                                                                                           \
        __VA_ARGS__                                                                                 \
    }
#define E(error_code, desc, workaround, message_template) registerError(NAME, #error_code, desc, workaround, message_template);

    ERROR_CLASS_LIST
#undef C
#undef E
}

void TiFlashErrorRegistry::registerError(const std::string & error_class, const std::string & error_code, const std::string & description, const std::string & workaround, const std::string & message_template)
{
    TiFlashError error{error_class, error_code, description, workaround, message_template};
    if (all_errors.find({error_class, error_code}) == all_errors.end())
    {
        all_errors.emplace(std::make_pair(error_class, error_code), std::move(error));
    }
    else
    {
        throw Exception("TiFLashError: " + error_class + ":" + error_code + " has been registered.");
    }
}

void TiFlashErrorRegistry::registerErrorWithNumericCode(const std::string & error_class, int error_code, const std::string & description, const std::string & workaround, const std::string & message_template)
{
    std::string error_code_str = std::to_string(error_code);
    registerError(error_class, error_code_str, description, workaround, message_template);
}

// Standard error text with format:
// [{Component}:{ErrorClass}:{ErrorCode}] {message}
std::string TiFlashException::standardText() const
{
    std::string text{};
    if (!message().empty())
    {
        text.append("[");
        text.append("FLASH:" + error.error_class + ":" + error.error_code);
        text.append("] ");
        text.append(message());
    }
    return text;
}

} // namespace DB
