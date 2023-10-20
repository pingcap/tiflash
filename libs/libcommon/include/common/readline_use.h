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

#include <common/config_common.h>

/// Different line editing libraries can be used depending on the environment.
#if USE_READLINE
#include <readline/history.h>
#include <readline/readline.h>
#elif USE_LIBEDIT
#include <editline/history.h>
#include <editline/readline.h>
#else
#include <cstring>
#include <iostream>
#include <string>
inline char * readline(const char * prompt)
{
    std::string s;
    std::cout << prompt;
    std::getline(std::cin, s);

    if (!std::cin.good())
        return nullptr;
    return strdup(s.data());
}
#define add_history(...) \
    do                   \
    {                    \
    } while (0);
#define rl_bind_key(...) \
    do                   \
    {                    \
    } while (0);
#endif
