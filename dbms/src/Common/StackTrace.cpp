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

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <Common/StackTrace.h>
#include <common/demangle.h>
#include <execinfo.h>
#include <string.h>

#include <sstream>


StackTrace::StackTrace()
{
    frames_size = backtrace(frames, STACK_TRACE_MAX_DEPTH);
}

std::string StackTrace::toString() const
{
    char ** symbols = backtrace_symbols(frames, frames_size);
    std::stringstream res;

    if (!symbols)
        return "Cannot get symbols for stack trace.\n";

    try
    {
        for (size_t i = 0, size = frames_size; i < size; ++i)
        {
            /// We do "demangling" of names. The name is in parenthesis, before the '+' character.

            char * name_start = nullptr;
            char * name_end = nullptr;
            std::string demangled_name;
            int status = 0;

            if (nullptr != (name_start = strchr(symbols[i], '('))
                && nullptr != (name_end = strchr(name_start, '+')))
            {
                ++name_start;
                *name_end = '\0';
                demangled_name = demangle(name_start, status);
                *name_end = '+';
            }

            res << i << ". ";

            if (0 == status && name_start && name_end)
            {
                res.write(symbols[i], name_start - symbols[i]);
                res << demangled_name << name_end;
            }
            else
                res << symbols[i];

            res << std::endl;
        }
    }
    catch (...)
    {
        free(symbols);
        throw;
    }

    free(symbols);
    return res.str();
}
