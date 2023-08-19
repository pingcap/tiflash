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

#include <Common/FmtUtils.h>
#include <Symbolization/Symbolization.h>

#include <string>
#define STACK_TRACE_MAX_DEPTH 32
#ifndef TIFLASH_SOURCE_PREFIX
#define TIFLASH_SOURCE_PREFIX ""
#endif
/// Lets you get a stacktrace
class StackTrace
{
public:
    /// The stacktrace is captured when the object is created
    StackTrace();

    /// Print to string
    std::string toString() const;

    template <class DemangleFunc>
    static void addr2line(DemangleFunc demangle_function, DB::FmtBuffer & buffer, void * address)
    {
        static constexpr size_t prefix_size = std::size(TIFLASH_SOURCE_PREFIX);
        auto sym_info = _tiflash_symbolize(address);
        buffer.fmtAppend("{:>16}", address);

        if (sym_info.symbol_name)
        {
            auto [demangled, status] = demangle_function(sym_info.symbol_name);
            if (status == 0)
            {
                buffer.append("\t");
                buffer.append(demangled);
            }
            else
            {
                buffer.append("\t");
                buffer.append(sym_info.symbol_name);
            }
        }
        else
        {
            buffer.append("\t<unknown symbol>");
        }

        if (sym_info.object_name)
        {
            std::string_view view(sym_info.object_name);
            auto pos = view.rfind('/');
            if (pos != std::string_view::npos)
            {
                buffer.fmtAppend(" [{}+{}]", view.substr(pos + 1), sym_info.svma);
            }
            else
            {
                buffer.fmtAppend(" [{}+{}]", view, sym_info.svma);
            }
        }

        if (sym_info.source_filename)
        {
            buffer.append("\n");
            std::string_view view(sym_info.source_filename, sym_info.source_filename_length);
            if (view.find(TIFLASH_SOURCE_PREFIX) != std::string_view::npos)
            {
                buffer.fmtAppend("{:>16}\t{}:{}", "", view.substr(prefix_size), sym_info.lineno);
            }
            else
            {
                buffer.fmtAppend("{:>16}\t{}:{}", "", view, sym_info.lineno);
            }
        }
    }

private:
    using Frame = void *;
    Frame frames[STACK_TRACE_MAX_DEPTH];
    size_t frames_size;
};
