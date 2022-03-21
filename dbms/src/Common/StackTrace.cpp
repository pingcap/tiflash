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
#include <Symbolization/Symbolization.h>
#include <fmt/core.h>
#include <Common/FmtUtils.h>
#include <sstream>


StackTrace::StackTrace()
{
    frames_size = backtrace(frames, STACK_TRACE_MAX_DEPTH);
}

std::string StackTrace::toString() const
{
     DB::FmtBuffer output;

    auto prefix_size = std::size(TIFLASH_SOURCE_PREFIX);

    for (size_t f = 0; f < frames_size; ++f)
    {
        output.append("\n");
        auto sym_info = _tiflash_symbolize(frames[f]);
        output.fmtAppend("{:>16}", frames[f]);

        if (sym_info.symbol_name)
        {
            int status = 0;
            auto demangled = demangle(sym_info.symbol_name, status);
            if (status == 0)
            {
                output.append("\t");
                output.append(demangled);
            }
            else
            {
                output.append("\t");
                output.append(sym_info.symbol_name);
            }
        }
        else
        {
            output.append("\t<unknown symbol>");
        }

        if (sym_info.object_name)
        {
            std::string_view view(sym_info.object_name);
            auto pos = view.rfind('/');
            if (pos != std::string_view::npos)
            {
                output.fmtAppend(" [{}+{}]", view.substr(pos + 1), sym_info.svma);
            }
            else
            {
                output.fmtAppend(" [{}+{}]", view, sym_info.svma);
            }
        }

        if (sym_info.source_filename)
        {
            output.append("\n");
            std::string_view view(sym_info.source_filename, sym_info.source_filename_length);
            if (view.find(TIFLASH_SOURCE_PREFIX) != std::string_view::npos)
            {
                output.fmtAppend("{:>16}\t{}:{}", "", view.substr(prefix_size), sym_info.lineno);
            }
            else
            {
                output.fmtAppend("{:>16}\t{}:{}", "", view, sym_info.lineno);
            }
        }
    }

    return output.toString();
}
