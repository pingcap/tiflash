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

#include <Common/TiFlashException.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <fmt/printf.h>

namespace DB
{
class ErrorExporter
{
public:
    explicit ErrorExporter(WriteBuffer & dest)
        : wb(dest)
    {}

    ErrorExporter() = delete;

    ~ErrorExporter() { flush(); }

    void writeError(const TiFlashError & error);

private:
    static const char * ERROR_TEMPLATE;

    WriteBuffer & wb;

    void flush();
};

const char * ErrorExporter::ERROR_TEMPLATE = "[\"%s\"]\n"
                                             "error = '''\n%s\n'''\n\n";

void ErrorExporter::writeError(const TiFlashError & error)
{
    String buffer = fmt::sprintf(
        ERROR_TEMPLATE,
        error.standardName().data(),
        error.message_template.data(),
        error.description.data(),
        error.workaround.data());
    DB::writeString(buffer, wb);
}

void ErrorExporter::flush()
{
    wb.next();
}

} // namespace DB
