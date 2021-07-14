#pragma once

#include <Common/TiFlashException.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <fmt/printf.h>

namespace DB
{

class ErrorExporter
{
public:
    explicit ErrorExporter(WriteBuffer & dest) : wb(dest) {}

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
        ERROR_TEMPLATE, error.standardName().data(), error.message_template.data(), error.description.data(), error.workaround.data());
    DB::writeString(buffer, wb);
    return;
}

void ErrorExporter::flush() { wb.next(); }

} // namespace DB
