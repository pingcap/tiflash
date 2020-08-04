#pragma once

#include <Common/TiFlashException.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

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

const char * ErrorExporter::ERROR_TEMPLATE = "[error.%s]\n"
                                             "error = '''%s'''\n"
                                             "description = '''%s'''\n"
                                             "workaround = '''%s'''\n\n";

void ErrorExporter::writeError(const TiFlashError & error)
{
    char buffer[4096];
    std::sprintf(
        buffer, ERROR_TEMPLATE, error.standardName().data(), error.message_template.data(), error.description.data(), error.workaround.data());
    DB::writeString(std::string(buffer), wb);
    return;
}

void ErrorExporter::flush() { wb.next(); }

} // namespace DB
