#pragma once

#include <string>

namespace DB
{
using String = std::string;

struct EncryptionPath
{
    EncryptionPath(const String & full_path_, const String & file_name_) : full_path{full_path_}, file_name{file_name_} {}
    const String full_path;
    const String file_name;
};
} // namespace DB
