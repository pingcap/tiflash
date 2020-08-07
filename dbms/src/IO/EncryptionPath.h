#include <string>

namespace DB
{
using String = std::string;

struct EncryptionPath
{
    EncryptionPath(const String & dir_name_, const String & file_name_) : dir_name{dir_name_}, file_name{file_name_} {}
    const String dir_name;
    const String file_name;
};
} // namespace DB
