#pragma once

namespace DB
{
struct ColumnFamilyName
{
    const static std::string Lock;
    const static std::string Default;
    const static std::string Write;
};

enum class ColumnFamilyType : uint8_t
{
    Lock = 0,
    Write,
    Default,
};

ColumnFamilyType NameToCF(const std::string & cf);
const std::string & CFToName(const ColumnFamilyType type);
} // namespace DB
