#pragma once

#include <RaftStoreProxyFFI/ColumnFamily.h>

#include <string>

namespace DB
{

struct ColumnFamilyName
{
    const static std::string Lock;
    const static std::string Default;
    const static std::string Write;
};

ColumnFamilyType NameToCF(const std::string & cf);
const std::string & CFToName(const ColumnFamilyType type);
} // namespace DB
