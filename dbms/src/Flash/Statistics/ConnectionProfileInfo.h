#pragma once

#include <common/types.h>

#include <memory>

namespace DB
{
struct ConnectionProfileInfo
{
    String connection_type;
    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    explicit ConnectionProfileInfo(const String & connection_type_)
        : connection_type(connection_type_)
    {}

    virtual String toJson() const = 0;

    virtual ~ConnectionProfileInfo() = default;
};

using ConnectionProfileInfoPtr = std::shared_ptr<ConnectionProfileInfo>;
} // namespace DB