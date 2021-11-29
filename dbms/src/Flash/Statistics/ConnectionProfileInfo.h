#pragma once

#include <common/types.h>

#include <memory>

namespace DB
{
struct ConnectionProfileInfo
{
    String connection_type;
    size_t packets = 0;
    size_t bytes = 0;

    explicit ConnectionProfileInfo(const String & connection_type_)
        : connection_type(connection_type_)
    {}

    virtual ~ConnectionProfileInfo() = default;

    virtual String toJson() const final;

protected:
    /// If not empty, start with ','
    virtual String extraToJson() const { return ""; }
};

using ConnectionProfileInfoPtr = std::shared_ptr<ConnectionProfileInfo>;
} // namespace DB