#pragma once

namespace DB
{
template <typename T>
struct UnaryCallback
{
    virtual void execute(T & val) = 0;
    virtual ~UnaryCallback() = default;
};
} // namespace DB
