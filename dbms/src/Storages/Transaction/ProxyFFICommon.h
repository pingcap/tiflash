#pragma once

#include <cstring>

namespace DB
{
struct RawCppString : std::string
{
    using Base = std::string;
    using Base::Base;
    RawCppString() = delete;
    RawCppString(Base && src) : Base(std::move(src)) {}
    RawCppString(const Base & src) : Base(src) {}
    RawCppString(const RawCppString &) = delete;

    template <class... Args>
    static RawCppString * New(Args &&... _args)
    {
        return new RawCppString{std::forward<Args>(_args)...};
    }
};

} // namespace DB
