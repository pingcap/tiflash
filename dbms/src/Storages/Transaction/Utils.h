#pragma once

#include <variant>

namespace variant_op
{
template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...)->overloaded<Ts...>;
template <class T>
struct always_false : std::false_type
{
};
} // namespace variant_op
