#pragma once

#include <random>

namespace DB
{
template <typename IntType>
class UniformRandomIntGenerator
{
public:
    /// [min, max]
    UniformRandomIntGenerator(IntType min, IntType max)
        : dis(std::uniform_int_distribution<IntType>(min, max))
        , gen(std::default_random_engine(std::random_device{}()))
    {}

    IntType rand()
    {
        return dis(gen);
    }

private:
    std::uniform_int_distribution<IntType> dis;
    std::default_random_engine gen;
};
} // namespace DB