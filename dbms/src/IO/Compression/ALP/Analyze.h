// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <IO/Compression/ALP/Compression.h>

#include <random>
#include <span>

namespace DB::ALP
{

template <typename T>
class Analyze
{
public:
    using State = CompressionState<T>;

    static void run(const std::span<const T> & vectors, size_t n_samples, State & state)
    {
        std::vector<std::vector<T>> vectors_sampled;
        vectors_sampled.reserve(n_samples);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, vectors.size() - 1);

        // Sample vectors.
        // Each vector is a sample of SAMPLES_PER_VECTOR randomly selected values.
        for (size_t i = 0; i < n_samples; ++i)
        {
            std::vector<T> sample;
            sample.reserve(Constants::SAMPLES_PER_VECTOR);
            for (size_t j = 0; j < Constants::SAMPLES_PER_VECTOR; ++j)
            {
                auto random_idx = dis(gen);
                sample.push_back(vectors[random_idx]);
            }
            vectors_sampled.emplace_back(std::move(sample));
        }
        Compression<T, true>::findTopKCombinations(vectors_sampled, state);
    }
};

}; // namespace DB::ALP
