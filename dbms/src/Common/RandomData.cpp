// Copyright 2023 PingCAP, Inc.
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

#include <Common/RandomData.h>
#include <fmt/format.h>

#include <random>

namespace DB::random
{

String randomString(UInt64 length)
{
    static const std::string charset{
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()|[]{}:;',<.>`~"};
    std::random_device rand_dev;
    std::mt19937_64 rand_gen(rand_dev());
    String str(length, '\x00');
    std::generate_n(str.begin(), str.size(), [&]() { return charset[rand_gen() % charset.size()]; });
    return str;
}

int randomTimeOffset()
{
    std::random_device rand_dev;
    std::mt19937_64 rand_gen(rand_dev());
    static constexpr int max_offset = 24 * 3600 * 10000; // 10000 days for test
    return (rand_gen() % max_offset) * (rand_gen() % 2 == 0 ? 1 : -1);
}

time_t randomUTCTimestamp()
{
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count() + randomTimeOffset();
}

struct tm randomLocalTime()
{
    time_t t = randomUTCTimestamp();
    struct tm res
    {
    };
    if (localtime_r(&t, &res) == nullptr)
    {
        throw std::invalid_argument(fmt::format("localtime_r({}) ret {}", t, strerror(errno)));
    }
    return res;
}

String randomDate()
{
    auto res = randomLocalTime();
    return fmt::format("{}-{}-{}", res.tm_year + 1900, res.tm_mon + 1, res.tm_mday);
}

String randomDateTime()
{
    auto res = randomLocalTime();
    return fmt::format(
        "{}-{}-{} {}:{}:{}",
        res.tm_year + 1900,
        res.tm_mon + 1,
        res.tm_mday,
        res.tm_hour,
        res.tm_min,
        res.tm_sec);
}

String randomDuration()
{
    auto res = randomLocalTime();
    return fmt::format("{}:{}:{}", res.tm_hour, res.tm_min, res.tm_sec);
}

String randomDecimal(uint64_t prec, uint64_t scale)
{
    std::random_device rand_dev;
    std::mt19937_64 rand_gen(rand_dev());
    auto s = std::to_string(rand_gen());
    if (s.size() < prec)
        s += String(prec - s.size(), '0');
    else if (s.size() > prec)
        s = s.substr(0, prec);
    return s.substr(0, prec - scale) + "." + s.substr(prec - scale);
}

} // namespace DB::random
