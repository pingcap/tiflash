// Copyright 2026 PingCAP, Inc.
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


#include <TestUtils/TimeZoneTestData.h>
#include <cctz/zone_info_source.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace
{
constexpr std::string_view prefix = "tiflash_test_2024a/";
#include "timezone_test_data/zones.inc"

class TestZoneInfoSource : public cctz::ZoneInfoSource
{
public:
    explicit TestZoneInfoSource(std::string_view data_)
        : data(data_)
    {}

    std::size_t Read(void * ptr, std::size_t size) override
    {
        size = std::min(size, data.size());
        std::memcpy(ptr, data.data(), size);
        data.remove_prefix(size);
        return size;
    }

    int Skip(std::size_t offset) override
    {
        if (offset > data.size())
            return -1;
        data.remove_prefix(offset);
        return 0;
    }

    std::string Version() const override { return "2024a"; }

private:
    std::string_view data;
};

std::unique_ptr<cctz::ZoneInfoSource> testZoneInfoFactory(
    const std::string & name,
    const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string &)> & fallback)
{
    if (!name.starts_with(prefix))
        return fallback(name);
    for (const auto & [zone, data] : zones)
        if (std::string_view(name).substr(prefix.size()) == zone)
            return std::make_unique<TestZoneInfoSource>(data);
    return nullptr;
}
} // namespace

namespace cctz_extension
{
// cctz's documented extension point; normal names retain the default source.
ZoneInfoSourceFactory zone_info_source_factory = testZoneInfoFactory;
} // namespace cctz_extension

namespace DB::tests
{
std::string timeZoneTestName(std::string_view name)
{
    return name == "UTC" ? "UTC" : std::string(prefix) + std::string(name);
}
} // namespace DB::tests
