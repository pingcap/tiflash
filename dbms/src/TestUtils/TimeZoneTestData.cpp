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

namespace DB::tests
{
namespace
{
#include <TimeZoneTestData.inc>

constexpr std::string_view timezone_test_prefix = "tiflash-test/";

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

    std::string Version() const override { return std::string(timezone_test_data_version); }

private:
    std::string_view data;
};

std::unique_ptr<cctz::ZoneInfoSource> getTestTimeZoneSource(
    const std::string & name,
    const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string &)> & fallback)
{
    std::string_view zone_name(name);
    if (!zone_name.starts_with(timezone_test_prefix))
        return fallback(name);
    zone_name.remove_prefix(timezone_test_prefix.size());
    for (const auto & [zone, data] : timezone_test_data)
        if (zone_name == zone)
            return std::make_unique<TestZoneInfoSource>(data);
    return nullptr;
}
} // namespace

std::string getTestTimeZoneName(std::string_view name)
{
    return std::string(timezone_test_prefix) + std::string(name);
}
} // namespace DB::tests

// cctz's extension point is linked only into the unit test binary.
namespace cctz_extension
{
ZoneInfoSourceFactory zone_info_source_factory = DB::tests::getTestTimeZoneSource;
} // namespace cctz_extension
