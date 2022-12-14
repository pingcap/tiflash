// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Mpp/Utils.h>
#include <Poco/String.h>
#include <fmt/format.h>

#include <memory>

namespace DB
{
mpp::MPPDataPacket getPacketWithError(String reason)
{
    mpp::MPPDataPacket data;
    auto err = std::make_unique<mpp::Error>();
    err->set_msg(std::move(reason));
    data.set_allocated_error(err.release());
    return data;
}

void trimStackTrace(String & message)
{
    auto stack_trace_pos = message.find("Stack trace");
    if (stack_trace_pos != String::npos)
    {
        message.resize(stack_trace_pos);
        Poco::trimRightInPlace(message);
    }
}

} // namespace DB

namespace TiDB
{

constexpr int64_t MPP_VERSION = 1;
static const char * MPP_TIFLASH_RELEASE_VERSION = "v6.5.0";
constexpr int64_t MIN_MPP_VERSION = 0;

bool CheckMppVersion(int64_t mpp_version)
{
    return mpp_version >= MIN_MPP_VERSION && mpp_version <= MPP_VERSION;
}

std::string GenMppVersionErrorMessage(int64_t mpp_version)
{
    auto err_msg = fmt::format("Invalid mpp version `{}`, expect version: min `{}`, max `{}` release version `{}`",
                               mpp_version,
                               TiDB::MIN_MPP_VERSION,
                               TiDB::MPP_VERSION,
                               MPP_TIFLASH_RELEASE_VERSION);
    return err_msg;
}

int64_t GetMppVersion()
{
    return MPP_VERSION;
}

} // namespace TiDB