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

#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/Utils.h>
#include <Poco/String.h>
#include <common/defines.h>
#include <fmt/format.h>

#include <array>
#include <memory>

namespace DB
{
mpp::MPPDataPacket getPacketWithError(String reason)
{
    mpp::MPPDataPacket data;
    auto err = std::make_unique<mpp::Error>();
    err->set_mpp_version(TiDB::GetMppVersion());
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
// Latest mpp-version supported by TiFlash
constexpr MppVersion MPP_VERSION = MppVersion((MppVersion::MppVersionMAX)-1);

// TODO: set version after committed
constexpr std::array<const char *, (MppVersion::MppVersionMAX)> MPP_TIFLASH_RELEASE_VERSION = {"", "?"};

// Check mpp-version is illegal
bool CheckMppVersion(int64_t mpp_version)
{
    return mpp_version >= (MppVersion::MppVersionV0) && mpp_version < (MppVersion::MppVersionMAX);
}

std::string GenMppVersionErrorMessage(int64_t mpp_version)
{
    auto err_msg = fmt::format("invalid mpp version `{}`, expect version: min `{}`, max `{}` release version `{}`",
                               mpp_version,
                               (MppVersion::MppVersionV0),
                               (MPP_VERSION),
                               MPP_TIFLASH_RELEASE_VERSION[(MPP_VERSION)]);
    return err_msg;
}

// Get latest mpp-version supported by TiFlash
int64_t GetMppVersion()
{
    return (MPP_VERSION);
}

std::string GetMppVersionReleaseInfo(int64_t mpp_version)
{
    if (CheckMppVersion(mpp_version))
    {
        return MPP_TIFLASH_RELEASE_VERSION[mpp_version];
    }
    return "unknown";
}

} // namespace TiDB