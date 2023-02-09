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

#pragma once

#include <string>

namespace DB
{
enum MppVersion : int64_t
{
    MppVersionV0 = 0,
    MppVersionV1,
    //
    MppVersionMAX,
};

enum MPPDataPacketVersion : int64_t
{
    MPPDataPacketV0 = 0,
    MPPDataPacketV1,
    //
    MPPDataPacketMAX,
};

bool CheckMppVersion(int64_t mpp_version);
std::string GenMppVersionErrorMessage(int64_t mpp_version);
int64_t GetMppVersion();

} // namespace DB