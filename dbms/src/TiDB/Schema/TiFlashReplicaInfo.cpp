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

#include <Common/Exception.h>
#include <Common/types ̰.h>
#include <TiDB/Schema/TiFlashReplicaInfo.h>

namespace TiDB
{
Poco::JSON::Object::Ptr TiFlashReplicaInfo::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("Count", count);

#ifndef NDEBUG
    // Check stringify in Debug mode
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (TiFlashReplicaInfo): " + e.displayText(),
        DB::Exception(e));
}

void TiFlashReplicaInfo::deserialize(Poco::JSON::Object::Ptr & json)
try
{
    count = json->getValue<UInt64>("Count");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        String(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (TiFlashReplicaInfo): " + e.displayText(),
        DB::Exception(e));
}
} // namespace TiDB