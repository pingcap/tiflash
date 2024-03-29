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

#include <Common/TiFlashBuildInfo.h>
#include <Common/getFQDNOrHostName.h>
#include <Core/Defines.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <port/unistd.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


void ClientInfo::write(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(query_kind), out);
    if (empty())
        return;

    writeBinary(initial_user, out);
    writeBinary(initial_query_id, out);
    writeBinary(initial_address.toString(), out);

    writeBinary(static_cast<UInt8>(interface), out);

    if (interface == Interface::TCP)
    {
        writeBinary(os_user, out);
        writeBinary(client_hostname, out);
        writeBinary(client_name, out);
        writeVarUInt(client_version_major, out);
        writeVarUInt(client_version_minor, out);
        writeVarUInt(client_version_patch, out);
    }

    writeBinary(quota_key, out);
}


void ClientInfo::read(ReadBuffer & in)
{
    UInt8 read_query_kind = 0;
    readBinary(read_query_kind, in);
    query_kind = static_cast<QueryKind>(read_query_kind);
    if (empty())
        return;

    readBinary(initial_user, in);
    readBinary(initial_query_id, in);

    String initial_address_string;
    readBinary(initial_address_string, in);
    initial_address = Poco::Net::SocketAddress(initial_address_string);

    UInt8 read_interface = 0;
    readBinary(read_interface, in);
    interface = static_cast<Interface>(read_interface);

    if (interface == Interface::TCP)
    {
        readBinary(os_user, in);
        readBinary(client_hostname, in);
        readBinary(client_name, in);
        readVarUInt(client_version_major, in);
        readVarUInt(client_version_minor, in);
        readVarUInt(client_version_patch, in);
    }

    readBinary(quota_key, in);
}


void ClientInfo::fillOSUserHostNameAndVersionInfo()
{
    os_user.resize(256, '\0');
    if (0 == getlogin_r(&os_user[0], os_user.size() - 1))
        os_user.resize(strlen(os_user.data()));
    else
        os_user.clear(); /// Don't mind if we cannot determine user login.

    client_hostname = getFQDNOrHostName();

    client_version_major = TiFlashBuildInfo::getMajorVersion();
    client_version_minor = TiFlashBuildInfo::getMinorVersion();
    client_version_patch = TiFlashBuildInfo::getPatchVersion();
}


} // namespace DB
