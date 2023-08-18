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

#include <Poco/ArchiveStrategy.h>
#include <Poco/Ext/TiFlashArchiveByTimestampsStrategy.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/LocalDateTime.h>
#include <Poco/String.h>
namespace Poco
{
void TiFlashLogFileChannel::setArchive(const std::string & archive)
{
    ArchiveStrategy * pStrategy = nullptr;
    if (archive == "number")
    {
        pStrategy = new ArchiveByNumberStrategy;
    }
    else if (archive == "timestamp")
    {
        if (_times == "utc")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<DateTime>;
        else if (_times == "local")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<LocalDateTime>;
        else
            throw PropertyNotSupportedException("times", _times);
    }
    else
        throw InvalidArgumentException("archive", archive);
    delete _pArchiveStrategy;
    pStrategy->compress(_compress);
    _pArchiveStrategy = pStrategy;
    _archive = archive;
}
} // namespace Poco