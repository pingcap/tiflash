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

#pragma once

#include <Poco/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/types.h>

#include <ext/singleton.h>


/** @brief Class that lets you know if a search engine or operating system belongs
  * another search engine or operating system, respectively.
  * Information about the hierarchy of regions is downloaded from the database.
  */
class TechDataHierarchy
{
private:
    UInt8 os_parent[256]{};
    UInt8 se_parent[256]{};

public:
    void reload();

    /// Has corresponding section in configuration file.
    static bool isConfigured(const Poco::Util::AbstractConfiguration & config);


    /// The "belongs" relation.
    bool isOSIn(UInt8 lhs, UInt8 rhs) const
    {
        while (lhs != rhs && os_parent[lhs])
            lhs = os_parent[lhs];

        return lhs == rhs;
    }

    bool isSEIn(UInt8 lhs, UInt8 rhs) const
    {
        while (lhs != rhs && se_parent[lhs])
            lhs = se_parent[lhs];

        return lhs == rhs;
    }


    UInt8 OSToParent(UInt8 x) const
    {
        return os_parent[x];
    }

    UInt8 SEToParent(UInt8 x) const
    {
        return se_parent[x];
    }


    /// To the topmost ancestor.
    UInt8 OSToMostAncestor(UInt8 x) const
    {
        while (os_parent[x])
            x = os_parent[x];
        return x;
    }

    UInt8 SEToMostAncestor(UInt8 x) const
    {
        while (se_parent[x])
            x = se_parent[x];
        return x;
    }
};


class TechDataHierarchySingleton : public ext::Singleton<TechDataHierarchySingleton>
    , public TechDataHierarchy
{
};
