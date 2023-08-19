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

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

#include <istream>

namespace cpptoml
{
class table;
}
namespace DB
{
using TOMLTablePtr = std::shared_ptr<cpptoml::table>;

class TOMLConfiguration : public Poco::Util::AbstractConfiguration
{
public:
    explicit TOMLConfiguration(TOMLTablePtr toml_doc);

protected:
    bool getRaw(const std::string & key, std::string & value) const;
    /// If the property with the given key exists, stores the property's value
    /// in value and returns true. Otherwise, returns false.

    void setRaw(const std::string & key, const std::string & value);
    /// Sets the property with the given key to the given value.
    /// An already existing value for the key is overwritten.

    void enumerate(const std::string & key, Keys & range) const;
    /// Returns in range the names of all subkeys under the given key.
    /// If an empty key is passed, all root level keys are returned.

    void removeRaw(const std::string & key);
    /// Removes the property with the given key.
    /// Does nothing if the key does not exist.
private:
    TOMLTablePtr root;
    bool findParent(const std::string & key, TOMLTablePtr & parent, std::string & child_key);
};

} // namespace DB
