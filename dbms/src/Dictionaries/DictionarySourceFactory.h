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

#include <Dictionaries/IDictionarySource.h>

#include <ext/singleton.h>
#include <unordered_map>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}

class Logger;
} // namespace Poco

namespace DB
{
class Context;
struct DictionaryStructure;

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : public ext::Singleton<DictionarySourceFactory>
{
public:
    using Creator = std::function<DictionarySourcePtr(
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        const Context & context)>;

    DictionarySourceFactory();

    void registerSource(const std::string & source_type, Creator create_source);

    DictionarySourcePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DictionaryStructure & dict_struct,
        Context & context) const;

private:
    using SourceRegistry = std::unordered_map<std::string, Creator>;
    SourceRegistry registered_sources;

    Poco::Logger * log;
};

} // namespace DB
