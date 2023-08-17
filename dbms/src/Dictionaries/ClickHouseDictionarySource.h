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

#include <Client/ConnectionPoolWithFailover.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/IDictionarySource.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <memory>


namespace DB
{
/** Allows loading dictionaries from local ClickHouse instance
  *    @todo use ConnectionPoolWithFailover
  *    @todo invent a way to keep track of source modifications
  */
class ClickHouseDictionarySource final : public IDictionarySource
{
public:
    ClickHouseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block,
        Context & context);

    /// copy-constructor is provided in order to support cloneability
    ClickHouseDictionarySource(const ClickHouseDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override { return true; }
    bool supportsSelectiveLoad() const override { return true; }

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

    std::string toString() const override;

private:
    std::string getUpdateFieldAndDate();

    BlockInputStreamPtr createStreamForSelectiveLoad(const std::string & query);

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const std::string host;
    const UInt16 port;
    const bool secure;
    const std::string user;
    const std::string password;
    const std::string db;
    const std::string table;
    const std::string where;
    const std::string update_field;
    ExternalQueryBuilder query_builder;
    Block sample_block;
    Context & context;
    const std::string load_all_query;
};

} // namespace DB
