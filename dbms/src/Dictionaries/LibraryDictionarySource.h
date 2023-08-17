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

#include <Common/SharedLibrary.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalResultDescription.h>
#include <Dictionaries/IDictionarySource.h>
#include <common/LocalDateTime.h>


namespace Poco
{
class Logger;

namespace Util
{
class AbstractConfiguration;
}
} // namespace Poco


namespace DB
{
class CStringsHolder;

/// Allows loading dictionaries from dynamic libraries (.so)
/// Experimental version
/// Example: dbms/tests/external_dictionaries/dictionary_library/dictionary_library.cpp
class LibraryDictionarySource final : public IDictionarySource
{
public:
    LibraryDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        const Context & context);

    LibraryDictionarySource(const LibraryDictionarySource & other);

    ~LibraryDictionarySource();

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{
            "Method loadUpdatedAll is unsupported for LibraryDictionarySource",
            ErrorCodes::NOT_IMPLEMENTED};
    }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    const DictionaryStructure dict_struct;
    const std::string config_prefix;
    const std::string path;
    Block sample_block;
    const Context & context;
    SharedLibraryPtr library;
    ExternalResultDescription description;
    std::shared_ptr<CStringsHolder> settings;
    void * lib_data = nullptr;
};
} // namespace DB
