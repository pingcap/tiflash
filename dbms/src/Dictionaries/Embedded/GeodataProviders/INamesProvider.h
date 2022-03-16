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

#include <Dictionaries/Embedded/GeodataProviders/Entries.h>

#include <memory>


// Iterates over all name entries in data source
class ILanguageRegionsNamesReader
{
public:
    virtual bool readNext(RegionNameEntry & entry) = 0;

    virtual ~ILanguageRegionsNamesReader() {}
};

using ILanguageRegionsNamesReaderPtr = std::unique_ptr<ILanguageRegionsNamesReader>;


// Regions names data source for one language
class ILanguageRegionsNamesDataSource
{
public:
    // data modified since last createReader invocation
    virtual bool isModified() const = 0;

    // Upper bound on total length of all names
    virtual size_t estimateTotalSize() const = 0;

    virtual ILanguageRegionsNamesReaderPtr createReader() = 0;

    virtual std::string getLanguage() const = 0;

    virtual std::string getSourceName() const = 0;

    virtual ~ILanguageRegionsNamesDataSource() {}
};

using ILanguageRegionsNamesDataSourcePtr = std::unique_ptr<ILanguageRegionsNamesDataSource>;


// Provides regions names data sources for different languages
class IRegionsNamesDataProvider
{
public:
    virtual ILanguageRegionsNamesDataSourcePtr getLanguageRegionsNamesSource(
        const std::string& language) const = 0;
    ;

    virtual ~IRegionsNamesDataProvider() {}
};

using IRegionsNamesDataProviderPtr = std::unique_ptr<IRegionsNamesDataProvider>;

