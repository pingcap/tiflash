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

#include <Dictionaries/Embedded/GeodataProviders/NamesProvider.h>
#include <Dictionaries/Embedded/GeodataProviders/NamesFormatReader.h>

#include <IO/ReadBufferFromFile.h>


bool LanguageRegionsNamesDataSource::isModified() const
{
    return updates_tracker.isModified();
}

size_t LanguageRegionsNamesDataSource::estimateTotalSize() const
{
    return Poco::File(path).getSize();
}

ILanguageRegionsNamesReaderPtr LanguageRegionsNamesDataSource::createReader()
{
    updates_tracker.fixCurrentVersion();
    auto file_reader = std::make_shared<DB::ReadBufferFromFile>(path);
    return std::make_unique<LanguageRegionsNamesFormatReader>(std::move(file_reader));
}

std::string LanguageRegionsNamesDataSource::getLanguage() const
{
    return language;
}

std::string LanguageRegionsNamesDataSource::getSourceName() const
{
    return path;
}


RegionsNamesDataProvider::RegionsNamesDataProvider(const std::string & directory_)
    : directory(directory_)
{}

ILanguageRegionsNamesDataSourcePtr RegionsNamesDataProvider::getLanguageRegionsNamesSource(
    const std::string & language) const
{
    const auto data_file = getDataFilePath(language);
    return std::make_unique<LanguageRegionsNamesDataSource>(data_file, language);
}

std::string RegionsNamesDataProvider::getDataFilePath(const std::string & language) const
{
    return directory + "/regions_names_" + language + ".txt";
}
