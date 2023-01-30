// Copyright 2023 PingCAP, Ltd.
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
#include <Core/SpillConfig.h>
#include <Encryption/FileProvider.h>
#include <Poco/Path.h>

namespace DB
{
namespace
{
bool needReplace(char c)
{
    static String forbidden_or_unusual_chars("\\/:?\"<>|,'*");
    return std::isspace(c) || String::npos != forbidden_or_unusual_chars.find(c);
}
} // namespace
SpillConfig::SpillConfig(const DB::String & spill_dir_, const DB::String & spill_id_, size_t max_spilled_size_per_spill_, const FileProviderPtr & file_provider_)
    : spill_dir(spill_dir_)
    , spill_id(spill_id_)
    , spill_id_as_file_name_prefix(spill_id)
    , max_spilled_size_per_spill(max_spilled_size_per_spill_)
    , file_provider(file_provider_)
{
    RUNTIME_CHECK_MSG(!spill_dir.empty(), "Spiller dir must be non-empty");
    RUNTIME_CHECK_MSG(!spill_id.empty(), "Spiller id must be non-empty");
    if (spill_dir.at(spill_dir.size() - 1) != Poco::Path::separator())
    {
        spill_dir += Poco::Path::separator();
    }
    std::replace_if(spill_id_as_file_name_prefix.begin(), spill_id_as_file_name_prefix.end(), needReplace, '_');
}
} // namespace DB
