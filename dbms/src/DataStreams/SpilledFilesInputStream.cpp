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

#include <DataStreams/SpilledFilesInputStream.h>

namespace DB
{
SpilledFilesInputStream::SpilledFilesInputStream(std::vector<SpilledFileInfo> && spilled_file_infos_, const Block & header_, const FileProviderPtr & file_provider_, Int64 max_supported_spill_version_)
    : spilled_file_infos(std::move(spilled_file_infos_))
    , header(header_)
    , file_provider(file_provider_)
    , max_supported_spill_version(max_supported_spill_version_)
{
    RUNTIME_CHECK_MSG(!spilled_file_infos.empty(), "Spilled files must not be empty");
    current_reading_file_index = 0;
    current_file_stream = std::make_unique<SpilledFileStream>(std::move(spilled_file_infos[0]), header, file_provider, max_supported_spill_version);
}

Block SpilledFilesInputStream::readImpl()
{
    if (unlikely(current_file_stream == nullptr))
        return {};
    Block ret = current_file_stream->block_in->read();
    if (ret)
        return ret;

    for (++current_reading_file_index; current_reading_file_index < spilled_file_infos.size(); ++current_reading_file_index)
    {
        current_file_stream = std::make_unique<SpilledFileStream>(std::move(spilled_file_infos[current_reading_file_index]),
                                                                  header,
                                                                  file_provider,
                                                                  max_supported_spill_version);
        ret = current_file_stream->block_in->read();
        if (ret)
            return ret;
    }
    current_file_stream.reset();
    return ret;
}

Block SpilledFilesInputStream::getHeader() const
{
    return header;
}
String SpilledFilesInputStream::getName() const
{
    return "SpilledFiles";
}

} // namespace DB
