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
SpilledFilesInputStream::SpilledFilesInputStream(const std::vector<String> & spilled_files_, const Block & header_)
    : spilled_files(spilled_files_)
    , header(header_)
{
    RUNTIME_CHECK_MSG(!spilled_files.empty(), "Spilled files must not be empty");
    current_reading_file_index = 0;
    current_file_stream = std::make_unique<SpilledFileStream>(spilled_files[0], header);
}

Block SpilledFilesInputStream::readImpl()
{
    Block ret;
    ret = current_file_stream->block_in->read();
    if (ret)
        return ret;
    current_reading_file_index++;
    for (; current_reading_file_index < spilled_files.size(); current_reading_file_index++)
    {
        current_file_stream = std::make_unique<SpilledFileStream>(spilled_files[current_reading_file_index], header);
        ret = current_file_stream->block_in->read();
        if (ret)
            return ret;
    }
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
