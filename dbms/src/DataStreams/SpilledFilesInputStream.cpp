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

#include <Common/FailPoint.h>
#include <DataStreams/SpilledFilesInputStream.h>

namespace DB
{
namespace FailPoints
{
extern const char random_restore_from_disk_failpoint[];
} // namespace FailPoints

SpilledFilesInputStream::SpilledFilesInputStream(
    std::vector<SpilledFileInfo> && spilled_file_infos_,
    const Block & header_,
    const Block & header_without_constants_,
    const std::vector<size_t> & const_column_indexes_,
    const FileProviderPtr & file_provider_,
    Int64 max_supported_spill_version_)
    : spilled_file_infos(std::move(spilled_file_infos_))
    , header(header_)
    , header_without_constants(header_without_constants_)
    , const_column_indexes(const_column_indexes_)
    , file_provider(file_provider_)
    , max_supported_spill_version(max_supported_spill_version_)
{
    RUNTIME_CHECK_MSG(!spilled_file_infos.empty(), "Spilled files must not be empty");
    current_reading_file_index = 0;
    current_file_stream = std::make_unique<SpilledFileStream>(
        std::move(spilled_file_infos[0]),
        header_without_constants,
        file_provider,
        max_supported_spill_version);
}

Block SpilledFilesInputStream::readImpl()
{
    auto ret = readInternal();
    if likely (ret)
    {
        assert(ret.columns() != 0);
        size_t rows = ret.rows();
        for (const auto index : const_column_indexes)
        {
            const auto & col_type_name = header.getByPosition(index);
            assert(col_type_name.column->isColumnConst());
            ret.insert(index, {col_type_name.column->cloneResized(rows), col_type_name.type, col_type_name.name});
        }
    }
    return ret;
}

Block SpilledFilesInputStream::readInternal()
{
    if (unlikely(current_file_stream == nullptr))
        return {};
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_restore_from_disk_failpoint);
    Block ret = current_file_stream->block_in->read();
    if (ret)
        return ret;

    for (++current_reading_file_index; current_reading_file_index < spilled_file_infos.size();
         ++current_reading_file_index)
    {
        current_file_stream = std::make_unique<SpilledFileStream>(
            std::move(spilled_file_infos[current_reading_file_index]),
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
