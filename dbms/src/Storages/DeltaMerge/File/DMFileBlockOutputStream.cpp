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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>

namespace DB::DM
{
DMFileBlockOutputStream::DMFileBlockOutputStream(
    const Context & context,
    const DMFilePtr & dmfile,
    const ColumnDefines & write_columns)
    : writer(
        dmfile,
        write_columns,
        context.getFileProvider(),
        context.getWriteLimiter(),
        DMFileWriter::Options{
            CompressionSettings(
                context.getSettingsRef().dt_compression_method,
                context.getSettingsRef().dt_compression_level),
            context.getSettingsRef().min_compress_block_size,
            context.getSettingsRef().max_compress_block_size})
{}

} // namespace DB::DM