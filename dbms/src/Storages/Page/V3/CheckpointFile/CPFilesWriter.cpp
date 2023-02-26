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

#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>

namespace DB::PS::V3
{

CPFilesWriter::CPFilesWriter(CPFilesWriter::Options options)
    : manifest_file_id(options.manifest_file_id)
    , data_writer(CPDataFileWriter::create({
          .file_path = options.data_file_path,
          .file_id = options.data_file_id,
      }))
    , manifest_writer(CPManifestFileWriter::create({
          .file_path = options.manifest_file_path,
      }))
    , data_source(options.data_source)
    , log(Logger::get())
{
}

} // namespace DB::PS::V3