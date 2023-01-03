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

#include <Flash/Coprocessor/MockSourceStream.h>

namespace DB
{
std::pair<NamesAndTypes, std::vector<std::shared_ptr<MockTableScanBlockInputStream>>> mockSourceStreamForMpp(Context & context, size_t max_streams, DB::LoggerPtr log, const TiDBTableScan & table_scan)
{
    ColumnsWithTypeAndName columns_with_type_and_name = context.mockStorage()->getColumnsForMPPTableScan(table_scan, context.mockMPPServerInfo().partition_id, context.mockMPPServerInfo().partition_num);
    return cutStreams<MockTableScanBlockInputStream>(context, columns_with_type_and_name, max_streams, log);
}
} // namespace DB
