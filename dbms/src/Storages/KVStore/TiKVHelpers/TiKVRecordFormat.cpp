// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::RecordKVFormat
{

TiKVKey genKey(const TiDB::TableInfo & table_info, std::vector<Field> keys)
{
    std::string key(RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = encodeInt64(table_info.id);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
    WriteBufferFromOwnString ss;

    for (size_t i = 0; i < keys.size(); i++)
    {
        DB::EncodeDatum(
            keys[i],
            table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset].getCodecFlag(),
            ss);
    }
    return encodeAsTiKVKey(key + ss.releaseStr());
}
} // namespace DB::RecordKVFormat
