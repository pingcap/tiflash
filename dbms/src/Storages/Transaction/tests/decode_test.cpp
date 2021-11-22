#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TiDB.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Common/Stopwatch.h>

#include <iostream>

#include <fmt/core.h>

#include "row_v2_basic_include.h"

using TableInfo = TiDB::TableInfo;
using ColumnInfo = TiDB::ColumnInfo;

namespace DB::tests
{
using RegionDataReadInfoLists = std::vector<RegionDataReadInfoList>;

// TODO: verify typical batch_row_num
int runDecodeBench(int column_num, int batch_row_num, int batch_num) {
    std::cout << "test column num: " << column_num << ", batch row num: " << batch_row_num << ", batch num: " << batch_num << std::endl;

    TableInfo table_info;
    ColumnID first_column_id = 1;
    {
        for (int i = 0; i < column_num; i++) {
            ColumnInfo column_info(getColumnInfo<UInt64>(first_column_id + i));
            column_info.name = fmt::format("a{}", i);
            table_info.columns.emplace_back(column_info);
        }
    }

    // create RegionDataReadInfoList for decoding
    RegionDataReadInfoLists data_lists_read;
    {
        RegionDataReadInfoList data_list_read;
        for (int i = 0; i < batch_row_num; i++) {
            // create PK
            WriteBufferFromOwnString pk_buf;
            DB::EncodeInt64(100 * i, pk_buf);
            RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};

            // create value
            std::vector<Field> fields;
            for (int j = 0; j < column_num; j++) {
                fields.emplace_back(static_cast<UInt64>(100 * j));
            }
            WriteBufferFromOwnString value_buf;
            encodeRowV2(table_info, fields, value_buf);

            data_list_read.emplace_back(pk, 0, 100 * i, std::make_shared<const TiKVValue>(std::move(value_buf.str())));
        }
        for (int batch_index = 0; batch_index < batch_num; batch_index++)
        {
            data_lists_read.push_back(data_list_read);
        }
    }
    std::cout << "generate data done\n";

    DM::ColumnDefines column_defines;
    DM::ColumnDefine handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
    {
        column_defines.emplace_back(handle_column);
        column_defines.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
        column_defines.emplace_back(TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE);
    }
    for (size_t i = 0; i < table_info.columns.size(); i++) {
        auto & column_info = table_info.columns[i];
        column_defines.emplace_back(column_info.id, column_info.name, std::make_shared<DataTypeUInt64>());
    }

    // decode
    {
        Stopwatch stopwatch;
        auto schema_snapshot = std::make_shared<DecodingStorageSchemaSnapshot>(std::make_shared<DM::ColumnDefines>(column_defines), table_info, handle_column, false);

        Block block = createBlockSortByColumnID(schema_snapshot);
        RegionBlockReader reader{schema_snapshot};
        for (int batch_index = 0; batch_index < batch_num; batch_index++) {
//            Block block = createBlockSortByColumnID(schema_snapshot);
            auto decoded = reader.read(block, data_lists_read[batch_index], true);
            assert(block.rows() == (UInt64)batch_row_num);
            assert(decoded == true);
            // clear block data
            size_t orig_rows = block.rows();
            for (size_t i = 0; i < block.columns(); i++) {
                auto * raw_column = const_cast<IColumn *>(block.getByPosition(i).column.get());
                raw_column->popBack(orig_rows);
            }
        }

        auto decode_time = stopwatch.elapsedMilliseconds();
        std::cout << "decode using optimized read cost " << decode_time << " milliseconds\n";
    }

    return 0;
}
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cout << "<bin> column_num batch_row_num batch_num\n";
        exit(0);
    }
    int column_num = std::stoi(argv[1]);
    int batch_row_num = std::stoi(argv[2]);
    int batch_num = std::stoi(argv[3]);
    return DB::tests::runDecodeBench(column_num, batch_row_num, batch_num);
}
