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
using ColumnIdValues = std::vector<ColumnIDValue<UInt64, false>>;
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
        for (int batch_index = 0; batch_index < batch_num; batch_index++)
        {
            RegionDataReadInfoList data_list_read;
            {
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
            }
            data_lists_read.push_back(data_list_read);
        }
    }
    std::cout << "generate data done\n";

    // decode
    ColumnsDescription column_desc;
    {
        NamesAndTypesList name_and_type_list;
        {
            NameAndTypePair nt{EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
            name_and_type_list.push_back(nt);
        }
        {
            NameAndTypePair nt{VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE};
            name_and_type_list.push_back(nt);
        }
        {
            NameAndTypePair nt{TAG_COLUMN_NAME, TAG_COLUMN_TYPE};
            name_and_type_list.push_back(nt);
        }
        for (int i = 0; i < column_num; i++) {
            NameAndTypePair nt{fmt::format("a{}", i), std::make_shared<DataTypeUInt64>()};
            name_and_type_list.push_back(nt);
        }
        // TODO: verify we just use ordinary
        column_desc.ordinary = name_and_type_list;
    }

//    {
//        Stopwatch stopwatch;
//        RegionBlockReader reader{table_info, column_desc};
//        for (int batch_index = 0; batch_index < batch_num; batch_index++) {
//            auto [block, decoded] = reader.read(data_lists_read[batch_index], true);
//            assert(block.rows() == (UInt64)batch_row_num);
//            assert(decoded == true);
//        }
//        auto decode_time = stopwatch.elapsedMilliseconds();
//        std::cout << "decode using read cost " << decode_time << " milliseconds\n";
//    }

    {
        Stopwatch stopwatch;
        Block block;
        DB::ColumnIDs column_ids;
        ColumnIdToColumnIndexMap column_index_map;
        for (auto & column : column_desc.getAllPhysical())
        {
            auto column_id = table_info.getColumnID(column.name);
            column_ids.insert(column_id);
            block.insert({column.type->createColumn(), column.type, column.name, column_id});
            column_index_map.emplace(column_id, block.columns() - 1);
        }

        RegionBlockReaderOptimized reader{table_info, column_desc};
        for (int batch_index = 0; batch_index < batch_num; batch_index++) {
            auto decoded = reader.read(column_ids, data_lists_read[batch_index], block, column_index_map, true);
            assert(block.rows() == (UInt64)batch_row_num);
            assert(decoded == true);
            // clear block data
            for (size_t i = 0; i < block.columns(); i++) {
                auto * raw_column = const_cast<IColumn *>(block.getByPosition(i).column.get());
                raw_column->popBack(block.rows());
            }
        }
        auto decode_time = stopwatch.elapsedMilliseconds();
        std::cout << "decode using optimized read cost " << decode_time << " milliseconds\n";
    }

//    {
//        RegionBlockReader reader{table_info, column_desc};
//        Stopwatch stopwatch;
//        for (int batch_index = 0; batch_index < batch_num; batch_index++) {
//            auto [block, decoded] = reader.read(data_lists_read[batch_index], true);
//            assert(block.rows() == (UInt64)batch_row_num);
//            assert(decoded == true);
//        }
//        auto decode_time = stopwatch.elapsedMilliseconds();
//        std::cout << "decode using read cost " << decode_time << " milliseconds\n";
//    }
//
//    {
//        Stopwatch stopwatch;
//        Block block;
//        DB::ColumnIDs column_ids;
//        ColumnIdToColumnIndexMap column_index_map;
//        for (auto & column : column_desc.getAllPhysical())
//        {
//            auto column_id = table_info.getColumnID(column.name);
//            column_ids.insert(column_id);
//            block.insert({column.type->createColumn(), column.type, column.name, column_id});
//            column_index_map.emplace(column_id, block.columns() - 1);
//        }
//
//        RegionBlockReaderOptimized reader{table_info, column_desc};
//        for (int batch_index = 0; batch_index < batch_num; batch_index++) {
//            auto decoded = reader.read(column_ids, data_lists_read[batch_index], block, column_index_map, true);
//            assert(block.rows() == (UInt64)batch_row_num);
//            assert(decoded == true);
//            // clear block data
//            for (size_t i = 0; i < block.columns(); i++) {
//                auto * raw_column = const_cast<IColumn *>(block.getByPosition(i).column.get());
//                raw_column->popBack(block.rows());
//            }
//        }
//        auto decode_time = stopwatch.elapsedMilliseconds();
//        std::cout << "decode using optimized read cost " << decode_time << " milliseconds\n";
//    }

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
