#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TiDB.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Common/Stopwatch.h>
#include <Common/Decimal.h>

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
int runDecodeBench(int batch_row_num, int batch_num) {
    std::cout << "batch row num: " << batch_row_num << ", batch num: " << batch_num << std::endl;

    TableInfo table_info;
    ColumnsDescription column_desc;
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
    {
        NameAndTypePair nt{"bill_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"order_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"order_create_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"earliest_order_create_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"latest_order_create_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"assign_site_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_disp_site_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_rec_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_disp_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"three_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receive_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_mobile", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receive_mobile", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_address", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receive_address", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"partner_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_prov_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receive_prov_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_city_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receiv_city_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"send_county_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"receiv_county_id", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"channel_code", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_first_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_last_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"deadline_rec_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_first_center_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_last_center_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_xinglian", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"earliest_rec_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"earliest_rec_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_weight", std::make_shared<DataTypeDecimal<Decimal128>>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"max_rec_weight", std::make_shared<DataTypeDecimal<Decimal128>>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_emp_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_emp_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_next_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_next_site_come_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_next_site_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_transfer_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_come_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_send_car_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_car_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"last_center_transfer_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"last_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"last_center_come_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"last_center_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"max_center_weight", std::make_shared<DataTypeDecimal<Decimal128>>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_emp_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_emp_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"dpm_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"dpm_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"com_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_local_dpm", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"sign_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"sign_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_return", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_problem", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_blocker", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"bill_state", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"lasted_scan_type", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"lasted_scan_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"lasted_scan_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"pre_scan_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"pre_scan_site_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"pre_scan_site_type", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_airway", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_scan_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_scan_type", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_scan_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_guo_guo", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"elec_bill_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"cust_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"cust_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"v1_forecast_sign_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"v2_forecast_sign_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_disp_emp_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_disp_emp_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"elec_create_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_disp_intercept", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"intercept_status", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"actual_intercept_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"actual_intercept_site_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"actual_intercept_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"intercept_emp_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"intercept_emp_name", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"intercept_site_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"intercept_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_rec_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_disp_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"earliest_rec_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"sign_site_league_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"car_code", std::make_shared<DataTypeString>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"district_night_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"suggest_send_car_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_depart_standard_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"first_center_gps_start_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_pull_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_pull_site_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_pull_site_rec_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"second_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"second_center_come_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"second_center_send_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_time", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"next_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"re_order_create_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"time_channal_code", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"forecast_rec_last_center_come_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"site_route_last_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"is_university", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_kpi_transfer_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"rec_kpi_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_first_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_last_center_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_first_center_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_last_center_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"create_time", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"update_time", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"end_time_rec_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_no", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_start_time", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"disp_end_time", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_sign_site_id", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }
    {
        NameAndTypePair nt{"biz_sign_date", std::make_shared<DataTypeUInt64>()};
        name_and_type_list.push_back(nt);
    }

    ColumnID next_column_id = 1;
    {
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "bill_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "order_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "order_create_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "earliest_order_create_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "latest_order_create_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "assign_site_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "forecast_disp_site_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_rec_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_disp_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "three_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receive_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_mobile";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receive_mobile";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_address";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receive_address";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "partner_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_prov_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receive_prov_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_city_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receiv_city_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "send_county_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "receiv_county_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "channel_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_first_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_last_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "deadline_rec_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_first_center_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_last_center_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_xinglian";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "earliest_rec_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "earliest_rec_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<DecimalField<Decimal128>>(next_column_id));
            column_info.name = "rec_weight";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<DecimalField<Decimal128>>(next_column_id));
            column_info.name = "max_rec_weight";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "rec_emp_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "rec_emp_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_next_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_next_site_come_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_next_site_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_transfer_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_come_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "first_center_send_car_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_car_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "last_center_transfer_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "last_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "last_center_come_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "last_center_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<DecimalField<Decimal128>>(next_column_id));
            column_info.name = "max_center_weight";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "disp_emp_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "disp_emp_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "dpm_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "dpm_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "com_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_local_dpm";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "sign_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "sign_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_return";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_problem";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_blocker";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "bill_state";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "lasted_scan_type";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "lasted_scan_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "lasted_scan_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "pre_scan_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "pre_scan_site_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "pre_scan_site_type";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_airway";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_scan_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "first_scan_type";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_scan_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_guo_guo";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "elec_bill_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "cust_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "cust_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "v1_forecast_sign_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "v2_forecast_sign_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "forecast_disp_emp_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "forecast_disp_emp_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "elec_create_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_disp_intercept";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "intercept_status";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "actual_intercept_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "actual_intercept_site_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "actual_intercept_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "intercept_emp_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "intercept_emp_name";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "intercept_site_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "intercept_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_rec_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_disp_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "earliest_rec_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "sign_site_league_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<String>(next_column_id));
            column_info.name = "car_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "district_night_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "suggest_send_car_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_depart_standard_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "first_center_gps_start_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_pull_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_pull_site_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_pull_site_rec_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "second_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "second_center_come_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "second_center_send_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_time";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "next_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "re_order_create_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "time_channal_code";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "forecast_rec_last_center_come_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "site_route_last_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "is_university";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_kpi_transfer_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "rec_kpi_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_first_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_last_center_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_first_center_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_last_center_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "create_time";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "update_time";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "end_time_rec_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_no";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_start_time";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "disp_end_time";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_sign_site_id";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
        {
            ColumnInfo column_info(getColumnInfo<UInt64>(next_column_id));
            column_info.name = "biz_sign_date";
            table_info.columns.emplace_back(column_info);
            next_column_id++;
        }
    }
    column_desc.ordinary = name_and_type_list;

    // create RegionDataReadInfoList for decoding
    RegionDataReadInfoLists data_lists_read;
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
                {
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(std::make_shared<DataTypeDecimal<Decimal128>>()->getDefault());
//                    fields.emplace_back(Field(DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)));

                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(std::make_shared<DataTypeDecimal<Decimal128>>()->getDefault());
//                    fields.emplace_back(Field(DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(std::make_shared<DataTypeDecimal<Decimal128>>()->getDefault());
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    {
                        String value{"hello world; this is a encoding"};
                        fields.emplace_back(Field(value.c_str(), value.size()));
                    }
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                    fields.emplace_back(static_cast<UInt64>(1293489311));
                }

                WriteBufferFromOwnString value_buf;
                encodeRowV2(table_info, fields, value_buf);

                data_list_read.emplace_back(pk, 0, 100 * i, std::make_shared<const TiKVValue>(std::move(value_buf.str())));
            }
        }
        for (int batch_index = 0; batch_index < batch_num; batch_index++)
        {
            data_lists_read.push_back(data_list_read);
        }
    }
    std::cout << "generate data done\n";

    // decode


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
    if (argc < 3) {
        std::cout << "<bin> batch_row_num batch_num\n";
        exit(0);
    }
    int batch_row_num = std::stoi(argv[1]);
    int batch_num = std::stoi(argv[2]);
    return DB::tests::runDecodeBench(batch_row_num, batch_num);
}
