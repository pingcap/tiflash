#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/TiFlashMetrics.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseTiFlash.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <optional>

namespace DB
{
namespace tests
{
class FilterParser_test : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
            registerAggregateFunctions();
            registerTableFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }

    FilterParser_test()
        : log(&Poco::Logger::get("DatabaseTiFlash_test"))
    {}

protected:
    Poco::Logger * log;

    DM::RSOperatorPtr generateRsOperator(const String & query);
};

DM::RSOperatorPtr FilterParser_test::generateRsOperator(const String & query)
{
    DAGProperties properties = getDAGProperties("");
    auto ctx = TiFlashTestEnv::getContext();
    properties.start_ts = ctx.getTMTContext().getPDClient()->getTS();
    QueryTasks query_tasks;
    std::tie(query_tasks, std::ignore) = compileQuery(
        ctx,
        query,
        [&](const String &, const String &) {
            String table_info_json = R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"col_1","O":"col_1"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":254}},{"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},{"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"col_3","O":"col_3"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":5}},{"comment":"","default":null,"default_bit":null,"id":4,"name":{"L":"col_time","O":"col_time"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":7}}],"comment":"Mocked.","id":30,"index_info":[],"is_common_handle":false,"name":{"L":"t_111","O":"t_111"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654})json";
            TiDB::TableInfo table_info(table_info_json);
            return table_info;
        },
        properties);
    RegionInfoMap regions;
    RegionInfoList retry_regions;
    regions.emplace(4, RegionInfo(4, 0, 0, {}, nullptr));
    auto & dag_request = *query_tasks[0].dag_request;
    DAGContext dag_context(dag_request);
    ctx.setDAGContext(&dag_context);
    DAGQuerySource dag(ctx, regions, retry_regions, dag_request, std::make_shared<LogWithPrefix>(&Poco::Logger::get("CoprocessorHandler"), ""), false);
    auto query_block = *dag.getRootQueryBlock();
    std::vector<const tipb::Expr *> conditions;
    if (query_block.children[0]->selection != nullptr)
    {
        for (auto & condition : query_block.children[0]->selection->selection().conditions())
            conditions.push_back(&condition);
    }
    NamesAndTypes source_columns{{"col_1", std::make_shared<DataTypeString>()},
                                 {"col_2", std::make_shared<DataTypeInt64>()},
                                 {"col_3", std::make_shared<DataTypeInt64>()},
                                 {"col_time", std::make_shared<DataTypeInt64>()}};
    SelectQueryInfo query_info;
    DAGPreparedSets dag_sets;
    query_info.query = makeDummyQuery();
    query_info.dag_query = std::make_unique<DAGQueryInfo>(
        conditions,
        dag_sets,
        source_columns,
        ctx.getTimezoneInfo());
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(ctx.getSettingsRef().resolve_locks, std::numeric_limits<UInt64>::max());

    Names column_names;
    DM::ColumnDefines columns_to_read;
    NamesAndTypesList names_and_types_list{
        {"col_1", std::make_shared<DataTypeString>()},
        {"col_2", std::make_shared<DataTypeInt64>()},
        {"col_3", std::make_shared<DataTypeInt64>()},
        {"col_time", std::make_shared<DataTypeInt64>()},
    };
    DM::ColId cur_col_id = 1;
    for (const auto & name_type : names_and_types_list)
    {
        column_names.push_back(name_type.name);
        columns_to_read.push_back(DM::ColumnDefine(cur_col_id, name_type.name, name_type.type));
        cur_col_id++;
    }
    auto create_attr_by_column_id = [columns_to_read](ColumnID column_id) -> DM::Attr {
        auto iter = std::find_if(
            columns_to_read.begin(),
            columns_to_read.end(),
            [column_id](const DM::ColumnDefine & d) -> bool { return d.id == column_id; });
        if (iter != columns_to_read.end())
            return DM::Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
        return DM::Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };
    auto rs_operator = DM::FilterParser::parseDAGQuery(*query_info.dag_query, columns_to_read, std::move(create_attr_by_column_id), log);
    return rs_operator;
}

TEST_F(FilterParser_test, TestRSOperatorPtr)
{
    auto ctx = TiFlashTestEnv::getContext();
    ctx.getTMTContext().setStatusRunning();

    {
        // FilterParser::RSFilterType::Equal
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 = 666"));
        EXPECT_EQ(rs_operator->name(), String("equal"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}"));
    }

    {
        // FilterParser::RSFilterType::Greater
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 > 666"));
        EXPECT_EQ(rs_operator->name(), String("greater"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"666\"}"));
    }

    {
        // FilterParser::RSFilterType::Greater
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_3 > 1234568.890123"));
        EXPECT_EQ(rs_operator->name(), String("unsupported"));
    }

    {
        // FilterParser::RSFilterType::GreaterEqual
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 >= 667"));
        EXPECT_EQ(rs_operator->name(), String("greater_equal"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}"));
    }

    {
        // FilterParser::RSFilterType::GreaterEqual
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where 667 <= col_2"));
        EXPECT_EQ(rs_operator->name(), String("greater_equal"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}"));
    }

    {
        // FilterParser::RSFilterType::Less
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 < 777"));
        EXPECT_EQ(rs_operator->name(), String("less"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}"));
    }

    {
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where 777 > col_2"));
        EXPECT_EQ(rs_operator->name(), String("less"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}"));
    }

    {
        // FilterParser::RSFilterType::LessEuqal
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 <= 776"));
        EXPECT_EQ(rs_operator->name(), String("less_equal"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"776\"}"));
    }

    {
        // FilterParser::RSFilterType::NotEqual
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_2 != 777"));
        EXPECT_EQ(rs_operator->name(), String("not_equal"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"not_equal\",\"col\":\"col_2\",\"value\":\"777\"}"));
    }

    {
        // FilterParser::RSFilterType::Not
        auto rs_operator = generateRsOperator(String("select col_1, col_2 from default.t_111 where NOT col_2=666"));
        EXPECT_EQ(rs_operator->name(), String("not"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), String("{\"op\":\"not\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}]}"));
    }

    {
        // FilterParser::RSFilterType::And
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_1 = 'test1' and col_2 = 666"));
        EXPECT_EQ(rs_operator->name(), String("and"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
    }

    {
        // FilterParser::RSFilterType::OR
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_1 = 'test5' or col_2 = 777"));
        EXPECT_EQ(rs_operator->name(), String("or"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_2"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
    }

    {
        // TimeStamp + FilterParser::RSFilterType::Equal
        auto rs_operator = generateRsOperator(String("select * from default.t_111 where col_time > cast_string_datetime('2021-10-26 17:00:00.00000')"));
        EXPECT_EQ(rs_operator->name(), String("greater"));
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, String("col_time"));
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 4);
    }
}

} // namespace tests
} // namespace DB