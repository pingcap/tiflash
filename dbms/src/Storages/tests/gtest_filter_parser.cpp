#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/FailPoint.h>
#include <Databases/DatabaseTiFlash.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/InterpreterDBGInvokeQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDBGInvokeQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
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
            registerStorages();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }

    static void TearDownTestCase()
    {
        // Clean all database from context.
        auto ctx = TiFlashTestEnv::getContext();
        for (const auto & [name, db] : ctx.getDatabases())
        {
            ctx.detachDatabase(name);
            db->shutdown();
        }
        clearPath();
    }

    FilterParser_test()
        : log(&Poco::Logger::get("DatabaseTiFlash_test"))
    {}

    void SetUp() override
    {
        auto ctx = TiFlashTestEnv::getContext();
        auto & storages = ctx.getTMTContext().getStorages();
        auto storage_map = storages.getAllStorage();
        for (auto it = storage_map.begin(); it != storage_map.end(); it++)
        {
            storages.get(it->first)->removeFromTMTContext();
        }
        ctx.getTMTContext().restore();
        recreateMetadataPath();
    }

    void recreateMetadataPath() const
    {
        String path = TiFlashTestEnv::getContext().getPath();

        auto p = path + "/metadata/";

        Poco::File{p}.createDirectory();

        p = path + "/data/";

        Poco::File{p}.createDirectory();
    }

    static void clearPath()
    {
        String path = TiFlashTestEnv::getContext().getPath();
        if (Poco::File file(path); file.exists())
            file.remove(true);
    }

protected:
    Poco::Logger * log;
};

namespace
{
ASTPtr parseDbgInvokeStatement(const String & statement)
{
    ParserDBGInvokeQuery parser;
    const char * pos = statement.data();
    std::string error_msg;
    auto ast = tryParseQuery(parser,
                             pos,
                             pos + statement.size(),
                             error_msg,
                             /*hilite=*/false,
                             String("in ") + __PRETTY_FUNCTION__,
                             /*allow_multi_statements=*/false,
                             0);
    if (!ast)
        throw Exception(error_msg, ErrorCodes::SYNTAX_ERROR);
    return ast;
}
} // namespace

TEST_F(FilterParser_test, SnapshotApply)
{
    auto ctx = TiFlashTestEnv::getContext();
    ctx.getTMTContext().setStatusRunning();

    {
        const String statement = "DBGInvoke __mock_tidb_table(default, test, 'col_1 Int64', '', 'dt')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __refresh_schemas()";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __region_snapshot(4, 0, 10000, default, test)";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __region_snapshot_pre_handle_file(default, test, 4, 3, 12, 'col_1 Int64', '', 1, 'write,default')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __region_snapshot_apply_file(4)";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke dag('select * from default.test')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();

        auto output = interpreter.execute();

        std::string col_1_name("test.col_1");
        Block res = output.in->read();
        for (size_t i = 0; i < 9; i++)
        {
            EXPECT_EQ(res.getByName(col_1_name).column->getInt(i), -3 - i);
        }
    }

    {
        const String statement = "DBGInvoke __drop_tidb_table(default, test)";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __clean_up_region()";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }
}

TEST_F(FilterParser_test, BasicExpression)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();
    ctx.getTMTContext().setStatusRunning();

    {
        const String statement = "DBGInvoke __mock_tidb_table(default, t_111, 'col_1 String, col_2 Int64, col_3 Float64, col_time default \\'asTiDBType|timestamp(5)\\'', '', 'dt')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __refresh_schemas()";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __put_region(4, 0, 100, default, t_111)";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __raft_insert_row(default, t_111, 4, 50, 'test1', 666, 1234567.890123, '2021-10-26 17:00:02.00000')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }

    {
        const String statement = "DBGInvoke __raft_insert_row(default, t_111, 4, 53, 'test3', 666, 1234568.890123, '2021-10-26 17:00:00.00000')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }
    {
        const String statement = "DBGInvoke __raft_insert_row(default, t_111, 4, 51, 'test2', 777, 1234569.890123, '2021-10-26 17:00:00.00000')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);
        interpreter.execute();
    }
    {
        // FilterParser::RSFilterType::Equal
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 = 666')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::Greater
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 > 666')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::Greater
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_3 > 1234568.890123')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::Greater
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 > 666')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::Greater + reverse
        const String statement = "DBGInvoke dag('select * from default.t_111 where 666 < col_2')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::GreaterEqual
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 >= 667')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::GreaterEqual + reverse
        const String statement = "DBGInvoke dag('select * from default.t_111 where 667 <= col_2')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::Less
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 < 777')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::Less + reverse
        const String statement = "DBGInvoke dag('select * from default.t_111 where 777 > col_2')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::LessEuqal
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 <= 776')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::LessEuqal + reverse
        const String statement = "DBGInvoke dag('select * from default.t_111 where 776 >= col_2')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::NotEqual
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_2 != 777')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);

        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(1)), String("test3"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(1), 666);
    }

    {
        // FilterParser::RSFilterType::Not
        const String statement = "DBGInvoke dag('select col_1, col_2 from default.t_111 where NOT col_2=666')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("col_1");
        std::string col_2_name("col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // FilterParser::RSFilterType::And
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_1 = \\'test1\\' and col_2 = 666')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);
    }

    {
        // FilterParser::RSFilterType::OR
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_1 = \\'test5\\' or col_2 = 777')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test2"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 777);
    }

    {
        // TimeStamp + FilterParser::RSFilterType::Equal
        const String statement = "DBGInvoke dag('select * from default.t_111 where col_time > cast_string_datetime(\\'2021-10-26 17:00:00.00000\\')', 4, 'encode_type:default,tz_offset:28800')";
        ASTPtr ast = parseDbgInvokeStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterDBGInvokeQuery interpreter(ast, ctx);

        auto output = interpreter.execute();

        std::string col_1_name("t_111.col_1");
        std::string col_2_name("t_111.col_2");
        Block res = output.in->read();
        EXPECT_EQ(String(res.getByName(col_1_name).column->getDataAt(0)), String("test1"));
        EXPECT_EQ(res.getByName(col_2_name).column->get64(0), 666);
    }
}
CATCH

} // namespace tests
} // namespace DB