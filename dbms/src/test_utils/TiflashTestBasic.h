#pragma once

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Poco/Path.h>
#include <Storages/Transaction/TMTContext.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

#define CATCH                                                                                      \
    catch (const Exception & e)                                                                    \
    {                                                                                              \
        std::string text = e.displayText();                                                        \
                                                                                                   \
        auto embedded_stack_trace_pos = text.find("Stack trace");                                  \
        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;               \
        if (std::string::npos == embedded_stack_trace_pos)                                         \
            std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl; \
                                                                                                   \
        throw;                                                                                     \
    }

/// helper functions for comparing DataType
inline ::testing::AssertionResult DataTypeCompare( //
    const char * lhs_expr,
    const char * rhs_expr,
    const DataTypePtr & lhs,
    const DataTypePtr & rhs)
{
    if (lhs->equals(*rhs))
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs->getName(), rhs->getName(), false);
}
#define ASSERT_DATATYPE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::DataTypeCompare, val1, val2)
#define EXPECT_DATATYPE_EQ(val1, val2) EXPECT_PRED_FORMAT2(::DB::tests::DataTypeCompare, val1, val2)

// A simple helper for getting DataType from type name
inline DataTypePtr typeFromString(const String & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}

class TiFlashTestEnv
{
public:
    static String getTemporaryPath() { return Poco::Path("./tmp/").absolute().toString(); }

    static std::vector<String> getExtraPaths()
    {
        std::vector<String> result;
        result.push_back(getTemporaryPath());
        return result;
    }

    static Context & getContext(const DB::Settings & settings = DB::Settings())
    {
        static Context context = DB::Context::createGlobal();
        context.setPath(getTemporaryPath());
        context.setExtraPaths(getExtraPaths());
        context.setGlobalContext(context);
        try
        {
            context.getTMTContext();
        }
        catch (Exception & e)
        {
            // set itself as global context
            context.setGlobalContext(context);
            context.setApplicationType(DB::Context::ApplicationType::SERVER);

            context.createTMTContext({}, "", "", {"default"}, getTemporaryPath() + "/kvstore", TiDB::StorageEngine::TMT, false);
            context.getTMTContext().restore();
        }
        context.getSettingsRef() = settings;
        return context;
    }
};

} // namespace tests
} // namespace DB
