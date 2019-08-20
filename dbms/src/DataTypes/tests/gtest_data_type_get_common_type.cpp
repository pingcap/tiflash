#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>
#include <DataTypes/isLossyCast.h>

#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{

DataTypePtr typeFromString(const String &str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}

DataTypes typesFromString(const String &str)
{
    DataTypes data_types;
    std::istringstream data_types_stream(str);
    std::string data_type;
    while (data_types_stream >> data_type)
        data_types.push_back(typeFromString(data_type));

    return data_types;
}

TEST(DataType_test, getLeastSuperType)
{
    try
    {
        ASSERT_TRUE(getLeastSupertype(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 Int8"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 Int16"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt32 Int64"))->equals(*typeFromString("Int64")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 Float64"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Float64")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Date Date"))->equals(*typeFromString("Date")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Date DateTime"))->equals(*typeFromString("DateTime")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("String FixedString(32) FixedString(8)"))->equals(*typeFromString("String")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Float64)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(Int16))")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(DateTime)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(String)")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nullable(Nothing)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("Nullable(Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nullable(Int16)")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(Int16,Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(UInt8))")));

        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Int8 String")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Int64 UInt64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Float32 UInt64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Float64 Int64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Tuple(Int64) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Tuple(Int64, Int8) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Array(Int64) Array(String)")));
    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        bool print_stack_trace = true;

        auto embedded_stack_trace_pos = text.find("Stack trace");
        if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
            text.resize(embedded_stack_trace_pos);

        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

        if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
        {
            std::cerr << "Stack trace:" << std::endl
                      << e.getStackTrace().toString();
        }

        throw;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
        throw;
    }
    catch (const std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        throw;
    }
    catch (...)
    {
        std::cerr << "Unknown exception" << std::endl;
        throw;
    }
}

TEST(DataType_test, getMostSubtype)
{
    try
    {
        ASSERT_TRUE(getMostSubtype(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 UInt16"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 Int64 UInt64"))->equals(*typeFromString("UInt8")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 Float64"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Int16")));

        ASSERT_TRUE(getMostSubtype(typesFromString("DateTime DateTime"))->equals(*typeFromString("DateTime")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Date DateTime"))->equals(*typeFromString("Date")));

        ASSERT_TRUE(getMostSubtype(typesFromString("String FixedString(8)"))->equals(*typeFromString("FixedString(8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("FixedString(16) FixedString(8)"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(Date)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(UInt8) Nullable(Int8)"))->equals(*typeFromString("Nullable(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) Nullable(Int8)"))->equals(*typeFromString("Nullable(Nothing)")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(UInt8,UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(Nothing))")));

        EXPECT_ANY_THROW(getMostSubtype(typesFromString("Int8 String"), true));
        EXPECT_ANY_THROW(getMostSubtype(typesFromString("Nothing"), true));
        EXPECT_ANY_THROW(getMostSubtype(typesFromString("FixedString(16) FixedString(8) String"), true));

    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        bool print_stack_trace = true;

        auto embedded_stack_trace_pos = text.find("Stack trace");
        if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
            text.resize(embedded_stack_trace_pos);

        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

        if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
        {
            std::cerr << "Stack trace:" << std::endl
                      << e.getStackTrace().toString();
        }

        throw;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
        throw;
    }
    catch (const std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        throw;
    }
    catch (...)
    {
        std::cerr << "Unknown exception" << std::endl;
        throw;
    }
}

TEST(DataType_test, isLossyCast)
{
    try
    {
        // same type is not lossy
        ASSERT_FALSE(isLossyCast(typeFromString("Int8"), typeFromString("Int8")));
        ASSERT_FALSE(isLossyCast(typeFromString("Int16"), typeFromString("Int16")));
        ASSERT_FALSE(isLossyCast(typeFromString("Int32"), typeFromString("Int32")));
        ASSERT_FALSE(isLossyCast(typeFromString("Int64"), typeFromString("Int64")));
        ASSERT_FALSE(isLossyCast(typeFromString("DateTime"), typeFromString("DateTime")));
        ASSERT_FALSE(isLossyCast(typeFromString("Date"), typeFromString("Date")));
        ASSERT_FALSE(isLossyCast(typeFromString("Decimal"), typeFromString("Decimal")));
        ASSERT_FALSE(isLossyCast(typeFromString("String"), typeFromString("String")));
        ASSERT_FALSE(isLossyCast(typeFromString("FixedString(16)"), typeFromString("FixedString(16)")));

        // signed -> unsigned is lossy
        ASSERT_TRUE(isLossyCast(typeFromString("Int8"), typeFromString("UInt8")));
        ASSERT_TRUE(isLossyCast(typeFromString("Int8"), typeFromString("UInt16")));
        ASSERT_TRUE(isLossyCast(typeFromString("Int8"), typeFromString("UInt32")));
        ASSERT_TRUE(isLossyCast(typeFromString("Int8"), typeFromString("UInt64")));

        // unsigned -> signed is lossy
        ASSERT_TRUE(isLossyCast(typeFromString("UInt8"), typeFromString("Int8")));
        ASSERT_TRUE(isLossyCast(typeFromString("UInt8"), typeFromString("Int16")));
        ASSERT_TRUE(isLossyCast(typeFromString("UInt8"), typeFromString("Int32")));
        ASSERT_TRUE(isLossyCast(typeFromString("UInt8"), typeFromString("Int64")));

        // nullable -> not null is ok
        ASSERT_FALSE(isLossyCast(typeFromString("Nullable(UInt32)"), typeFromString("UInt32")));
        ASSERT_FALSE(isLossyCast(typeFromString("Nullable(UInt16)"), typeFromString("UInt32")));
        ASSERT_FALSE(isLossyCast(typeFromString("Nullable(Int32)"), typeFromString("Int64")));

        // not null -> nullable is ok
        ASSERT_FALSE(isLossyCast(typeFromString("UInt32"), typeFromString("Nullable(UInt32)")));
        ASSERT_FALSE(isLossyCast(typeFromString("UInt16"), typeFromString("Nullable(UInt32)")));

        // float32 -> float64 is lossy
        ASSERT_TRUE(isLossyCast(typeFromString("Float32"), typeFromString("Float64")));
        // float64 -> float32 is lossy
        ASSERT_TRUE(isLossyCast(typeFromString("Float64"), typeFromString("Float32")));

        // not support datatime <-> date
        ASSERT_TRUE(isLossyCast(typeFromString("DateTime"), typeFromString("Date")));
        ASSERT_TRUE(isLossyCast(typeFromString("Date"), typeFromString("DateTime")));

        // strings
        ASSERT_FALSE(isLossyCast(typeFromString("FixedString(16)"), typeFromString("FixedString(100)")));
        ASSERT_TRUE(isLossyCast(typeFromString("String"), typeFromString("FixedString(1024)")));
        ASSERT_FALSE(isLossyCast(typeFromString("FixedString(16)"), typeFromString("String")));
    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        bool print_stack_trace = true;

        auto embedded_stack_trace_pos = text.find("Stack trace");
        if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
            text.resize(embedded_stack_trace_pos);

        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

        if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
        {
            std::cerr << "Stack trace:" << std::endl
                      << e.getStackTrace().toString();
        }

        throw;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
        throw;
    }
    catch (const std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        throw;
    }
    catch (...)
    {
        std::cerr << "Unknown exception" << std::endl;
        throw;
    }
}

} // namespace tests
} // namespace DB

