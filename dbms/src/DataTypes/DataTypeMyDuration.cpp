#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
DataTypeMyDuration::DataTypeMyDuration(int fsp_)
{
    fsp = fsp_;
    if (fsp < 0 || fsp > 6)
        throw Exception("fsp must >= 0 and <= 6", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

bool DataTypeMyDuration::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && fsp == static_cast<const DataTypeMyDuration &>(rhs).fsp;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<DB::DataTypeMyDuration>(0);

    if (arguments->children.size() != 1)
        throw Exception("MyDuration data type can optionally have only one argument - fsp", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Parameter for MyDuration data type must be uint literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeMyDuration>(arg->value.get<int>());
}

void registerDataTypeDuration(DataTypeFactory & factory)
{
    factory.registerDataType("MyDuration", create, DataTypeFactory::CaseInsensitive);
}

} // namespace DB
