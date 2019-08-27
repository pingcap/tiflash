#include <test_utils/TiflashTestBasic.h>

#include <Interpreters/Context.h>
#include <DataTypes/DataTypeDecimal.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{
namespace tests
{

TEST(DataTypeDecimal_test, A)
{
    DataTypePtr lhs = createDecimal(10, 4);
    DataTypePtr rhs = createDecimal(10, 6);

    const ScaleType scale_max = std::max(typeid_cast<const DataTypeDecimal64 *>(lhs.get())->getScale(), (typeid_cast<const DataTypeDecimal64 *>(rhs.get()))->getScale());
    const ScaleType scale_sum = typeid_cast<const DataTypeDecimal64 *>(lhs.get())->getScale() + (typeid_cast<const DataTypeDecimal64 *>(rhs.get()))->getScale();
    const DataTypePtr expect_add = createDecimal(10, scale_max);
    const DataTypePtr expect_mul = createDecimal(20, scale_sum);

    Context context = TiFlashTestEnv::getContext();
    DataTypes args{lhs, rhs};

    FunctionPtr func = FunctionPlus::create(context);
    DataTypePtr return_type = func->getReturnTypeImpl(args);
    ASSERT_TRUE(return_type->equals(*expect_add)) << "return type:" + return_type->getName() + " expected type: " + expect_add->getName();

    func = FunctionMinus::create(context);
    return_type = func->getReturnTypeImpl(args);
    ASSERT_TRUE(return_type->equals(*expect_add)) << "return type:" + return_type->getName() + " expected type: " + expect_add->getName();

    func = FunctionMultiply::create(context);
    return_type = func->getReturnTypeImpl(args);
    ASSERT_TRUE(return_type->equals(*expect_mul)) << "return type:" + return_type->getName() + " expected type: " + expect_add->getName();

}

} // namespace tests
} // namespace DB
