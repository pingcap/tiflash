#include <TestUtils/TiFlashTestBasic.h>

#include <DataTypes/DataTypeDecimal.h>
#include <Functions/FunctionsArithmetic.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace tests
{

void ASSERT_DecimalDataTypeScaleEq(const DataTypePtr & actual_, ScaleType expected_scale)
{
    if (auto actual = checkDecimal<Decimal32>(*actual_))
        ASSERT_EQ(actual->getScale(), expected_scale);
    else if (auto actual = checkDecimal<Decimal64>(*actual_))
        ASSERT_EQ(actual->getScale(), expected_scale);
    else if (auto actual = checkDecimal<Decimal128>(*actual_))
        ASSERT_EQ(actual->getScale(), expected_scale);
    else if (auto actual = checkDecimal<Decimal256>(*actual_))
        ASSERT_EQ(actual->getScale(), expected_scale);
    else
        ASSERT_TRUE(false) << "type: " + actual_->getName() + " is not decimal!";
}

//  1) If the declared type of both operands of a dyadic arithmetic operator is exact numeric, then the declared
//  type of the result is an implementation-defined exact numeric type, with precision and scale determined as
//  follows:
//    a) Let S1 and S2 be the scale of the first and second operands respectively.
//    b) The precision of the result of addition and subtraction is implementation-defined, and the scale is the
//       maximum of S1 and S2.
//    c) The precision of the result of multiplication is implementation-defined, and the scale is S1 + S2.
//    d) The precision and scale of the result of division are implementation-defined.
TEST(DataTypeDecimal_test, A)
{
    DataTypePtr lhs = createDecimal(10, 4);
    DataTypePtr rhs = createDecimal(10, 6);

    const ScaleType scale_max = std::max(
        typeid_cast<const DataTypeDecimal64 *>(lhs.get())->getScale(), (typeid_cast<const DataTypeDecimal64 *>(rhs.get()))->getScale());
    const ScaleType scale_sum
        = typeid_cast<const DataTypeDecimal64 *>(lhs.get())->getScale() + (typeid_cast<const DataTypeDecimal64 *>(rhs.get()))->getScale();

    Context context = TiFlashTestEnv::getContext();
    DataTypes args{lhs, rhs};

    // Decimal(10, 4) + Decimal(10, 6)
    FunctionPtr func = FunctionPlus::create(context);
    DataTypePtr return_type = func->getReturnTypeImpl(args);
    ASSERT_DecimalDataTypeScaleEq(return_type, scale_max);

    // Decimal(10, 4) - Decimal(10, 6)
    func = FunctionMinus::create(context);
    return_type = func->getReturnTypeImpl(args);
    ASSERT_DecimalDataTypeScaleEq(return_type, scale_max);

    // Decimal(10, 4) * Decimal(10, 6)
    func = FunctionMultiply::create(context);
    return_type = func->getReturnTypeImpl(args);
    ASSERT_DecimalDataTypeScaleEq(return_type, scale_sum);
}

} // namespace tests
} // namespace DB
