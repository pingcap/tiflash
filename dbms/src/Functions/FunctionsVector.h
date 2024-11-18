// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsVector.h>
#include <Functions/IFunction.h>
#include <TiDB/Decode/Vector.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionsCastVectorFloat32AsString : public IFunction
{
public:
    static constexpr auto name = "cast_vector_float32_as_string";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsCastVectorFloat32AsString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(arguments[0].get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_result = ColumnString::create();
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RUNTIME_CHECK(!col_a->isNullAt(i));
            auto data = col_a->getDataAt(i);
            auto v = VectorFloat32Ref(data);
            col_result->insert(v.toString());
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsCastVectorFloat32AsVectorFloat32 : public IFunction
{
public:
    static constexpr auto name = "cast_vector_float32_as_vector_float32";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsCastVectorFloat32AsVectorFloat32>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(arguments[0].get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_result = ColumnArray::create(ColumnFloat32::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RUNTIME_CHECK(!col_a->isNullAt(i));
            auto data = col_a->getDataAt(i);
            auto v = VectorFloat32Ref(data); // Still construct a VectorFloat32Ref to do sanity checks
            UNUSED(v);
            col_result->insertData(data.data, data.size);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecAsText : public IFunction
{
public:
    static constexpr auto name = "vecAsText";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecAsText>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(arguments[0].get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_result = ColumnString::create();
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RUNTIME_CHECK(!col_a->isNullAt(i));
            auto data = col_a->getDataAt(i);
            auto v = VectorFloat32Ref(data);
            col_result->insert(v.toString());
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecDims : public IFunction
{
public:
    static constexpr auto name = "vecDims";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecDims>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(arguments[0].get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt32>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_result = ColumnUInt32::create();
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RUNTIME_CHECK(!col_a->isNullAt(i));
            auto data = col_a->getDataAt(i);
            auto v = VectorFloat32Ref(data);
            col_result->insert(v.size());
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecL1Distance : public IFunction
{
public:
    static constexpr auto name = "vecL1Distance";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecL1Distance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    // Calculating vectors with different dimensions is disallowed, so that we cannot use the default impl.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[0]).get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[1]).get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeFloat64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_b = block.safeGetByPosition(arguments[1]).column;
        auto col_result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (col_a->isNullAt(i) || col_b->isNullAt(i))
            {
                col_result->insertDefault();
                continue;
            }

            auto v1 = VectorFloat32Ref(col_a->getDataAt(i));
            auto v2 = VectorFloat32Ref(col_b->getDataAt(i));
            auto d = v1.l1Distance(v2);
            if (std::isnan(d))
                col_result->insertDefault();
            else
                col_result->insert(d);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecL2Distance : public IFunction
{
public:
    static constexpr auto name = "vecL2Distance";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecL2Distance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    // Calculating vectors with different dimensions is disallowed, so that we cannot use the default impl.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[0]).get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[1]).get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeFloat64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_b = block.safeGetByPosition(arguments[1]).column;
        auto col_result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (col_a->isNullAt(i) || col_b->isNullAt(i))
            {
                col_result->insertDefault();
                continue;
            }

            auto v1 = VectorFloat32Ref(col_a->getDataAt(i));
            auto v2 = VectorFloat32Ref(col_b->getDataAt(i));
            auto d = v1.l2Distance(v2);

            if (std::isnan(d))
                col_result->insertDefault();
            else
                col_result->insert(d);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecCosineDistance : public IFunction
{
public:
    static constexpr auto name = "vecCosineDistance";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecCosineDistance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    // Calculating vectors with different dimensions is disallowed, so that we cannot use the default impl.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[0]).get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[1]).get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeFloat64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_b = block.safeGetByPosition(arguments[1]).column;
        auto col_result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (col_a->isNullAt(i) || col_b->isNullAt(i))
            {
                col_result->insertDefault();
                continue;
            }

            auto v1 = VectorFloat32Ref(col_a->getDataAt(i));
            auto v2 = VectorFloat32Ref(col_b->getDataAt(i));
            auto d = v1.cosineDistance(v2);
            if (std::isnan(d))
                col_result->insertDefault();
            else
                col_result->insert(d);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecNegativeInnerProduct : public IFunction
{
public:
    static constexpr auto name = "vecNegativeInnerProduct";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecNegativeInnerProduct>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    // Calculating vectors with different dimensions is disallowed, so that we cannot use the default impl.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[0]).get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(removeNullable(arguments[1]).get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeFloat64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_b = block.safeGetByPosition(arguments[1]).column;
        auto col_result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (col_a->isNullAt(i) || col_b->isNullAt(i))
            {
                col_result->insertDefault();
                continue;
            }

            auto v1 = VectorFloat32Ref(col_a->getDataAt(i));
            auto v2 = VectorFloat32Ref(col_b->getDataAt(i));
            auto d = v1.innerProduct(v2) * -1;
            if (std::isnan(d))
                col_result->insertDefault();
            else
                col_result->insert(d);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

class FunctionsVecL2Norm : public IFunction
{
public:
    static constexpr auto name = "vecL2Norm";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsVecL2Norm>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!checkDataTypeArray<DataTypeFloat32>(arguments[0].get()))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeFloat64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_a = block.safeGetByPosition(arguments[0]).column;
        auto col_result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
        col_result->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RUNTIME_CHECK(!col_a->isNullAt(i));
            auto v1 = VectorFloat32Ref(col_a->getDataAt(i));
            auto d = v1.l2Norm();
            if (std::isnan(d))
                col_result->insertDefault();
            else
                col_result->insert(d);
        }

        block.safeGetByPosition(result).column = std::move(col_result);
    }
};

} // namespace DB
