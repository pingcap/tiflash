// Copyright 2023 PingCAP, Inc.
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

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <TiDB/Collation/Collator.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

struct NullPresence
{
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const Block &, const ColumnNumbers &);

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each block.
///  * Provide const interface for IFunctionBase (later).
///  * Create one executable function per thread to use caches without synchronization (later).
class IExecutableFunction
{
public:
    virtual ~IExecutableFunction() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    void execute(Block & block, const ColumnNumbers & args, size_t result) const;

protected:
    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const = 0;

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

private:
    bool defaultImplementationForNulls(Block & block, const ColumnNumbers & args, size_t result) const;
    bool defaultImplementationForConstantArguments(Block & block, const ColumnNumbers & args, size_t result) const;
};

using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

/// Function with known arguments and return type.
class IFunctionBase
{
public:
    virtual ~IFunctionBase() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    virtual const DataTypes & getArgumentTypes() const = 0;
    virtual const DataTypePtr & getReturnType() const = 0;

    /// Do preparations and return executable.
    /// sample_block should contain data types of arguments and values of constants, if relevant.
    virtual ExecutableFunctionPtr prepare(const Block & sample_block) const = 0;

    /// both arguments and result are column positions in block.
    virtual void execute(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        return prepare(block)->execute(block, arguments, result);
    }

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisable by two;
      *  concat(x, 'const');
      *  concat(x, 'const', y) where const contain at least one non-numeric character;
      *  concat with FixedString
      *  dictGet... functions takes name of dictionary as its argument,
      *   and some dictionaries could be explicitly defined as injective.
      *
      * It could be used, for example, to remove useless function applications from GROUP BY.
      *
      * Sometimes, function is not really injective, but considered as injective, for purpose of query optimization.
      * For example, toString function is not injective for Float64 data type,
      *  as it returns 'nan' for many different representation of NaNs.
      * But we assume, that it is injective. This could be documented as implementation-specific behaviour.
      *
      * sample_block should contain data types of arguments and values of constants, if relevant.
      */
    virtual bool isInjective(const Block & /*sample_block*/) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool isDeterministic() const { return true; }

    virtual bool isDeterministicInScopeOfQuery() const { return true; }

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    /// The property of monotonicity for a certain range.
    struct Monotonicity
    {
        bool is_monotonic = false; /// Is the function monotonous (nondecreasing or nonincreasing).
        bool is_positive
            = true; /// true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
        bool is_always_monotonic = false; /// Is true if function is monotonic on the whole input range I

        Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true, bool is_always_monotonic_ = false)
            : is_monotonic(is_monotonic_)
            , is_positive(is_positive_)
            , is_always_monotonic(is_always_monotonic_)
        {}
    };

    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity getMonotonicityForRange(
        const IDataType & /*type*/,
        const Field & /*left*/,
        const Field & /*right*/) const
    {
        throw Exception(
            fmt::format("Function {} has no information about its monotonicity.", getName()),
            ErrorCodes::NOT_IMPLEMENTED);
    }
};

using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/// Creates IFunctionBase from argument types list.
class IFunctionBuilder
{
public:
    virtual ~IFunctionBuilder() = default;

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments, const TiDB::TiDBCollatorPtr & collator = nullptr)
        const;

    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const;

    void getLambdaArgumentTypes(DataTypes & arguments) const;

    void checkNumberOfArguments(size_t number_of_arguments) const;

    /// Get the main function name.
    virtual String getName() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const { return false; }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

protected:
    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual void getLambdaArgumentTypesImpl(DataTypes & /*arguments*/) const
    {
        throw Exception(
            "Function " + getName() + " can't have lambda-expressions as arguments",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);
    }

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** If useDefaultImplementationForNulls() is true, then change arguments for getReturnType() and buildImpl():
      *  if some of arguments are Nullable(Nothing) then don't call getReturnType(), call buildImpl() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for getReturnType() function
      *   - wrap getReturnType() result in Nullable type and pass to buildImpl
      *
      * Otherwise build returns buildImpl(arguments, getReturnType(arguments));
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    virtual FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & return_type,
        const TiDB::TiDBCollatorPtr & collator) const
        = 0;
};

using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

/// Old function interface. Check documentation in IFunction.h.
class IFunction
{
public:
    virtual ~IFunction() = default;

    virtual String getName() const = 0;

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const = 0;

    /// Override these functions to change default implementation behavior. See details in IExecutableFunction.
    virtual bool useDefaultImplementationForNulls() const { return true; }
    virtual bool useDefaultImplementationForConstants() const { return false; }
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /// Override these functions to change default implementation behavior. See details in IFunctionBase.
    virtual bool isSuitableForConstantFolding() const { return true; }
    virtual bool isInjective(const Block & /*sample_block*/) const { return false; }
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
    virtual Monotonicity getMonotonicityForRange(
        const IDataType & /*type*/,
        const Field & /*left*/,
        const Field & /*right*/) const
    {
        throw Exception(
            fmt::format("Function {} has no information about its monotonicity.", getName()),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception(fmt::format("getReturnType is not implemented for {}", getName()), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);
    }

    virtual bool isVariadic() const { return false; }

    virtual void getLambdaArgumentTypes(DataTypes & /*arguments*/) const
    {
        throw Exception(
            fmt::format("Function {} can't have lambda-expressions as arguments", getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    virtual void setCollator(const TiDB::TiDBCollatorPtr &) {}
};

/// Wrappers over IFunction.

class DefaultExecutable final : public IExecutableFunction
{
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function)
        : function(std::move(function))
    {}

    String getName() const override { return function->getName(); }

protected:
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const final
    {
        return function->executeImpl(block, arguments, result);
    }
    bool useDefaultImplementationForNulls() const final { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForConstants() const final { return function->useDefaultImplementationForConstants(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final
    {
        return function->getArgumentsThatAreAlwaysConstant();
    }

private:
    std::shared_ptr<IFunction> function;
};

class DefaultFunctionBase final : public IFunctionBase
{
public:
    DefaultFunctionBase(std::shared_ptr<IFunction> function, DataTypes arguments, DataTypePtr return_type)
        : function(std::move(function))
        , arguments(std::move(arguments))
        , return_type(std::move(return_type))
    {}

    String getName() const override { return function->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const Block & /*sample_block*/) const override
    {
        return std::make_shared<DefaultExecutable>(function);
    }

    bool isSuitableForConstantFolding() const override { return function->isSuitableForConstantFolding(); }

    bool isInjective(const Block & sample_block) override { return function->isInjective(sample_block); }

    bool isDeterministic() const override { return function->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return function->hasInformationAboutMonotonicity(); }

    IFunctionBase::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right)
        const override
    {
        return function->getMonotonicityForRange(type, left, right);
    }

private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultFunctionBuilder : public IFunctionBuilder
{
public:
    explicit DefaultFunctionBuilder(std::shared_ptr<IFunction> function)
        : function(std::move(function))
    {}

    String getName() const override { return function->getName(); }
    bool isVariadic() const override { return function->isVariadic(); }
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return function->getReturnTypeImpl(arguments);
    }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return function->getReturnTypeImpl(arguments);
    }

    bool useDefaultImplementationForNulls() const override { return function->useDefaultImplementationForNulls(); }

    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const TiDB::TiDBCollatorPtr & collator) const override
    {
        function->setCollator(collator);

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
    }

    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override
    {
        function->getLambdaArgumentTypes(arguments);
    }

    std::shared_ptr<IFunction> getFunctionImpl() const { return function; }

private:
    std::shared_ptr<IFunction> function;
};

using FunctionPtr = std::shared_ptr<IFunction>;

} // namespace DB
