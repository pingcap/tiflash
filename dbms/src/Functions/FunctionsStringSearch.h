#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

/** Search and replace functions in strings:
  *
  * position(haystack, needle)     - the normal search for a substring in a string, returns the position (in bytes) of the found substring starting with 1, or 0 if no substring is found.
  * positionUTF8(haystack, needle) - the same, but the position is calculated at code points, provided that the string is encoded in UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, pattern)        - search by the regular expression LIKE; Returns 0 or 1. Case-insensitive, but only for Latin.
  * notLike(haystack, pattern)
  *
  * match(haystack, pattern)       - search by regular expression re2; Returns 0 or 1.
  *
  * Applies regexp re2 and pulls:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, pattern)
  *
  * replaceOne(haystack, pattern, replacement) - replacing the pattern with the specified rules, only the first occurrence.
  * replaceAll(haystack, pattern, replacement) - replacing the pattern with the specified rules, all occurrences.
  *
  * replaceRegexpOne(haystack, pattern, replacement) - replaces the pattern with the specified regexp, only the first occurrence.
  * replaceRegexpAll(haystack, pattern, replacement) - replaces the pattern with the specified type, all occurrences.
  *
  * Warning! At this point, the arguments needle, pattern, n, replacement must be constants.
  */

static const UInt8 CH_ESCAPE_CHAR = '\\';

template <typename Impl, typename Name, size_t num_args = 2>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr auto has_3_args = (num_args == 3);
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionsStringSearch>();
    }

    String getName() const override
    {
        return name;
    }

    void setCollator(std::shared_ptr<TiDB::ITiDBCollator> collator_) override { collator = collator_; }

    size_t getNumberOfArguments() const override
    {
        return num_args;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (has_3_args && !arguments[2]->isInteger())
            throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        UInt8 escape_char = CH_ESCAPE_CHAR;
        if (has_3_args)
        {
            auto * col_escape_const = typeid_cast<const ColumnConst *>(&*block.getByPosition(arguments[2]).column);
            bool valid_args = true;
            if (col_needle_const == nullptr || col_escape_const == nullptr)
            {
                valid_args = false;
            }
            else
            {
                auto c = col_escape_const->getValue<Int32>();
                if (c < 0 || c > 255)
                {
                    // todo maybe use more strict constraint
                    valid_args = false;
                }
                else
                {
                    escape_char = (UInt8) c;
                }
            }
            if (!valid_args)
            {
                throw Exception("2nd and 3rd arguments of function " + getName() + " must "
                      "be constants, and the 3rd argument must between 0 and 255.");
            }
        }

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            String needle_string = col_needle_const->getValue<String>();
            Impl::constant_constant(col_haystack_const->getValue<String>(), needle_string, escape_char, collator, res);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(col_haystack_const->size(), toField(res));
            return;
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vector_vector(col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                escape_char,
                collator,
                vec_res);
        else if (col_haystack_vector && col_needle_const)
        {
            String needle_string = col_needle_const->getValue<String>();
            Impl::vector_constant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(),
                                  needle_string, escape_char, collator, vec_res);
        }
        else if (col_haystack_const && col_needle_vector)
            Impl::constant_vector(col_haystack_const->getValue<String>(), col_needle_vector->getChars(),
                    col_needle_vector->getOffsets(), escape_char, collator, vec_res);
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " and "
                    + block.getByPosition(arguments[1]).column->getName()
                    + " of arguments of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    std::shared_ptr<TiDB::ITiDBCollator> collator;
};


template <typename Impl, typename Name>
class FunctionsStringSearchToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionsStringSearchToString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), col_needle->getValue<String>(), vec_res, offsets_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
