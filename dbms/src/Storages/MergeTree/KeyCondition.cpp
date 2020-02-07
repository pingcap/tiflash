#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


String Range::toString() const
{
    std::stringstream str;

    if (!left_bounded)
        str << "(-inf, ";
    else
        str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";

    if (!right_bounded)
        str << "+inf)";
    else
        str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
}


/// Example: for `Hello\_World% ...` string it returns `Hello_World`, and for `%test%` returns an empty string.
static String extractFixedPrefixFromLikePattern(const String & like_pattern)
{
    String fixed_prefix;

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
                [[fallthrough]];
            case '_':
                return fixed_prefix;

            case '\\':
                ++pos;
                if (pos == end)
                    break;
                [[fallthrough]];
            default:
                fixed_prefix += *pos;
                break;
        }

        ++pos;
    }

    return fixed_prefix;
}


/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
static String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is 255, then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == 255)
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}


/// A dictionary containing actions to the corresponding functions to turn them into `RPNElement`
const KeyCondition::AtomMap KeyCondition::atom_map
{
    {
        "notEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "equals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "less",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, false);
            return true;
        }
    },
    {
        "greater",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, false);
            return true;
        }
    },
    {
        "lessOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, true);
            return true;
        }
    },
    {
        "greaterOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, true);
            return true;
        }
    },
    {
        "in",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "notIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "like",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>());
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    }
};


inline bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }


FieldWithInfinity::FieldWithInfinity(const Field & field_)
    : field(field_),
    type(Type::NORMAL)
{
}

FieldWithInfinity::FieldWithInfinity(Field && field_)
    : field(std::move(field_)),
    type(Type::NORMAL)
{
}

FieldWithInfinity::FieldWithInfinity(const Type type_)
    : field(),
    type(type_)
{
}

FieldWithInfinity FieldWithInfinity::getMinusInfinity()
{
    return FieldWithInfinity(Type::MINUS_INFINITY);
}

FieldWithInfinity FieldWithInfinity::getPlusinfinity()
{
    return FieldWithInfinity(Type::PLUS_INFINITY);
}

bool FieldWithInfinity::operator<(const FieldWithInfinity & other) const
{
    return type < other.type || (type == other.type && type == Type::NORMAL && field < other.field);
}

bool FieldWithInfinity::operator==(const FieldWithInfinity & other) const
{
    return type == other.type && (type != Type::NORMAL || field == other.field);
}


/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block KeyCondition::getBlockWithConstants(
    const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    const auto expr_for_constant_folding = ExpressionAnalyzer{query, context, nullptr, all_columns}.getConstActions();

    expr_for_constant_folding->execute(result);

    return result;
}


KeyCondition::KeyCondition(
    const SelectQueryInfo & query_info,
    const Context & context,
    const NamesAndTypesList & all_columns,
    const SortDescription & sort_descr_,
    const ExpressionActionsPtr & key_expr_)
    : sort_descr(sort_descr_), key_expr(key_expr_)
{
    for (size_t i = 0; i < sort_descr.size(); ++i)
    {
        std::string name = sort_descr[i].column_name;
        if (!key_columns.count(name))
            key_columns[name] = i;
    }

    if (query_info.fromAST())
    {
        RPNBuilder<ASTPtr, PreparedSets> rpn_builder(key_expr_, key_columns, {});
        PreparedSets sets(query_info.sets);

        /** Evaluation of expressions that depend only on constants.
          * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
          */
        Block block_with_constants = getBlockWithConstants(query_info.query, context, all_columns);

        /// Transform WHERE section to Reverse Polish notation
        const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query_info.query);
        if (select.where_expression)
        {
            rpn_builder.traverseNodeTree(select.where_expression, context, block_with_constants, sets, rpn);

            if (select.prewhere_expression)
            {
                rpn_builder.traverseNodeTree(select.prewhere_expression, context, block_with_constants, sets, rpn);
                rpn.emplace_back(RPNElement::FUNCTION_AND);
            }
        }
        else if (select.prewhere_expression)
            rpn_builder.traverseNodeTree(select.prewhere_expression, context, block_with_constants, sets, rpn);
        else
            rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
    else
    {
        RPNBuilder<tipb::Expr, DAGPreparedSets> rpn_builder(key_expr_, key_columns, query_info.dag_query->source_columns);
        DAGPreparedSets sets(query_info.dag_query->dag_sets);
        const auto & filters = query_info.dag_query->filters;
        if (!filters.empty())
        {
            Block block_with_constants{{DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}};
            for (UInt32 i = 0; i < filters.size(); i++)
            {
                rpn_builder.traverseNodeTree(*filters[i], context, block_with_constants, sets, rpn);
                if (i != 0)
                    rpn.emplace_back(RPNElement::FUNCTION_AND);
            }
        }
        else
            rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
}

bool KeyCondition::addCondition(const String & column, const Range & range)
{
    if (!key_columns.count(column))
        return false;
    rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, key_columns[column], range);
    rpn.emplace_back(RPNElement::FUNCTION_AND);
    return true;
}

String KeyCondition::toString() const
{
    String res;
    for (size_t i = 0; i < rpn.size(); ++i)
    {
        if (i)
            res += ", ";
        res += rpn[i].toString();
    }
    return res;
}


/** Index is the value of key every `index_granularity` rows.
  * This value is called a "mark". That is, the index consists of marks.
  *
  * The key is the tuple.
  * The data is sorted by key in the sense of lexicographic order over tuples.
  *
  * A pair of marks specifies a segment with respect to the order over the tuples.
  * Denote it like this: [ x1 y1 z1 .. x2 y2 z2 ],
  *  where x1 y1 z1 - tuple - value of key in left border of segment;
  *        x2 y2 z2 - tuple - value of key in right boundary of segment.
  * In this section there are data between these marks.
  *
  * Or, the last mark specifies the range open on the right: [ a b c .. + inf )
  *
  * The set of all possible tuples can be considered as an n-dimensional space, where n is the size of the tuple.
  * A range of tuples specifies some subset of this space.
  *
  * Parallelograms (you can also find the term "rail")
  *  will be the subrange of an n-dimensional space that is a direct product of one-dimensional ranges.
  * In this case, the one-dimensional range can be: a period, a segment, an interval, a half-interval, unlimited on the left, unlimited on the right ...
  *
  * The range of tuples can always be represented as a combination of parallelograms.
  * For example, the range [ x1 y1 .. x2 y2 ] given x1 != x2 is equal to the union of the following three parallelograms:
  * [x1]       x [y1 .. +inf)
  * (x1 .. x2) x (-inf .. +inf)
  * [x2]       x (-inf .. y2]
  *
  * Or, for example, the range [ x1 y1 .. +inf ] is equal to the union of the following two parallelograms:
  * [x1]         x [y1 .. +inf)
  * (x1 .. +inf) x (-inf .. +inf)
  * It's easy to see that this is a special case of the variant above.
  *
  * This is important because it is easy for us to check the feasibility of the condition over the parallelogram,
  *  and therefore, feasibility of condition on the range of tuples will be checked by feasibility of condition
  *  over at least one parallelogram from which this range consists.
  */

template <typename F>
static bool forAnyParallelogram(
    size_t key_size,
    const Field * key_left,
    const Field * key_right,
    bool left_bounded,
    bool right_bounded,
    std::vector<Range> & parallelogram,
    size_t prefix_size,
    F && callback)
{
    if (!left_bounded && !right_bounded)
        return callback(parallelogram);

    if (left_bounded && right_bounded)
    {
        /// Let's go through the matching elements of the key.
        while (prefix_size < key_size)
        {
            if (key_left[prefix_size] == key_right[prefix_size])
            {
                /// Point ranges.
                parallelogram[prefix_size] = Range(key_left[prefix_size]);
                ++prefix_size;
            }
            else
                break;
        }
    }

    if (prefix_size == key_size)
        return callback(parallelogram);

    if (prefix_size + 1 == key_size)
    {
        if (left_bounded && right_bounded)
            parallelogram[prefix_size] = Range(key_left[prefix_size], true, key_right[prefix_size], true);
        else if (left_bounded)
            parallelogram[prefix_size] = Range::createLeftBounded(key_left[prefix_size], true);
        else if (right_bounded)
            parallelogram[prefix_size] = Range::createRightBounded(key_right[prefix_size], true);

        return callback(parallelogram);
    }

    /// (x1 .. x2) x (-inf .. +inf)

    if (left_bounded && right_bounded)
        parallelogram[prefix_size] = Range(key_left[prefix_size], false, key_right[prefix_size], false);
    else if (left_bounded)
        parallelogram[prefix_size] = Range::createLeftBounded(key_left[prefix_size], false);
    else if (right_bounded)
        parallelogram[prefix_size] = Range::createRightBounded(key_right[prefix_size], false);

    for (size_t i = prefix_size + 1; i < key_size; ++i)
        parallelogram[i] = Range();

    if (callback(parallelogram))
        return true;

    /// [x1]       x [y1 .. +inf)

    if (left_bounded)
    {
        parallelogram[prefix_size] = Range(key_left[prefix_size]);
        if (forAnyParallelogram(key_size, key_left, key_right, true, false, parallelogram, prefix_size + 1, callback))
            return true;
    }

    /// [x2]       x (-inf .. y2]

    if (right_bounded)
    {
        parallelogram[prefix_size] = Range(key_right[prefix_size]);
        if (forAnyParallelogram(key_size, key_left, key_right, false, true, parallelogram, prefix_size + 1, callback))
            return true;
    }

    return false;
}


bool KeyCondition::mayBeTrueInRange(
    size_t used_key_size,
    const Field * left_key,
    const Field * right_key,
    const DataTypes & data_types,
    bool right_bounded) const
{
    std::vector<Range> key_ranges(used_key_size, Range());

/*  std::cerr << "Checking for: [";
    for (size_t i = 0; i != used_key_size; ++i)
        std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), left_key[i]);
    std::cerr << " ... ";

    if (right_bounded)
    {
        for (size_t i = 0; i != used_key_size; ++i)
            std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), right_key[i]);
        std::cerr << "]\n";
    }
    else
        std::cerr << "+inf)\n";*/

    return forAnyParallelogram(used_key_size, left_key, right_key, true, right_bounded, key_ranges, 0,
        [&] (const std::vector<Range> & key_ranges)
    {
        auto res = mayBeTrueInRangeImpl(key_ranges, data_types);

/*      std::cerr << "Parallelogram: ";
        for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
            std::cerr << (i != 0 ? " x " : "") << key_ranges[i].toString();
        std::cerr << ": " << res << "\n";*/

        return res;
    });
}

std::optional<Range> KeyCondition::applyMonotonicFunctionsChainToRange(
    Range key_range,
    RPNElement::MonotonicFunctionsChain & functions,
    DataTypePtr current_type
)
{
    for (auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        IFunction::Monotonicity monotonicity = func->getMonotonicityForRange(
            *current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// Apply the function.
        DataTypePtr new_type;
        if (!key_range.left.isNull())
            applyFunction(func, current_type, key_range.left, new_type, key_range.left);
        if (!key_range.right.isNull())
            applyFunction(func, current_type, key_range.right, new_type, key_range.right);

        if (!new_type)
        {
            return {};
        }

        current_type.swap(new_type);

        if (!monotonicity.is_positive)
            key_range.swapLeftAndRight();
    }
    return key_range;
}

bool KeyCondition::mayBeTrueInRangeImpl(const std::vector<Range> & key_ranges, const DataTypes & data_types) const
{
    std::vector<BoolMask> rpn_stack;
    for (size_t i = 0; i < rpn.size(); ++i)
    {
        const auto & element = rpn[i];
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            const Range * key_range = &key_ranges[element.key_column];

            /// The case when the column is wrapped in a chain of possibly monotonic functions.
            Range transformed_range;
            if (!element.monotonic_functions_chain.empty())
            {
                std::optional<Range> new_range = applyMonotonicFunctionsChainToRange(
                    *key_range,
                    element.monotonic_functions_chain,
                    data_types[element.key_column]
                );

                if (!new_range)
                {
                    rpn_stack.emplace_back(true, true);
                    continue;
                }
                transformed_range = *new_range;
                key_range = &transformed_range;
            }

            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            rpn_stack.emplace_back(element.set_index->mayBeTrueInRange(key_ranges, data_types));
            if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::mayBeTrueInRange", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}


bool KeyCondition::mayBeTrueInRange(
    size_t used_key_size, const Field * left_key, const Field * right_key, const DataTypes & data_types) const
{
    return mayBeTrueInRange(used_key_size, left_key, right_key, data_types, true);
}

bool KeyCondition::mayBeTrueAfter(
    size_t used_key_size, const Field * left_key, const DataTypes & data_types) const
{
    return mayBeTrueInRange(used_key_size, left_key, nullptr, data_types, false);
}


String RPNElement::toString() const
{
    auto print_wrapped_column = [this](std::ostringstream & ss)
    {
        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
            ss << (*it)->getName() << "(";

        ss << "column " << key_column;

        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
            ss << ")";
    };

    std::ostringstream ss;
    switch (function)
    {
        case FUNCTION_AND:
            return "and";
        case FUNCTION_OR:
            return "or";
        case FUNCTION_NOT:
            return "not";
        case FUNCTION_UNKNOWN:
            return "unknown";
        case FUNCTION_NOT_IN_SET:
        case FUNCTION_IN_SET:
        {
            ss << "(";
            print_wrapped_column(ss);
            ss << (function == FUNCTION_IN_SET ? " in " : " notIn ");
            if (!set_index)
                ss << "unknown size set";
            else
                ss << set_index->size() << "-element set";
            ss << ")";
            return ss.str();
        }
        case FUNCTION_IN_RANGE:
        case FUNCTION_NOT_IN_RANGE:
        {
            ss << "(";
            print_wrapped_column(ss);
            ss << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString();
            ss << ")";
            return ss.str();
        }
        case ALWAYS_FALSE:
            return "false";
        case ALWAYS_TRUE:
            return "true";
        default:
            throw Exception("Unknown function in RPNElement", ErrorCodes::LOGICAL_ERROR);
    }
}


bool KeyCondition::alwaysUnknownOrTrue() const
{
    std::vector<UInt8> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}


size_t KeyCondition::getMaxKeyColumn() const
{
    size_t res = 0;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (element.key_column > res)
                res = element.key_column;
        }
    }
    return res;
}

}
