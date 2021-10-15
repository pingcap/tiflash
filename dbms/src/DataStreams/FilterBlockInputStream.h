#pragma once

#include <Columns/FilterDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{
class ExpressionActions;


/** Implements WHERE, HAVING operations.
  * A stream of blocks and an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions, are passed as input.
  * The expression is evaluated and a stream of blocks is returned, which contains only the filtered rows.
  */
class FilterBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    FilterBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & expression_,
        const String & filter_column_name_,
        const LogWithPrefixPtr & log_);

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

    void dumpExtra(std::ostream & ostr) const override;

private:
    ExpressionActionsPtr expression;
    Block header;
    ssize_t filter_column;

    ConstantFilterDescription constant_filter_description;

    const LogWithPrefixPtr log;
};

} // namespace DB
