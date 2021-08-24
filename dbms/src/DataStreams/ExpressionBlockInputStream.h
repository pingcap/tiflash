#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/MPPHandler.h>


namespace DB
{

class ExpressionActions;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, std::shared_ptr<MPPTaskLog> mpp_task_log = nullptr);

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    ExpressionActionsPtr expression;
    std::shared_ptr<MPPTaskLog> mpp_task_log;
};

}
