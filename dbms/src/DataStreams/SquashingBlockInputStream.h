#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingTransform.h>
#include <common/logger_useful.h>
#include <Flash/Mpp/MPPHandler.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockInputStream : public IProfilingBlockInputStream
{
public:
    SquashingBlockInputStream(const BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes, std::shared_ptr<MPPTaskLog> mpp_task_log_ = nullptr);

    String getName() const override { return "Squashing"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    SquashingTransform transform;
    bool all_read = false;
    std::shared_ptr<MPPTaskLog> mpp_task_log;
};

}
