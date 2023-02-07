#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Implements the LIMIT relational operation.
  */
class LimitBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** If always_read_till_end = false (by default), then after reading enough data,
      *  returns an empty block, and this causes the query to be canceled.
      * If always_read_till_end = true - reads all the data to the end, but ignores them. This is necessary in rare cases:
      *  when otherwise, due to the cancellation of the request, we would not have received the data for GROUP BY WITH TOTALS from the remote server.
      */
<<<<<<< HEAD
    LimitBlockInputStream(const BlockInputStreamPtr & input, size_t limit_, size_t offset_, bool always_read_till_end_ = false);
=======
    LimitBlockInputStream(
        const BlockInputStreamPtr & input,
        size_t limit_,
        size_t offset_,
        const String & req_id);
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))

    String getName() const override { return "Limit"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
<<<<<<< HEAD
    size_t limit;
    size_t offset;
    size_t pos = 0;
    bool always_read_till_end;
=======
    LoggerPtr log;
    size_t limit;
    size_t offset;
    /// how many lines were read, including the last read block
    size_t pos = 0;
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))
};

}
