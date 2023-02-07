#include <DataStreams/LimitBlockInputStream.h>

#include <algorithm>


namespace DB
{
LimitBlockInputStream::LimitBlockInputStream(
    const BlockInputStreamPtr & input,
    size_t limit_,
    size_t offset_,
<<<<<<< HEAD
    const LogWithPrefixPtr & log_,
    bool always_read_till_end_)
    : limit(limit_)
    , offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , log(getMPPTaskLog(log_, NAME))
=======
    const String & req_id)
    : log(Logger::get(req_id))
    , limit(limit_)
    , offset(offset_)
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))
{
    children.push_back(input);
}


Block LimitBlockInputStream::readImpl()
{
    Block res;
    size_t rows = 0;

<<<<<<< HEAD
    /// pos - how many lines were read, including the last read block

=======
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))
    if (pos >= offset + limit)
    {
        if (!always_read_till_end)
            return res;
        else
        {
            while (children.back()->read())
                ;
            return res;
        }
    }

    do
    {
        res = children.back()->read();
        if (!res)
            return res;
        rows = res.rows();
        pos += rows;
    } while (pos <= offset);

    /// give away the whole block
    if (pos >= offset + rows && pos <= offset + limit)
        return res;
<<<<<<< HEAD

    /// give away a piece of the block
    size_t start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    size_t length = std::min(
=======
    }

    do
    {
        res = children.back()->read();
        if (!res)
            return res;
        rows = res.rows();
        pos += rows;
    } while (pos <= offset);

    /// give away the whole block
    if (pos >= offset + rows && pos <= offset + limit)
        return res;

    /// give away a piece of the block
    UInt64 start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    UInt64 length = std::min(
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))
        static_cast<Int64>(limit),
        std::min(
            static_cast<Int64>(pos) - static_cast<Int64>(offset),
            static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));

    for (size_t i = 0; i < res.columns(); ++i)
        res.safeGetByPosition(i).column = res.safeGetByPosition(i).column->cut(start, length);

    return res;
}

<<<<<<< HEAD
=======
void LimitBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", limit = {}", limit);
}
>>>>>>> 3f0dae0d3f (fix the problem that offset in limit query for tiflash system tables doesn't take effect (#6745))
} // namespace DB
