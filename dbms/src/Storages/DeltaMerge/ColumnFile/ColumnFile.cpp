#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnInMemoryFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnBigFile.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>

namespace DB
{
namespace DM
{
ColumnInMemoryFile * ColumnFile::tryToInMemoryFile()
{
    return !isInMemoryFile() ? nullptr : static_cast<ColumnInMemoryFile *>(this);
}

ColumnTinyFile * ColumnFile::tryToTinyFile()
{
    return !isTinyFile() ? nullptr : static_cast<ColumnTinyFile *>(this);
}

ColumnDeleteRangeFile * ColumnFile::tryToDeleteRange()
{
    return !isDeleteRange() ? nullptr : static_cast<ColumnDeleteRangeFile *>(this);
}

ColumnBigFile * ColumnFile::tryToBigFile()
{
    return !isBigFile() ? nullptr : static_cast<ColumnBigFile *>(this);
}


/// ======================================================
/// Helper methods.
/// ======================================================
size_t copyColumnsData(
    const Columns & from,
    const ColumnPtr & pk_col,
    MutableColumns & to,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    if (range)
    {
        RowKeyColumnContainer rkcc(pk_col, range->is_common_handle);
        if (rows_limit == 1)
        {
            if (range->check(rkcc.getRowKeyValue(rows_offset)))
            {
                for (size_t col_index = 0; col_index < to.size(); ++col_index)
                    to[col_index]->insertFrom(*from[col_index], rows_offset);
                return 1;
            }
            else
            {
                return 0;
            }
        }
        else
        {
            auto [actual_offset, actual_limit] = RowKeyFilter::getPosRangeOfSorted(*range, pk_col, rows_offset, rows_limit);
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], actual_offset, actual_limit);
            return actual_limit;
        }
    }
    else
    {
        if (rows_limit == 1)
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertFrom(*from[col_index], rows_offset);
        }
        else
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], rows_offset, rows_limit);
        }
        return rows_limit;
    }
}

template<class T>
String columnFilesToString(const T & column_files)
{
    String column_files_info = "[";
    for (const auto & f : column_files)
    {
        if (f->isInMemoryFile())
            column_files_info += "B_" + DB::toString(f->getRows()) + "_N,";
        else if (f->isTinyFile())
            column_files_info += "B_" + DB::toString(f->getRows()) + "_S,";
        else if (f->isBigFile())
            column_files_info += "F_" + DB::toString(f->getRows()) + "_S,";
        else if (auto * f_delete = f->tryToDeleteRange(); f_delete)
            column_files_info += "D_" + f_delete->getDeleteRange().toString() + "_S,";
    }

    if (!column_files.empty())
        column_files_info.erase(column_files_info.size() - 1);
    column_files_info += "]";
    return column_files_info;
}

template String columnFilesToString<ColumnFiles>(const ColumnFiles & column_files);
template String columnFilesToString<ColumnStableFiles>(const ColumnStableFiles & column_files);

}
}
