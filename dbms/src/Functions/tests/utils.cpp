#include <Functions/tests/utils.h>

namespace DB
{
Table Table::clone() const { return data; }

void insertColumnDef(Block & block, const String & col_name, const DataTypePtr data_type)
{
    ColumnWithTypeAndName col_with_type_and_name(data_type, col_name);
    block.insert(col_with_type_and_name);
}

void evalFunc(Block & block, const String & func_name, const Strings & args, const String & result)
{
    for (auto & name : block.getNames())
    {
        if (name == result)
            throw Exception("Result column: `" + result + "` has been in the block");
    }
    ColumnsWithTypeAndName args_{};
    ColumnNumbers column_numbers{};
    for (auto & arg : args)
    {
        args_.push_back(block.getByName(arg));
        column_numbers.push_back(block.getPositionByName(arg));
    }
    auto builder = FunctionFactory::instance().get(func_name, tests::TiFlashTestEnv::getContext());
    auto function = builder->build(args_);
    ColumnWithTypeAndName result_column(function->getReturnType(), result);
    block.insert(result_column);
    function->execute(block, column_numbers, block.getPositionByName(result));
}

void formatBlock(const Block & block, String & buff)
{
    WriteBufferFromString wb(buff);

    std::vector<size_t> aligns(block.columns());
    for (auto & col : block)
    {
        writeString(col.name, wb);
        writeChar('(', wb);
        writeString(col.type->getName(), wb);
        writeChar(')', wb);
        writeChar('\t', wb);
    }
    writeChar('\n', wb);
    Field field;
    for (size_t i = 0; i < block.rows(); i++)
    {
        for (auto & col : block)
        {
            col.column->get(i, field);
            String str = applyVisitor(FieldVisitorToString(), field);
            writeString(str, wb);
            writeChar('\t', wb);
        }
        writeChar('\n', wb);
    }
    buff.resize(wb.count());
}

bool operator==(const IColumn & lhs, const IColumn & rhs)
{
    if (lhs.getName() == rhs.getName())
    {
        if (lhs.size() != rhs.size())
        {
            throw Exception("Comparing columns with different size: lhs " + toString(lhs.size()) + ", rhs " + toString(rhs.size()));
        }
        for (size_t i = 0; i < lhs.size(); i++)
        {
            if (lhs.compareAt(i, i, rhs, 1) != 0)
                return false;
        }
        return true;
    }
    return false;
}

std::ostream & operator<<(std::ostream & stream, IColumn const & column)
{
    stream << "[";
    Field buff;
    for (size_t i = 0; i < column.size(); i++)
    {
        if (i > 0)
        {
            stream << " ";
        }
        column.get(i, buff);
        stream << applyVisitor(FieldVisitorToString(), buff);
    }
    stream << "]";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, Block const & block)
{
    String buff;
    formatBlock(block, buff);
    stream << buff;
    return stream;
}

} // namespace DB
