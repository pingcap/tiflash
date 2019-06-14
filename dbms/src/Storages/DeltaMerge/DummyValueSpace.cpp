#include <Common/Exception.h>

#include <Storages/DeltaMerge/DummyValueSpace.h>

namespace DB
{
MemoryValueSpace::MemoryValueSpace(String name, const NamesAndTypesList & name_types, const SortDescription & sort_desc)
    : log(&Logger::get("MemoryValueSpace(" + name + ")"))
{

    for (const auto & nt : name_types)
    {
        names_and_types.emplace_back(nt.name, nt.type);
        split_columns.emplace_back(nt.name, nt.type);
    }
    for (const auto & desc : sort_desc)
        sort_column_names.insert(desc.column_name);

    num_columns = split_columns.size();

    LOG_TRACE(log, "MM create");
}

Ids MemoryValueSpace::addFromInsert(const Block & block)
{
    if (unlikely(block.columns() < split_columns.size()))
        throw Exception("Not enough columns!");

    Ids    ids;
    UInt64 id = split_columns.front().size();
    for (size_t i = 0; i < block.rows(); ++i)
        ids.push_back(id++);
    for (auto & split_column : split_columns)
        split_column.append(*block.getByName(split_column.name).column);

    return ids;
}

RefTuples MemoryValueSpace::addFromModify(const Block & block)
{
    if (unlikely(block.columns() < split_columns.size()))
        throw Exception("Not enough columns!");

    RefTuples tuples;
    auto      rows = block.rows();

    std::vector<std::vector<UInt64>> idmap;
    for (size_t column_id = 0; column_id < split_columns.size(); ++column_id)
    {
        auto & split_column = split_columns[column_id];
        auto   offset       = split_column.size();
        if (sort_column_names.find(split_column.name) == sort_column_names.end() && block.has(split_column.name))
        {
            split_column.append(*block.getByName(split_column.name).column);

            idmap.emplace_back();
            auto & ids = idmap[column_id];
            for (size_t row_id = 0; row_id < rows; ++row_id)
                ids.push_back(offset++);
        }
        else
        {
            idmap.emplace_back(rows, INVALID_ID);
        }
    }

    for (size_t row_id = 0; row_id < rows; ++row_id)
    {
        ColumnAndValues values;
        for (size_t column_id = 0; column_id < split_columns.size(); ++column_id)
        {
            auto value_id = idmap[column_id][row_id];
            if (value_id != INVALID_ID)
                values.emplace_back(column_id, value_id);
        }
        tuples.emplace_back(values);
    }

    return tuples;
}

void MemoryValueSpace::removeFromInsert(UInt64 id)
{
    for (size_t i = 0; i < num_columns; ++i)
        split_columns[i].remove(id);
}

void MemoryValueSpace::removeFromModify(UInt64 id, size_t column_id)
{
    split_columns[column_id].remove(id);
}

UInt64 MemoryValueSpace::withModify(UInt64 old_tuple_id, const ValueSpace & modify_value_space, const RefTuple & tuple)
{
    // TODO improvement: in-place update for fixed size type, like numbers.
    for (size_t column_id = 0; column_id < split_columns.size(); ++column_id)
    {
        auto & split_column = split_columns[column_id];
        size_t new_value_id = INVALID_ID;
        for (const auto & cv : tuple.values)
        {
            if (cv.column == column_id)
            {
                new_value_id = cv.value;
                break;
            }
        }
        if (new_value_id != INVALID_ID)
        {
            split_column.append(modify_value_space.split_columns[column_id], new_value_id);
        }
        else
        {
            split_column.append(this->split_columns[column_id], old_tuple_id);
        }
    }

    removeFromInsert(old_tuple_id);

    return split_columns.front().size() - 1;
}

void MemoryValueSpace::gc()
{
    for (auto & sc : split_columns)
        sc.gc();
}

} // namespace DB