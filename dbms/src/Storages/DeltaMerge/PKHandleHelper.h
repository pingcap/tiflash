#pragma once

#include <Storages/DeltaMerge/PKRange.h>

namespace DB
{
namespace DM {

inline std::pair<size_t, size_t> getPosRangeOfSortedBlock(const PKRange &pk_range,
                                                     const Block & block,
                                                     const size_t offset,
                                                     const size_t limit) {
    auto & columns = block.getColumnsWithTypeAndName();
    std::lower_bound(offset, offset + limit, )


    const auto begin_it = handle_col_data.cbegin() + offset;
    const auto end_it = begin_it + limit;
    const auto first_value = handle_col_data[offset];
    const auto last_value = handle_col_data[offset + limit - 1];

    const auto
        low_it = handle_range.check(first_value) ? begin_it : std::lower_bound(begin_it, end_it, handle_range.start);
    const auto high_it = handle_range.check(last_value) ? end_it : std::lower_bound(low_it, end_it, handle_range.end);

    size_t low_pos = low_it - handle_col_data.cbegin();
    ssize_t res_limit = high_it - low_it;

    return {low_pos, res_limit};
}

}
}