#pragma once
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM::tests
{
struct WorkloadOptions;
class TableInfo;
class KeyGenerator;
class TimestampGenerator;

class DataGenerator
{
public:
    static std::unique_ptr<DataGenerator> create(const WorkloadOptions & opts, const TableInfo & table_info, KeyGenerator & key_gen, TimestampGenerator & ts_gen);
    virtual std::tuple<Block, uint64_t, uint64_t> get() = 0;
    virtual ~DataGenerator() {}
};

std::string blockToString(const Block & block);

} // namespace DB::DM::tests