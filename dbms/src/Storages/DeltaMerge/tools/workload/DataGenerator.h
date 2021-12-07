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
    static std::unique_ptr<DataGenerator> create(const WorkloadOptions & opts, const TableInfo & table_info, TimestampGenerator & ts_gen);
    virtual std::tuple<Block, uint64_t> get(uint64_t key) = 0;
    virtual ~DataGenerator() {}
};

std::string blockToString(const Block & block);

} // namespace DB::DM::tests