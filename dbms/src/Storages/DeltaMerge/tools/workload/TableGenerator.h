#pragma once
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM::tests
{
struct WorkloadOptions;

struct TableInfo
{
    TableID table_id;
    std::string db_name;
    std::string table_name;
    DB::DM::ColumnDefinesPtr columns;
    std::vector<int> rowkey_column_indexes;
    DB::DM::ColumnDefine handle;
    bool is_common_handle;
    std::vector<std::string> toStrings() const;
};

class TableGenerator
{
public:
    static std::unique_ptr<TableGenerator> create(const WorkloadOptions & opts);

    virtual TableInfo get() = 0;

    virtual ~TableGenerator() {}
};
} // namespace DB::DM::tests