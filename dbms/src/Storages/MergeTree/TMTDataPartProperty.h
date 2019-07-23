#pragma once

#include <Core/Row.h>

namespace DB
{

class MergeTreeData;

enum TMTDataPartPropertyType : UInt32
{
    END,
    MIN_MAX_PK,
};

struct TMTDataPartProperty
{
    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

public:
    TMTDataPartProperty() = default;

    void load(const MergeTreeData & storage, const String & part_path);
    void store(const MergeTreeData & storage, const String & part_path, Checksums & checksums) const;

    void update(const Block & block, const std::string & pk_name);
    void merge(const TMTDataPartProperty & other);

public:
    Field min_pk;
    Field max_pk;
    bool initialized = false;
};

} // namespace DB
