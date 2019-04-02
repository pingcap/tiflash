#pragma once

#include <Core/Names.h>
#include <ext/singleton.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MutableSupport : public ext::singleton<MutableSupport>
{
public:
    MutableSupport()
    {
        mutable_hidden.push_back(version_column_name);
        mutable_hidden.push_back(delmark_column_name);
        //mutable_hidden.push_back(tidb_pk_column_name);

        all_hidden.insert(all_hidden.end(), mutable_hidden.begin(), mutable_hidden.end());
    }

    const OrderedNameSet & hiddenColumns(std::string table_type_name)
    {
        if (storage_name == table_type_name || txn_storage_name == table_type_name)
            return mutable_hidden;
        return empty;
    }

    void eraseHiddenColumns(Block & block, std::string table_type_name)
    {
        const OrderedNameSet & names = hiddenColumns(table_type_name);
        for (auto & it : names)
            if (block.has(it))
                block.erase(it);
    }

    static const std::string storage_name;
    static const std::string version_column_name;
    static const std::string delmark_column_name;
    static const std::string tidb_pk_column_name;
    static const std::string txn_storage_name;

    enum DeduperType
    {
        DeduperOriginStreams            = 0,
        DeduperOriginUnity              = 1,
        DeduperReplacingUnity           = 2,
        DeduperReplacingPartitioning    = 3,
        DeduperDedupPartitioning        = 4,
        DeduperReplacingPartitioningOpt = 5,
    };

    static DeduperType toDeduperType(UInt64 type)
    {
        if(type > 5){
            throw Exception("illegal DeduperType: " + toString(type));
        }
        return (DeduperType)type;
    }

    // TODO: detail doc about these delete marks
    struct DelMark
    {
        static const UInt8 NONE = 0x00;
        static const UInt8 INTERNAL_DEL = 0x01;
        static const UInt8 DEFINITE_DEL = (INTERNAL_DEL << 1);
        static const UInt8 DEL_MASK = (INTERNAL_DEL | DEFINITE_DEL);
        static const UInt8 DATA_MASK = ~DEL_MASK;

        static UInt8 getData(UInt8 raw_data)
        {
            return raw_data & DATA_MASK;
        }

        static bool isDel(UInt8 raw_data)
        {
            return raw_data & DEL_MASK;
        }

        static bool isDefiniteDel(UInt8 raw_data)
        {
            return raw_data & DEFINITE_DEL;
        }

        static UInt8 genDelMark(bool internal_del, bool definite_del, UInt8 src_data)
        {
            return (internal_del ? INTERNAL_DEL : NONE) |
                (definite_del ? DEFINITE_DEL : NONE) | getData(src_data);
        }

        static UInt8 genDelMark(bool internal_del, bool definite_del = false)
        {
            return genDelMark(internal_del, definite_del, 0);
        }
    };

private:
    OrderedNameSet empty;
    OrderedNameSet mutable_hidden;
    OrderedNameSet all_hidden;
};

}
