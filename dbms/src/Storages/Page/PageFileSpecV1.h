
#pragma once
#include <common/types.h>

#include <type_traits>

namespace DB
{

#pragma pack(1)

union PFMetaInfoV1
{
    struct
    {
        // The meta buffer size
        UInt32 meta_byte_size : 32;

        // The page format version, should be V1 or V2
        UInt32 meta_info_version : 32;
    } bits;
    UInt8 raws[8];
};
static_assert(sizeof(union PFMetaInfoV1) == 8, "Invaild size of PFMetaInfoV1.");

union PFWriteBatchTypePUV1
{
    struct
    {
        // Write Batch type, here is PUT or UPSERT
        UInt8 type : 8;

        // The page id
        UInt64 page_id : 64;

        // The tag
        UInt64 tag : 64;

        // The page offset, it record data offset in data buffer
        UInt64 page_offset : 64;

        // The page size, records data size
        UInt64 page_size : 64;

        // The page checksum
        UInt64 page_checksum : 64;

    } bits;
    UInt8 raws[41];
};
static_assert(sizeof(union PFWriteBatchTypePUV1) == 41, "Invaild size of PFWriteBatchTypePUV1.");

#pragma pack()
} // namespace DB