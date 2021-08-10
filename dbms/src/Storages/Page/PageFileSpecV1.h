
#pragma once
#include <common/types.h>

#include <type_traits>

namespace DB
{

#pragma pack(1)

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

        // The length of field offset
        UInt64 field_offset_len : 64;
    } bits;
    UInt8 raws[49];
};
static_assert(sizeof(union PFWriteBatchTypePUV1) == 49, "Invaild size of PFWriteBatchTypePU.");

#pragma pack()
} // namespace DB