#pragma once
#include <common/types.h>

#include <type_traits>

namespace DB
{

#define PAGE_FLAG_NORMAL 0x0
#define PAGE_FLAG_DETACH 0x1

/**
     * Here is what page file look like :
     * +-------------------------------------------------------+
     * |  meta info(pf_meta_info)                              |
     * |  write batch 1(pf_wb_t_pu/pf_wb_t_del/pf_wb_t_ref)    |
     * |  ....                                                 |
     * |  write batch n(pf_wb_t_pu/pf_wb_t_del/pf_wb_t_ref)    |
     * |  meta checksum(meta_info_checksum)                    |
     * +-------------------------------------------------------+
     * 
     * +----------------------------------+
     * |  write batch(pf_wb_t_pu)         |
     * |  filed offset 1(pf_wb_fo)        |
     * |  ... // size is field_offset_len |
     * |  filed offset N(pf_wb_fo)        |
     * |  end of write batch              |
     * +----------------------------------+
     * 
     * Actully, The "bit field" only serves as a decoration. 
     * But if we need defined next generation PageStorage. 
     * The "bit field" can make meta info more compact.
     */
#pragma pack(1)

// page file meta infomation
union PFMetaInfo
{
    struct
    {
        // The meta buffer size
        UInt32 meta_byte_size : 32;

        // The page format version, should be V1 or V2
        UInt32 meta_info_version : 32;

        // The ID of sequence
        UInt64 meta_seq_id : 64;
    } bits;
    UInt8 raws[16];
};

static_assert(sizeof(union PFMetaInfo) == 16, "Invaild size of PFMetaInfo.");

// add in meta info end
using PFMetaInfoChecksum = UInt64;

using PFWriteBatchType = UInt8;

// write batch type:PUT/UPSERT
union PFWriteBatchTypePU
{
    struct
    {
        // Write Batch type, here is PUT or UPSERT
        UInt8 type : 8;

        // The page id
        UInt64 page_id : 64;

        // The file id
        UInt64 file_id : 64;

        // The page level
        UInt32 level : 32;

        // The flag
        UInt32 flag : 32;

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
    UInt8 raws[65];
};
static_assert(sizeof(union PFWriteBatchTypePU) == 65, "Invaild size of PFWriteBatchTypePU.");

// write batch type:DEL
union PFWriteBatchTypeDel
{
    struct
    {
        // Write Batch type, here is DEL
        UInt8 type : 8;

        // The page id
        UInt64 page_id : 64;
    } bits;
    UInt8 raws[9];
};
static_assert(sizeof(union PFWriteBatchTypeDel) == 9, "Invaild size of PFWriteBatchTypeDel.");

// write batch type:REF
union PFWriteBatchTypeRef
{
    struct
    {
        // Write Batch type, here is REF
        UInt8 type : 8;

        // The page id
        UInt64 page_id : 64;

        // The Origin page id
        UInt64 og_page_id : 64;
    } bits;
    UInt8 raws[17];
};
static_assert(sizeof(union PFWriteBatchTypeRef) == 17, "Invaild size of PFWriteBatchTypeRef.");

// field offset
union PFWriteBatchFieldOffset
{
    struct
    {
        // The field offset
        UInt64 field_offset : 64;

        // The field checksum
        UInt64 field_checksum : 64;
    } bits;
    UInt8 raws[16];
};
static_assert(sizeof(union PFWriteBatchFieldOffset) == 16, "Invaild size of PFWriteBatchFieldOffset.");

#pragma pack()
} // namespace DB