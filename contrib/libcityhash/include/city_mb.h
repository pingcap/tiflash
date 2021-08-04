// Copyright (c) 2021 PingCAP Inc
// Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>
#ifndef _CH_AVX2_MB_H_
#define _CH_AVX2_MB_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CH_DIGEST_NWORDS 8 /* Word in CH is 32-bit */
#define CH_X8_LANES 8
#define CH_BLOCK_SIZE 64
#define CH_MAX_LANES CH_X8_LANES
#define CH_LOG2_BLOCK_SIZE 6
#define CH_PADLENGTHFIELD_SIZE 8

#define CH_INITIAL_DIGEST 0xc3a5c85c, 0x97cb3127, 0xb492b66f, 0xbe98f273, 0x9ae16a3b, 0x2f90404f, 0xc949d7c7, 0x509e6557

typedef uint32_t ch_digest_array[CH_DIGEST_NWORDS][CH_MAX_LANES];
typedef uint32_t CH_WORD_T;

/**
 * TODO : should not be here
 */
#if defined __unix__ || defined __APPLE__
#define DECLARE_ALIGNED(decl, alignval) decl __attribute__((aligned(alignval)))
#define __forceinline static inline
#define aligned_free(x) free(x)
#else
#ifdef __MINGW32__
#define DECLARE_ALIGNED(decl, alignval) decl __attribute__((aligned(alignval)))
#define posix_memalign(p, algn, len) (NULL == (*((char **)(p)) = (void *)_aligned_malloc(len, algn)))
#define aligned_free(x) _aligned_free(x)
#else
#define DECLARE_ALIGNED(decl, alignval) __declspec(align(alignval)) decl
#define posix_memalign(p, algn, len) (NULL == (*((char **)(p)) = (void *)_aligned_malloc(len, algn)))
#define aligned_free(x) _aligned_free(x)
#endif
#endif

typedef enum
{
    STS_UNKNOWN = 0,         //!< STS_UNKNOWN
    STS_BEING_PROCESSED = 1, //!< STS_BEING_PROCESSED
    STS_COMPLETED = 2,       //!< STS_COMPLETED
    STS_INTERNAL_ERROR,      //!< STS_INTERNAL_ERROR
    STS_ERROR                //!< STS_ERROR
} JOB_STS;

typedef enum
{
    HASH_CTX_STS_IDLE = 0x00,       //!< HASH_CTX_STS_IDLE
    HASH_CTX_STS_PROCESSING = 0x01, //!< HASH_CTX_STS_PROCESSING
    HASH_CTX_STS_LAST = 0x02,       //!< HASH_CTX_STS_LAST
    HASH_CTX_STS_COMPLETE = 0x04,   //!< HASH_CTX_STS_COMPLETE
} HASH_CTX_STS;

typedef enum
{
    HASH_CTX_ERROR_NONE = 0,                //!< HASH_CTX_ERROR_NONE
    HASH_CTX_ERROR_INVALID_FLAGS = -1,      //!< HASH_CTX_ERROR_INVALID_FLAGS
    HASH_CTX_ERROR_ALREADY_PROCESSING = -2, //!< HASH_CTX_ERROR_ALREADY_PROCESSING
    HASH_CTX_ERROR_ALREADY_COMPLETED = -3,  //!< HASH_CTX_ERROR_ALREADY_COMPLETED
    HASH_CTX_ERROR_INVALID_BUFFER_LEN = -4, //!< HASH_CTX_ERROR_INVALID_LEN
} HASH_CTX_ERROR;


typedef enum
{
    HASH_UPDATE = 0x00, //!< HASH_UPDATE
    HASH_FIRST = 0x01,  //!< HASH_FIRST
    HASH_LAST = 0x02,   //!< HASH_LAST
    HASH_ENTIRE = 0x03, //!< HASH_ENTIRE
} HASH_CTX_FLAG;


typedef struct
{
    uint8_t * buffer; //!< pointer to data buffer for this job
    uint64_t len;     //!< length of buffer for this job in blocks.
    DECLARE_ALIGNED(uint32_t result_digest[CH_DIGEST_NWORDS], 64);
    JOB_STS status;   //!< output job status
    void * user_data; //!< pointer for user's job-related data
} CH_JOB;

typedef struct
{
    ch_digest_array digest;
    uint8_t * data_ptr[CH_MAX_LANES];
} CH_MB_ARGS_X8;

typedef struct
{
    CH_JOB * job_in_lane;
} CH_LANE_DATA;


typedef struct
{
    CH_MB_ARGS_X8 args;
    uint32_t lens[CH_MAX_LANES];
    uint64_t unused_lanes; //!< each nibble is index (0...3 or 0...7) of unused lanes, nibble 4 or 8 is set to F as a flag
    CH_LANE_DATA ldata[CH_MAX_LANES];
    uint32_t num_lanes_inuse;
} CH_MB_JOB_MGR;

typedef struct
{
    CH_MB_JOB_MGR mgr;
} CH_HASH_CTX_MGR;


typedef struct
{
    CH_JOB job;                                      // Must be at struct offset 0.
    HASH_CTX_STS status;                             //!< Context status flag
    HASH_CTX_ERROR error;                            //!< Context error flag
    uint64_t total_length;                           //!< Running counter of length processed for this CTX's job
    const void * incoming_buffer;                    //!< pointer to data input buffer for this CTX's job
    uint32_t incoming_buffer_length;                 //!< length of buffer for this job in bytes.
    uint8_t partial_block_buffer[CH_BLOCK_SIZE * 2]; //!< CTX partial blocks
    uint32_t partial_block_buffer_length;
    void * user_data; //!< pointer for user to keep any job-related data
} CH_HASH_CTX;

void ch_ctx_mgr_init(CH_HASH_CTX_MGR * mgr);
CH_HASH_CTX * ch_ctx_mgr_submit(CH_HASH_CTX_MGR * mgr, CH_HASH_CTX * ctx, const void * buffer, uint32_t len, HASH_CTX_FLAG flags);
CH_HASH_CTX * ch_ctx_mgr_flush(CH_HASH_CTX_MGR * mgr);


#ifdef __cplusplus
}
#endif
#endif