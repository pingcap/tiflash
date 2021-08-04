// Copyright (c) 2021 PingCAP Inc
// Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>
#include <stddef.h>
#include <string.h>

#include "city_mb.h"

#ifdef _MSC_VER
#include <intrin.h>
#define inline __inline
#endif

#ifdef __cplusplus
extern "C" {
#endif


void ch_mb_mgr_init_avx2(CH_MB_JOB_MGR * state);
CH_JOB * ch_mb_mgr_submit_avx2(CH_MB_JOB_MGR * state, CH_JOB * job);
CH_JOB * ch_mb_mgr_flush_avx2(CH_MB_JOB_MGR * state);

static inline void hash_init_digest(CH_WORD_T * digest) { memset(digest, 0, sizeof(CH_WORD_T *)); }

void ch_mb_mgr_init_avx2(CH_MB_JOB_MGR * state)
{
    unsigned int j;
    state->unused_lanes = 0xF76543210;
    state->num_lanes_inuse = 0;
    for (j = 0; j < CH_X8_LANES; j++)
    {
        state->lens[j] = 0;
        state->ldata[j].job_in_lane = 0;
    }
}

void chctx_mgr_init_avx2(CH_HASH_CTX_MGR * mgr) { ch_mb_mgr_init_avx2(&mgr->mgr); }

CH_HASH_CTX * sm3_ctx_mgr_submit_avx2(CH_HASH_CTX_MGR * mgr, CH_HASH_CTX * ctx, const void * buffer, uint32_t len, HASH_CTX_FLAG flags)
{
    // Check length is bigger than 64
    // If not bigger than 64 should direct call cityhash64
    if (len < CH_BLOCK_SIZE)
    {
        ctx->error = HASH_CTX_ERROR_INVALID_BUFFER_LEN;
        return ctx;
    }

    if (flags & (~HASH_ENTIRE))
    {
        // User should not pass anything other than FIRST, UPDATE, or LAST
        ctx->error = HASH_CTX_ERROR_INVALID_FLAGS;
        return ctx;
    }

    if (ctx->status & HASH_CTX_STS_PROCESSING)
    {
        // Cannot submit to a currently processing job.
        ctx->error = HASH_CTX_ERROR_ALREADY_PROCESSING;
        return ctx;
    }

    if ((ctx->status & HASH_CTX_STS_COMPLETE) && !(flags & HASH_FIRST))
    {
        // Cannot update a finished job.
        ctx->error = HASH_CTX_ERROR_ALREADY_COMPLETED;
        return ctx;
    }

    if (flags & HASH_FIRST)
    {
        // Init digest
        hash_init_digest(ctx->job.result_digest);

        // Reset byte counter
        ctx->total_length = 0;

        // Clear extra blocks
        ctx->partial_block_buffer_length = 0;
    }
    // If we made it here, there were no errors during this call to submit
    ctx->error = HASH_CTX_ERROR_NONE;

    // Store buffer ptr info from user
    ctx->incoming_buffer = buffer;
    ctx->incoming_buffer_length = len;

    // Store the user's request flags and mark this ctx as currently being processed.
    ctx->status = (flags & HASH_LAST) ? (HASH_CTX_STS)(HASH_CTX_STS_PROCESSING | HASH_CTX_STS_LAST) : HASH_CTX_STS_PROCESSING;

    // Advance byte counter
    ctx->total_length += len;

    ctx->status = (HASH_CTX_STS)(HASH_CTX_STS_PROCESSING | HASH_CTX_STS_COMPLETE);
    ctx->job.buffer = ctx->incoming_buffer;
    ctx->job.len = (uint32_t)ctx->incoming_buffer_length;

    ctx = (CH_HASH_CTX *)ch_mb_mgr_submit_avx2(&mgr->mgr, &ctx->job);
    return ctx;
}

CH_HASH_CTX * ch_ctx_mgr_flush_avx2(CH_HASH_CTX_MGR * mgr)
{
    CH_HASH_CTX * ctx;

    while (1)
    {
        ctx = (CH_HASH_CTX *)ch_mb_mgr_flush_avx2(&mgr->mgr);

        // If flush returned 0, there are no more jobs in flight.
        if (!ctx)
            return NULL;
    }
}


#ifdef __cplusplus
}
#endif