; Copyright (c) 2021 PingCAP Inc 
; Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>
%include "city_mb_mgr_datastruct.asm"

extern ch_mb_x8_avx2

%ifidn __OUTPUT_FORMAT__, elf64
; Linux register definitions
%define arg1    rdi ; rcx
%define arg2    rsi ; rdx

%define size_offset     rcx ; rdi
%define tmp2            rcx ; rdi

%else
; WINDOWS register definitions
%define arg1    rcx
%define arg2    rdx

%define size_offset     rdi
%define tmp2            rdi

%endif

; Common definitions
%define state   arg1
%define job     arg2
%define len2    arg2
%define p2      arg2

%define idx             r8
%define last_len        r8
%define p               r11
%define start_offset    r11

%define unused_lanes    rbx
%define job_rax         rax
%define len             rax
%define lane            rbp
%define tmp3            rbp
%define tmp             r9
%define lane_data       r10


; STACK_SPACE needs to be an odd multiple of 8
%define STACK_SPACE	8*8 + 16*10 + 8

; STACK_SPACE needs to be an odd multiple of 8
_XMM_SAVE_SIZE  equ 10*16
_GPR_SAVE_SIZE  equ 8*8
_ALIGN_SIZE     equ 8

_XMM_SAVE       equ 0
_GPR_SAVE       equ _XMM_SAVE + _XMM_SAVE_SIZE
STACK_SPACE     equ _GPR_SAVE + _GPR_SAVE_SIZE + _ALIGN_SIZE

; CH_JOB *ch_mb_mgr_submit_avx2(CH_MB_JOB_MGR * state, CH_JOB * job);
; arg 1 : rcx : state
; arg 2 : rdx : job
ch_mb_mgr_submit_avx2:

; Save register into stack
    sub     rsp, STACK_SPACE
	mov     [rsp + _GPR_SAVE + 8*0], rbx
	mov     [rsp + _GPR_SAVE + 8*3], rbp
	mov     [rsp + _GPR_SAVE + 8*4], r12
	mov     [rsp + _GPR_SAVE + 8*5], r13
	mov     [rsp + _GPR_SAVE + 8*6], r14
	mov     [rsp + _GPR_SAVE + 8*7], r15
%ifidn __OUTPUT_FORMAT__, win64
	mov     [rsp + _GPR_SAVE + 8*1], rsi
	mov     [rsp + _GPR_SAVE + 8*2], rdi
	vmovdqa  [rsp + _XMM_SAVE + 16*0], xmm6
	vmovdqa  [rsp + _XMM_SAVE + 16*1], xmm7
	vmovdqa  [rsp + _XMM_SAVE + 16*2], xmm8
	vmovdqa  [rsp + _XMM_SAVE + 16*3], xmm9
	vmovdqa  [rsp + _XMM_SAVE + 16*4], xmm10
	vmovdqa  [rsp + _XMM_SAVE + 16*5], xmm11
	vmovdqa  [rsp + _XMM_SAVE + 16*6], xmm12
	vmovdqa  [rsp + _XMM_SAVE + 16*7], xmm13
	vmovdqa  [rsp + _XMM_SAVE + 16*8], xmm14
	vmovdqa  [rsp + _XMM_SAVE + 16*9], xmm15
%endif
	mov	unused_lanes, [state + _unused_lanes]
	mov	lane, unused_lanes
	and	lane, 0xF
	shr	unused_lanes, 4
	imul	lane_data, lane, _LANE_DATA_size
	mov	dword [job + _status], STS_BEING_PROCESSED
	lea	lane_data, [state + _ldata + lane_data]
	mov	[state + _unused_lanes], unused_lanes
	mov	DWORD(len), [job + _len]

	shl	len, 4
	or	len, lane
	mov	[state + _lens + 4*lane], DWORD(len)

	mov	[lane_data + _job_in_lane], job

	; Load digest words from result_digest
	vmovdqu	xmm0, [job + _result_digest + 0*16]
	vmovdqu xmm1, [job + _result_digest + 1*16]
	vmovd	[state + _args_digest + 4*lane + 0*4*8], xmm0
	vpextrd	[state + _args_digest + 4*lane + 1*4*8], xmm0, 1
	vpextrd	[state + _args_digest + 4*lane + 2*4*8], xmm0, 2
	vpextrd	[state + _args_digest + 4*lane + 3*4*8], xmm0, 3
	vmovd	[state + _args_digest + 4*lane + 4*4*8], xmm1
	vpextrd	[state + _args_digest + 4*lane + 5*4*8], xmm1, 1
	vpextrd	[state + _args_digest + 4*lane + 6*4*8], xmm1, 2
	vpextrd	[state + _args_digest + 4*lane + 7*4*8], xmm1, 3


	mov	p, [job + _buffer]
	mov	[state + _args_data_ptr + 8*lane], p

	add	dword [state + _num_lanes_inuse], 1
	cmp	unused_lanes, 0xf
	jne	return_null

start_loop:
	; Find min length
	vmovdqa xmm0, [state + _lens + 0*16]
	vmovdqa xmm1, [state + _lens + 1*16]

	vpminud xmm2, xmm0, xmm1        ; xmm2 has {D,C,B,A}
	vpalignr xmm3, xmm3, xmm2, 8    ; xmm3 has {x,x,D,C}
	vpminud xmm2, xmm2, xmm3        ; xmm2 has {x,x,E,F}
	vpalignr xmm3, xmm3, xmm2, 4    ; xmm3 has {x,x,x,E}
	vpminud xmm2, xmm2, xmm3        ; xmm2 has min value in low dword

	vmovd   DWORD(idx), xmm2
	mov	len2, idx
	and	idx, 0xF
	shr	len2, 4
	jz	len_is_0

	vpand   xmm2, xmm2, [rel clear_low_nibble]
	vpshufd xmm2, xmm2, 0

	vpsubd  xmm0, xmm0, xmm2
	vpsubd  xmm1, xmm1, xmm2

	vmovdqa [state + _lens + 0*16], xmm0
	vmovdqa [state + _lens + 1*16], xmm1


	; "state" and "args" are the same address, arg1
	; len is arg2
	call	ch_mb_x8_avx2

	; state and idx are intact

len_is_0:
	; process completed job "idx"
	imul	lane_data, idx, _LANE_DATA_size
	lea	lane_data, [state + _ldata + lane_data]

	mov	job_rax, [lane_data + _job_in_lane]
	mov	unused_lanes, [state + _unused_lanes]
	mov	qword [lane_data + _job_in_lane], 0
	mov	dword [job_rax + _status], STS_COMPLETED
	shl	unused_lanes, 4
	or	unused_lanes, idx
	mov	[state + _unused_lanes], unused_lanes

	sub	dword [state + _num_lanes_inuse], 1

	vmovd	xmm0, [state + _args_digest + 4*idx + 0*4*8]
	vpinsrd	xmm0, [state + _args_digest + 4*idx + 1*4*8], 1
	vpinsrd	xmm0, [state + _args_digest + 4*idx + 2*4*8], 2
	vpinsrd	xmm0, [state + _args_digest + 4*idx + 3*4*8], 3
	vmovd	xmm1, [state + _args_digest + 4*idx + 4*4*8]
	vpinsrd	xmm1, [state + _args_digest + 4*idx + 5*4*8], 1
	vpinsrd	xmm1, [state + _args_digest + 4*idx + 6*4*8], 2
	vpinsrd	xmm1, [state + _args_digest + 4*idx + 7*4*8], 3

	vmovdqa	[job_rax + _result_digest + 0*16], xmm0
	vmovdqa	[job_rax + _result_digest + 1*16], xmm1

return:

%ifidn __OUTPUT_FORMAT__, win64
	vmovdqa  xmm6, [rsp + 8*8 + 16*0]
	vmovdqa  xmm7, [rsp + 8*8 + 16*1]
	vmovdqa  xmm8, [rsp + 8*8 + 16*2]
	vmovdqa  xmm9, [rsp + 8*8 + 16*3]
	vmovdqa  xmm10, [rsp + 8*8 + 16*4]
	vmovdqa  xmm11, [rsp + 8*8 + 16*5]
	vmovdqa  xmm12, [rsp + 8*8 + 16*6]
	vmovdqa  xmm13, [rsp + 8*8 + 16*7]
	vmovdqa  xmm14, [rsp + 8*8 + 16*8]
	vmovdqa  xmm15, [rsp + 8*8 + 16*9]
	mov     rsi, [rsp + 8*1]
	mov     rdi, [rsp + 8*2]
%endif
	mov     rbx, [rsp + 8*0]
	mov     rbp, [rsp + 8*3]
	mov     r12, [rsp + 8*4]
	mov     r13, [rsp + 8*5]
	mov     r14, [rsp + 8*6]
	mov     r15, [rsp + 8*7]
	add     rsp, STACK_SPACE

	ret

return_null:
	xor     job_rax, job_rax
	jmp     return

section .data align=16

align 16
clear_low_nibble:
	dq 0x00000000FFFFFFF0, 0x0000000000000000