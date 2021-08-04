; Copyright (c) 2021 PingCAP Inc 
; Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>
%include "ch_mb_mgr_datastruct.asm"


%ifidn __OUTPUT_FORMAT__, elf64
 ; Linux definitions
     %define arg1 	rdi
     %define arg2	rsi
     %define reg3	rcx
     %define reg4	rdx
%else
 ; Windows definitions
     %define arg1 	rcx
     %define arg2 	rdx
     %define reg3	rsi
     %define reg4	rdi
%endif

; Common definitions
%define STATE    arg1
%define INP_SIZE arg2
%define SIZE	 INP_SIZE ; rsi

%define IDX     rax
%define TBL	reg3

%define inp0 r9
%define inp1 r10
%define inp2 r11
%define inp3 r12
%define inp4 r13
%define inp5 r14
%define inp6 r15
%define inp7 reg4


struc stack_frame
  .data		resb	16*SZ8
  .digest	resb	8*SZ8
  .wbtmp	resb	69*SZ8
  .rsp		resb	8
endstruc


%define FRAMESZ	stack_frame_size
%define _DIGEST	stack_frame.digest
%define _WBTMP	stack_frame.wbtmp
%define _RSP_SAVE	stack_frame.rsp

;;;;;;;;
; Rolling YMM to right shape
;;;;;;;;
%macro TRANSPOSE8 10
%define %%r0 %1
%define %%r1 %2
%define %%r2 %3
%define %%r3 %4
%define %%r4 %5
%define %%r5 %6
%define %%r6 %7
%define %%r7 %8
%define %%t0 %9
%define %%t1 %10
	; process top half (r0..r3) {a...d}
	vshufps	%%t0, %%r0, %%r1, 0x44	; t0 = {b5 b4 a5 a4   b1 b0 a1 a0}
	vshufps	%%r0, %%r0, %%r1, 0xEE	; r0 = {b7 b6 a7 a6   b3 b2 a3 a2}
	vshufps %%t1, %%r2, %%r3, 0x44	; t1 = {d5 d4 c5 c4   d1 d0 c1 c0}
	vshufps	%%r2, %%r2, %%r3, 0xEE	; r2 = {d7 d6 c7 c6   d3 d2 c3 c2}
	vshufps	%%r3, %%t0, %%t1, 0xDD	; r3 = {d5 c5 b5 a5   d1 c1 b1 a1}
	vshufps	%%r1, %%r0, %%r2, 0x88	; r1 = {d6 c6 b6 a6   d2 c2 b2 a2}
	vshufps	%%r0, %%r0, %%r2, 0xDD	; r0 = {d7 c7 b7 a7   d3 c3 b3 a3}
	vshufps	%%t0, %%t0, %%t1, 0x88	; t0 = {d4 c4 b4 a4   d0 c0 b0 a0}

	; use r2 in place of t0
	; process bottom half (r4..r7) {e...h}
	vshufps	%%r2, %%r4, %%r5, 0x44	; r2 = {f5 f4 e5 e4   f1 f0 e1 e0}
	vshufps	%%r4, %%r4, %%r5, 0xEE	; r4 = {f7 f6 e7 e6   f3 f2 e3 e2}
	vshufps %%t1, %%r6, %%r7, 0x44	; t1 = {h5 h4 g5 g4   h1 h0 g1 g0}
	vshufps	%%r6, %%r6, %%r7, 0xEE	; r6 = {h7 h6 g7 g6   h3 h2 g3 g2}
	vshufps	%%r7, %%r2, %%t1, 0xDD	; r7 = {h5 g5 f5 e5   h1 g1 f1 e1}
	vshufps	%%r5, %%r4, %%r6, 0x88	; r5 = {h6 g6 f6 e6   h2 g2 f2 e2}
	vshufps	%%r4, %%r4, %%r6, 0xDD	; r4 = {h7 g7 f7 e7   h3 g3 f3 e3}
	vshufps	%%t1, %%r2, %%t1, 0x88	; t1 = {h4 g4 f4 e4   h0 g0 f0 e0}

	vperm2f128	%%r6, %%r5, %%r1, 0x13	; h6...a6
	vperm2f128	%%r2, %%r5, %%r1, 0x02	; h2...a2
	vperm2f128	%%r5, %%r7, %%r3, 0x13	; h5...a5
	vperm2f128	%%r1, %%r7, %%r3, 0x02	; h1...a1
	vperm2f128	%%r7, %%r4, %%r0, 0x13	; h7...a7
	vperm2f128	%%r3, %%r4, %%r0, 0x02	; h3...a3
	vperm2f128	%%r4, %%t1, %%t0, 0x13	; h4...a4
	vperm2f128	%%r0, %%t1, %%t0, 0x02	; h0...a0
%endmacro


;; void sm3_x8_avx2(CH_ARGS *args, uint64_t bytes);
;; arg 1 : STATE : pointer to input data
;; arg 2 : INP_SIZE  : size of input in blocks
ch_mb_x8_avx2:

    ; save rsp, allocate 32-byte aligned for local variables
	mov	IDX, rsp
	sub	rsp, FRAMESZ
	and	rsp, ~31
	mov	[rsp + _RSP_SAVE], IDX

	lea	TBL,[TABLE]

	;; load the address of each of the 8 message lanes
	;; getting ready to transpose input onto stack
	mov	inp0,[STATE + _args_data_ptr + 0*PTR_SZ]
	mov	inp1,[STATE + _args_data_ptr + 1*PTR_SZ]
	mov	inp2,[STATE + _args_data_ptr + 2*PTR_SZ]
	mov	inp3,[STATE + _args_data_ptr + 3*PTR_SZ]
	mov	inp4,[STATE + _args_data_ptr + 4*PTR_SZ]
	mov	inp5,[STATE + _args_data_ptr + 5*PTR_SZ]
	mov	inp6,[STATE + _args_data_ptr + 6*PTR_SZ]
	mov	inp7,[STATE + _args_data_ptr + 7*PTR_SZ]

	xor	IDX, IDX

    ;;;;;; pre-cal

lloop:
    


align 64
global TABLE
TABLE:
dq 0xc3a5c85cc3a5c85c,0xc3a5c85cc3a5c85c
dq 0xc3a5c85cc3a5c85c,0xc3a5c85cc3a5c85c
dq 0x97cb312797cb3127,0x97cb312797cb3127
dq 0x97cb312797cb3127,0x97cb312797cb3127
dq 0xb492b66fb492b66f,0xb492b66fb492b66f
dq 0xb492b66fb492b66f,0xb492b66fb492b66f
dq 0xbe98f273be98f273,0xbe98f273be98f273
dq 0xbe98f273be98f273,0xbe98f273be98f273
dq 0x9ae16a3b9ae16a3b,0x9ae16a3b9ae16a3b
dq 0x9ae16a3b9ae16a3b,0x9ae16a3b9ae16a3b
dq 0x2f90404f2f90404f,0x2f90404f2f90404f
dq 0x2f90404f2f90404f,0x2f90404f2f90404f
dq 0xc949d7c7c949d7c7,0xc949d7c7c949d7c7
dq 0xc949d7c7c949d7c7,0xc949d7c7c949d7c7
dq 0x509e6557509e6557,0x509e6557509e6557
dq 0x509e6557509e6557,0x509e6557509e6557