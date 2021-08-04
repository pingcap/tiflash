; Copyright (c) 2021 PingCAP Inc 
; Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>
%include "datastruct.asm"

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Define City Hash Out Of Order Data Structures
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

START_FIELDS    ; LANE_DATA
;;;     name            size    align
FIELD   _job_in_lane,   8,      8       ; pointer to job object
END_FIELDS

%assign _LANE_DATA_size _FIELD_OFFSET
%assign _LANE_DATA_align _STRUCT_ALIGN


START_FIELDS    ; CH_ARGS_X8
;;;     name            size    align
FIELD   _digest,        4*8*16,  4       ; transposed digest
FIELD   _data_ptr,      8*16,    8       ; array of pointers to data
END_FIELDS

;; For now , I only have AVX2 machine
;; Also i don't think support SSE is a good idea for me
%assign _CH_ARGS_X8_size	_FIELD_OFFSET
%assign _CH_ARGS_X8_align	_STRUCT_ALIGN

; STS_STATUS
%define STS_UNKNOWN		0
%define STS_BEING_PROCESSED	1
%define STS_COMPLETED		2

START_FIELDS	; CH_JOB

;;;	name				size	align
FIELD	_buffer,			8,	8	; pointer to buffer
FIELD	_len,				8,	8	; length in bytes
FIELD	_result_digest,		8*4,	64	; Digest (output)
FIELD	_status,			4,	4
FIELD	_user_data,			8,	8

%assign _CH_JOB_size	_FIELD_OFFSET
%assign _CH_JOB_align	_STRUCT_ALIGN


START_FIELDS    ; MB_MGR
;;;     name            size    align
FIELD   _args,          _CH_ARGS_X8_size, _CH_ARGS_X8_align
FIELD   _lens,          4*16,    8
FIELD   _unused_lanes,  8,      8
FIELD   _ldata,         _LANE_DATA_size*16, _LANE_DATA_align
FIELD   _num_lanes_inuse, 4,    4
END_FIELDS

%assign _MB_MGR_size    _FIELD_OFFSET
%assign _MB_MGR_align   _STRUCT_ALIGN

_args_digest    equ     _args + _digest
_args_data_ptr  equ     _args + _data_ptr
