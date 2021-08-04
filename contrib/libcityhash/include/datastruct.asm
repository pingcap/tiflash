; Copyright (c) 2021 PingCAP Inc 
; Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>

; Macros for defining data structures

; Usage example

;START_FIELDS	; JOB_AES
;;;	name		size	align
;FIELD	_plaintext,	8,	8	; pointer to plaintext
;FIELD	_ciphertext,	8,	8	; pointer to ciphertext
;FIELD	_IV,		16,	8	; IV
;FIELD	_keys,		8,	8	; pointer to keys
;FIELD	_len,		4,	4	; length in bytes
;FIELD	_status,	4,	4	; status enumeration
;FIELD	_user_data,	8,	8	; pointer to user data
;UNION  _union,         size1,  align1, \
;	                size2,  align2, \
;	                size3,  align3, \
;	                ...
;END_FIELDS
;%assign _JOB_AES_size	_FIELD_OFFSET
;%assign _JOB_AES_align	_STRUCT_ALIGN

%ifndef _DATASTRUCT_ASM_
%define _DATASTRUCT_ASM_

;; START_FIELDS
%macro START_FIELDS 0
%assign _FIELD_OFFSET 0
%assign _STRUCT_ALIGN 0
%endm

;; FIELD name size align
%macro FIELD 3
%define %%name  %1
%define %%size  %2
%define %%align %3

%assign _FIELD_OFFSET (_FIELD_OFFSET + (%%align) - 1) & (~ ((%%align)-1))
%%name	equ	_FIELD_OFFSET
%assign _FIELD_OFFSET _FIELD_OFFSET + (%%size)
%if (%%align > _STRUCT_ALIGN)
%assign _STRUCT_ALIGN %%align
%endif
%endm

;; END_FIELDS
%macro END_FIELDS 0
%assign _FIELD_OFFSET (_FIELD_OFFSET + _STRUCT_ALIGN-1) & (~ (_STRUCT_ALIGN-1))
%endm

%endif ; end ifdef _DATASTRUCT_ASM_
