// Copyright (c) 2011 Google, Inc.
// Copyright (c) 2021 PingCAP Inc 
// Author: Jiaqi Zhou<zhoujiaqi@pingcap.com>


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

%macro END_FIELDS 0
%assign _FIELD_OFFSET (_FIELD_OFFSET + _STRUCT_ALIGN-1) & (~ (_STRUCT_ALIGN-1))
%endm

%endif ; end ifdef _DATASTRUCT_ASM_