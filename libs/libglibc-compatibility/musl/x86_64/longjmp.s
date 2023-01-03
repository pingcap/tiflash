// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* Copyright 2011-2012 Nicholas J. Kain, licensed under standard MIT license */
.global musl_glibc_longjmp
.type musl_glibc_longjmp,@function
musl_glibc_longjmp:
    mov    0x30(%rdi),%r8
    mov    0x8(%rdi),%r9
    mov    0x38(%rdi),%rdx
    ror    $0x11,%r8
    xor    %fs:0x30,%r8     /* this ends up being the stack pointer */
    ror    $0x11,%r9
    xor    %fs:0x30,%r9
    ror    $0x11,%rdx
    xor    %fs:0x30,%rdx    /* this is the instruction pointer */
    mov    (%rdi),%rbx      /* rdi is the jmp_buf, restore regs from it */
    mov    0x10(%rdi),%r12
    mov    0x18(%rdi),%r13
    mov    0x20(%rdi),%r14
    mov    0x28(%rdi),%r15
    mov    %esi,%eax
    mov    %r8,%rsp
    mov    %r9,%rbp
    jmpq   *%rdx            /* goto saved address without altering rsp */
