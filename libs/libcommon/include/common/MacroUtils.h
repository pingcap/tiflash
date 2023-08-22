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

#pragma once

#define TF_GET_1ST_ARG(a, ...) a
#define TF_GET_2ND_ARG(a1, a2, ...) a2
#define TF_GET_3RD_ARG(a1, a2, a3, ...) a3
#define TF_GET_4TH_ARG(a1, a2, a3, a4, ...) a4
#define TF_GET_5TH_ARG(a1, a2, a3, a4, a5, ...) a5
#define TF_GET_6TH_ARG(a1, a2, a3, a4, a5, a6, ...) a6
#define TF_GET_7TH_ARG(a1, a2, a3, a4, a5, a6, a7, ...) a7
#define TF_GET_8TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, ...) a8
#define TF_GET_9TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, ...) a9
#define TF_GET_10TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, ...) a10
#define TF_GET_11TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, ...) a11
#define TF_GET_12TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, ...) a12
#define TF_GET_13TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, ...) a13
#define TF_GET_14TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, ...) a14
#define TF_GET_15TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, ...) a15
#define TF_GET_16TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, ...) a16
#define TF_GET_17TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, ...) a17
#define TF_GET_18TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, ...) a18
#define TF_GET_19TH_ARG(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, ...) a19
#define TF_GET_20TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    ...)                 \
    a20
#define TF_GET_21TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    ...)                 \
    a21
#define TF_GET_22TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    ...)                 \
    a22
#define TF_GET_23TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    ...)                 \
    a23
#define TF_GET_24TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    ...)                 \
    a24
#define TF_GET_25TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    a25,                 \
    ...)                 \
    a25
#define TF_GET_26TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    a25,                 \
    a26,                 \
    ...)                 \
    a26
#define TF_GET_27TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    a25,                 \
    a26,                 \
    a27,                 \
    ...)                 \
    a27
#define TF_GET_28TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    a25,                 \
    a26,                 \
    a27,                 \
    a28,                 \
    ...)                 \
    a28
#define TF_GET_29TH_ARG( \
    a1,                  \
    a2,                  \
    a3,                  \
    a4,                  \
    a5,                  \
    a6,                  \
    a7,                  \
    a8,                  \
    a9,                  \
    a10,                 \
    a11,                 \
    a12,                 \
    a13,                 \
    a14,                 \
    a15,                 \
    a16,                 \
    a17,                 \
    a18,                 \
    a19,                 \
    a20,                 \
    a21,                 \
    a22,                 \
    a23,                 \
    a24,                 \
    a25,                 \
    a26,                 \
    a27,                 \
    a28,                 \
    a29,                 \
    ...)                 \
    a29
