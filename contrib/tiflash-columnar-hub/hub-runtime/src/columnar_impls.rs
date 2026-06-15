// Copyright 2026 PingCAP, Inc.
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

use std::convert::TryInto;

use kvengine::{CloudColumnarReaders, TableCtx};
use protobuf::{parse_from_bytes, Message};

use crate::{
    build_from_string, build_from_vec_string,
    interfaces_ffi::{
        BaseBuffView, ColumnarReaderErrorType, ColumnarReaderPtr, ColumnarScanStats,
        RaftStoreProxyPtr, RawRustPtr, RawVoidPtr, RustStrWithView, RustStrWithViewVec,
    },
    RawRustPtrType,
};

impl ColumnarReaderPtr {
    unsafe fn as_mut(&mut self) -> &mut CloudColumnarReaders {
        &mut *(self.inner.ptr as *mut CloudColumnarReaders)
    }
}

impl From<RawVoidPtr> for ColumnarReaderPtr {
    fn from(ptr: RawVoidPtr) -> Self {
        Self {
            inner: RawRustPtr {
                ptr,
                type_: RawRustPtrType::ColumnarReader.into(),
            },
            error: RustStrWithView::default(),
            error_type: ColumnarReaderErrorType::OK,
        }
    }
}

impl From<crate::Error> for ColumnarReaderPtr {
    fn from(error: crate::Error) -> Self {
        let inner = RawRustPtr::default();
        match error {
            crate::Error::RegionError(err) => Self {
                inner,
                error: build_from_string(err.write_to_bytes().unwrap()),
                error_type: ColumnarReaderErrorType::RegionError,
            },
            crate::Error::KeyIsLocked(lock) => Self {
                inner,
                error: build_from_string(lock.write_to_bytes().unwrap()),
                error_type: ColumnarReaderErrorType::LockedError,
            },
            crate::Error::PdClientError(err) => Self {
                inner,
                error: build_from_string(err.to_string().into_bytes()),
                error_type: ColumnarReaderErrorType::PdClientError,
            },
            crate::Error::Other(err) => Self {
                inner,
                error: build_from_string(err.into_bytes()),
                error_type: ColumnarReaderErrorType::Other,
            },
        }
    }
}

impl From<kvengine::table::columnar::ColumnarRuntimeStats> for ColumnarScanStats {
    fn from(stats: kvengine::table::columnar::ColumnarRuntimeStats) -> Self {
        Self {
            mvcc_input_rows: stats.mvcc_input_rows,
            mvcc_input_bytes: stats.mvcc_input_bytes,
            mvcc_output_rows: stats.mvcc_output_rows,
            read_block_ns: stats.read_block_ns,
            serialize_ns: stats.serialize_ns,
            init_reader_ns: stats.init_reader_ns,
            prefetch_ns: stats.prefetch_ns,
            rough_check_total_packs: stats.rough_check_total_packs,
            rough_check_selected_packs: stats.rough_check_selected_packs,
            rough_check_skipped_packs: stats.rough_check_skipped_packs,
            rough_check_unknown_packs: stats.rough_check_unknown_packs,
            remote_segments: stats.remote_segments,
            total_segments: stats.total_segments,
        }
    }
}

pub unsafe extern "C" fn ffi_get_region_bucket_keys(
    region_id: u64,
    region_ver: u64,
    hub_ptr: RaftStoreProxyPtr,
) -> RustStrWithViewVec {
    let hub = hub_ptr.as_ref();
    let bucket_keys = hub
        .cloud_helper
        .get_region_bucket_keys(region_id, region_ver);
    if bucket_keys.is_empty() {
        RustStrWithViewVec::default()
    } else {
        build_from_vec_string(bucket_keys)
    }
}

pub unsafe extern "C" fn ffi_clear_shared_snap_access_by_start_ts(
    start_ts: u64,
    hub_ptr: RaftStoreProxyPtr,
) {
    let hub = hub_ptr.as_ref();
    hub.cloud_helper
        .clear_shared_snap_access_by_start_ts(start_ts);
}

pub unsafe extern "C" fn ffi_make_columnar_reader(
    shard_id: u64,
    shard_ver: u64,
    start_ts: u64,
    tables_range_view: BaseBuffView,
    columns: BaseBuffView,
    table_scan: BaseBuffView,
    filter_conditions: BaseBuffView,
    ann_query_info: BaseBuffView,
    fts_query_info: BaseBuffView,
    hub_ptr: RaftStoreProxyPtr,
) -> ColumnarReaderPtr {
    let mut cols_pb = tipb::TableInfo::default();
    cols_pb.merge_from_bytes(columns.to_slice()).unwrap();
    let columns = cols_pb.take_columns();

    let mut table_scan_pb = tipb::Executor::default();
    table_scan_pb
        .merge_from_bytes(table_scan.to_slice())
        .unwrap();

    let filter_conditions_bytes = filter_conditions.to_slice();
    let mut filter_conditions_pb = vec![];
    let mut offset = 0;
    while offset < filter_conditions_bytes.len() {
        let data_len = u32::from_le_bytes(
            filter_conditions_bytes[offset..offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += 4;
        let expr =
            parse_from_bytes::<tipb::Expr>(&filter_conditions_bytes[offset..offset + data_len])
                .unwrap();
        offset += data_len;
        filter_conditions_pb.push(expr);
    }

    let parse_table_ranges = |ranges_bytes: &[u8]| -> Vec<tipb::KeyRange> {
        let mut ranges_pb = vec![];
        let mut offset = 0;
        while offset < ranges_bytes.len() {
            let data_len =
                u32::from_le_bytes(ranges_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let range =
                parse_from_bytes::<tipb::KeyRange>(&ranges_bytes[offset..offset + data_len])
                    .unwrap();
            offset += data_len;
            ranges_pb.push(range);
        }
        ranges_pb.sort_by(|a, b| a.get_low().cmp(&b.get_low()));
        ranges_pb
    };

    let mut tables = vec![];
    offset = 0;
    while offset < tables_range_view.to_slice().len() {
        let table_id = i64::from_le_bytes(
            tables_range_view.to_slice()[offset..offset + 8]
                .try_into()
                .unwrap(),
        );
        offset += 8;
        let ranges_data_len = u32::from_le_bytes(
            tables_range_view.to_slice()[offset..offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += 4;
        let ranges_bytes = &tables_range_view.to_slice()[offset..offset + ranges_data_len];
        offset += ranges_data_len;
        tables.push(TableCtx {
            table_id,
            ranges: parse_table_ranges(ranges_bytes),
        });
    }
    tables.sort_by(|a, b| a.table_id.cmp(&b.table_id));

    let mut ann_query_info_pb = tipb::AnnQueryInfo::default();
    ann_query_info_pb
        .merge_from_bytes(ann_query_info.to_slice())
        .unwrap();

    let mut fts_query_info_pb = tipb::FtsQueryInfo::default();
    fts_query_info_pb
        .merge_from_bytes(fts_query_info.to_slice())
        .unwrap();

    let hub = hub_ptr.as_ref();
    match hub.cloud_helper.make_columnar_reader(
        shard_id,
        shard_ver,
        start_ts,
        tables,
        &columns,
        table_scan_pb,
        filter_conditions_pb,
        ann_query_info_pb,
        fts_query_info_pb,
    ) {
        Ok(reader) => (Box::into_raw(Box::new(reader)) as RawVoidPtr).into(),
        Err(err) => err.into(),
    }
}

pub unsafe extern "C" fn ffi_read_block(mut reader: ColumnarReaderPtr, limit: u64) -> u64 {
    match reader.as_mut().ffi_read_block(limit as usize) {
        Ok(rows) => rows as u64,
        Err(err) => {
            error!("ffi_read_block failed, limit={}: {}", limit, err);
            u64::MAX
        }
    }
}

pub unsafe extern "C" fn ffi_read_handle(mut reader: ColumnarReaderPtr) -> RustStrWithView {
    build_from_string(reader.as_mut().ffi_read_handle())
}

pub unsafe extern "C" fn ffi_read_version(mut reader: ColumnarReaderPtr) -> RustStrWithView {
    build_from_string(reader.as_mut().ffi_read_version())
}

pub unsafe extern "C" fn ffi_read_column(
    mut reader: ColumnarReaderPtr,
    col_id: i64,
) -> RustStrWithView {
    build_from_string(reader.as_mut().ffi_read_column(col_id))
}

pub unsafe extern "C" fn ffi_physical_table_id(mut reader: ColumnarReaderPtr) -> i64 {
    reader.as_mut().ffi_physical_table_id()
}

pub unsafe extern "C" fn ffi_columnar_scan_stats(
    mut reader: ColumnarReaderPtr,
) -> ColumnarScanStats {
    reader.as_mut().take_columnar_runtime_stats().into()
}
