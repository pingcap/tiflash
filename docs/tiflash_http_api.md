# TiFlash HTTP API

In the context of the following line,

- `TIFLASH_IP` is the ip or domain name of TiFlash server. `127.0.0.1` by default.
- `TIFLASH_STATUS_PORT` is the port of `flash.proxy.status-addr`. `20292` by default.

Note that if TLS is enabled, you need to specify the `--key`, `--cert`, `--cacert` for running `curl`. For example:

```bash
curl --key tls.key --cert tls.crt --cacert ca.crt "https://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/profile"
```

## CPU Profiling

Collect and export CPU profiling data within a specified time range.

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/profile?seconds=<seconds>"
curl -H 'Content-Type:<type>' -X GET "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/profile?seconds=<seconds>&frequency=<frequency>"
```

#### Parameters

- **seconds** (optional): Specifies the number of seconds to collect CPU profiling data.
  - Default: 10
  - Example: `?seconds=20`

- **frequency** (optional): Specifies the sampling frequency for CPU profiling data.
  - Default: 99
  - Example: `?frequency=100`

- **type** (optional): Specifies the Content-Type of the response.
  - Options: `application/protobuf` for raw profile data, any other types for flame graph.
  - Default: `N/A`
  - Example: `-H "Content-Type:application/protobuf"`

#### Response

The server will return CPU profiling data. The response format is determined by the Content-Type in the request header and can be either raw profile data in protobuf format or flame graph in SVG format.

The raw profile data can be handled by `pprof` tool. For example, use `go tool pprof --http=0.0.0.0:1234 xxx.proto` to open a interactive web browser.

## Memory Profiling

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/set_prof_active"
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/set_prof_inactive"
# Get a svg file of memory allocation. Only available after `set_prof_active`.
# This require perl, objdump, nm, addr2line, c++filt, dot are installed in the TiFlash running host
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/heap?jeprof=true&text=svg" > s.svg
# Get a raw heap file of memory allocation. Only available after `set_prof_active`.
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/heap" > h.heap
# Start a dedicated thread to dump heap files periodically. Only available after `set_prof_active`.
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/heap_activate?interval=<seconds>"
# Return the generated heap file list
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/heap_list"
# Stop the thread to dump heap files. This will also remove the heap files on disk
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/heap_deactivate"
```

## Get number of symbol

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/symbol"
```

#### Response

Always return `1` symbol

```
num_symbols: 1
```

## Resolve address to symbol

```bash
curl -X POST "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/symbol" -d '<address-list>'
```

### Parameters

- **address-list** : Specifies the addresses to be resolved to symbol. The addresses are joined by '+'
  - Example: `0x56424a39c97e+0x564251100e4c`

### Response

Returns the address and resolved symbol line by line

```bash
curl -X POST "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/symbol" -d '0x56424a39c97e+0x564251100e4c'
0x56424a39c97e _ZNK2DB2PS2V313PageDirectoryINS1_4u12818PageDirectoryTraitEE14createSnapshotERKNSt3__112basic_stringIcNS6_11char_traitsIcEENS6_9allocatorIcEEEE
0x564251100e4c _ZN2DB2PS2V315PageStorageImpl11getSnapshotERKNSt3__112basic_stringIcNS3_11char_traitsIcEENS3_9allocatorIcEEEE
```

## Memory arena purge (jemalloc)

purge the jemalloc arena to release the memory owned by jemalloc

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/arena_purge"
```

## Memory status (jemalloc)

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/memory_status"
```

## Running status

Get the current status of TiFlash

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/store-status"
```

### Response

Return a string represent the current status of TiFlash. The returned result is one of the following strings, `Idle`/`Ready`/`Running`/`Stopping`/`Terminated`.


## TiFlash replica syncing status

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/sync-status/${table_id}"
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/sync-status/keyspace/${keyspace_id}/table/${table_id}"
```

### Parameters

- **table_id**: Specifies the table_id to get the tiflash replica syncing status
  - Example: `100`

- **keyspace_id**: Specifies the keyspace_id under multiple tenants
  - Example: `1`


### Response

```
<num_replicated_region>
<region_id_1> <region_id_2> ... <region_id_n>
```

## TiFlash write node remote gc owner info under disaggregated arch 

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/owner/info"
```

### Response


```json
{
    "status": "IsOwner",
    "owner_id": "172.31.9.1:3930"
}
```

```json
{
    "status": "NotOwner",
    "owner_id": "172.31.9.1:3930"
}
```

## Resign the TiFlash write node remote gc owner under disaggregated arch 

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/owner/resign"
```

### Response

```json
{
    "message": "Done"
}
```

```json
{
    "message": "This node is not the remote gc owner, can't be resigned."
}
```

## Execute TiFlash write node remote gc under disaggregated arch 

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/gc"
```

### Response

```json
{
    "status": "IsOwner",
    "owner_id": "172.31.9.1:3930",
    "execute": "true"
}
```

```json
{
    "status": "NotOwner",
    "owner_id": "172.31.9.1:3930",
    "execute": "false"
}
```

## Execute TiFlash write node upload under disaggregated arch 

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/upload"
```

### Response

```json
{
    "message": "flag_set=true"
}
```

## Get the local cache info for TiFlash compute node under disaggregated arch 

```bash
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tiflash/remote/cache/info"
```

### Response

```json
[
    {"file_type":"Meta","file_type_int":1,"histogram":{"in360min":{"bytes":32582014,"count":3537},"oldest":{"access_time":"2025-12-22 21:23:30.447107","size":9105},"total":{"bytes":32582014,"count":3537}}},
    {"file_type":"Merged","file_type_int":5,"histogram":{"in360min":{"bytes":870994443,"count":3532},"oldest":{"access_time":"2025-12-22 21:23:30.447114","size":221668},"total":{"bytes":870994443,"count":3532}}},
    {"file_type":"VersionColData","file_type_int":10,"histogram":{"in360min":{"bytes":4220970,"count":7},"oldest":{"access_time":"2025-12-22 21:23:30.514550","size":643757},"total":{"bytes":4220970,"count":7}}},
    {"file_type":"HandleColData","file_type_int":11,"histogram":{"in360min":{"bytes":3386871454,"count":920},"oldest":{"access_time":"2025-12-22 21:23:30.447121","size":4033498},"total":{"bytes":3386871454,"count":920}}},
    {"file_type":"ColData","file_type_int":12,"histogram":{"in360min":{"bytes":17178044030,"count":7926},"oldest":{"access_time":"2025-12-22 21:23:30.447124","size":505010},"total":{"bytes":17178044030,"count":7926}}}
]
```

## Evict the local cache for TiFlash compute node under disaggregated arch

```bash
# Evict by size. Will try to reserve ${size_in_bytes} free space, evicting
# the old cache that is older than configured
# `profiles.default.dt_filecache_min_age_seconds`.
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tiflash/remote/cache/evict/size/{size_in_bytes}"
# Evict by size. Will force to reserve ${size_in_bytes} free space when no
# enough space after evicting old cache. The age is overwrite by ${min_age}.
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tiflash/remote/cache/evict/size/{size_in_bytes}?force&age=<min_age>"
# Evict by file type. Will evict all cache files with type >= ${file_type_int}.
curl "http://${TIFLASH_IP}:${TIFLASH_STATUS_PORT}/tiflash/remote/cache/evict/type/{file_type_int}"
```

### Response

```json
{
    "req":"{method=ByEvictSize reserve_size=408022894000 min_age=0 force=false}",
    "released_size":"59473308"
}
```

```json
{
    "req":"{method=ByFileType evict_type=Merged}",
    "released_size":"21380742440"
}
```
