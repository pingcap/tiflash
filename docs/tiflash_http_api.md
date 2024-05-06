# TiFlash HTTP API

In the context of the following line,

- `TIFLASH_IP` is the ip of TiFlash server. `127.0.0.1` by default.
- `TIFLASH_STATUS_PORT` is the port of `flash.proxy.status-addr`. `20292` by default.

## CPU Profiling

Collect and export CPU profiling data within a specified time range.

```bash
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/profile?seconds=<seconds>"
curl -H 'Content-Type:<type>' -X GET "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/debug/pprof/profile?seconds=<seconds>&frequency=<frequency>"
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

## Running status

Get the current status of TiFlash

```bash
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/store-status"
```

### Response

Retrun a string represent the current status of TiFlash. The returned result is one of the following strings, `Idle`/`Redy`/`Running`/`Stopping`/`Terminated`.


## TiFlash replica syncing status

```bash
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/sync-status/${table_id}"
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/sync-status/keyspace/${keyspace_id}/table/${table_id}"
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
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/owner/info"
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
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/owner/resign"
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
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/gc"
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
curl "http://${TiFLASH_IP}:${TIFLASH_STATUS_PORT}/tifash/remote/upload"
```

### Response

```json
{
    "message": "flag_set=true"
}
```
