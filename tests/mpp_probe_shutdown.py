#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify real IsAlive, SIGTERM, late DispatchMPPTask and normal task drain.

Uses standalone TiFlash with mock PD/regions. A baseline debug MPP query supplies
actual DAG plans; late requests and their result stream use production gRPC.
Requires grpcio/protobuf and the protoc/clean tipb protos from the binary's build.
"""

import argparse
import json
import pathlib
import re
import signal
import socket
import subprocess
import tempfile
import time

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, text_format


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--work-dir", type=pathlib.Path)
    parser.add_argument("--probe-window", type=int, default=8)
    parser.add_argument("--grpc-port", type=int, default=3930)
    parser.add_argument("--tcp-port", type=int, default=19062)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if args.probe_window != 0 and args.probe_window < 4:
        parser.error("--probe-window must be 0 (disabled) or at least 4 seconds")
    work = args.work_dir or pathlib.Path(tempfile.mkdtemp(prefix="mpp-probe-shutdown-"))
    work.mkdir(parents=True, exist_ok=True)
    if any(work.iterdir()):
        parser.error("--work-dir must be empty")
    work = work.resolve()
    for port in (args.grpc_port, args.tcp_port):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))

    build = binary.parents[3]
    descriptor = work / "tipb-descriptor.pb"
    subprocess.run([str(build / "contrib/protobuf-cmake/protoc"),
                    "--proto_path=" + str(build / "contrib/tipb/cpp/protos"), "--include_imports",
                    "--descriptor_set_out=" + str(descriptor), "select.proto"], check=True)
    pool = descriptor_pool.DescriptorPool()
    for item in descriptor_pb2.FileDescriptorSet.FromString(descriptor.read_bytes()).file:
        pool.Add(item)
    schema = descriptor_pb2.FileDescriptorProto(name="probe.proto", package="probe", syntax="proto3")

    def message(name, fields):
        result = schema.message_type.add(name=name)
        for name, number, kind, repeated, target in fields:
            field = result.field.add(name=name, number=number, type=kind, label=3 if repeated else 1)
            if target:
                field.type_name = ".probe." + target

    # These are the kvproto wire fields used by this isolated request. DAGs use the complete build descriptor.
    message("Error", [("msg", 2, 9, False, None)])
    message("TaskMeta", [("start_ts", 1, 4, False, None), ("task_id", 2, 3, False, None),
                         ("partition_id", 3, 3, False, None), ("address", 4, 9, False, None),
                         ("gather_id", 5, 4, False, None), ("query_ts", 6, 4, False, None),
                         ("local_query_id", 7, 4, False, None), ("server_id", 8, 4, False, None)])
    message("Epoch", [("conf_ver", 1, 4, False, None), ("version", 2, 4, False, None)])
    message("KeyRange", [("start", 1, 12, False, None), ("end", 2, 12, False, None)])
    message("Region", [("region_id", 1, 4, False, None), ("region_epoch", 2, 11, False, "Epoch"),
                       ("ranges", 3, 11, True, "KeyRange")])
    message("Dispatch", [("meta", 1, 11, False, "TaskMeta"), ("encoded_plan", 2, 12, False, None),
                         ("timeout", 3, 3, False, None), ("regions", 4, 11, True, "Region"),
                         ("schema_ver", 5, 3, False, None)])
    message("DispatchResponse", [("error", 1, 11, False, "Error")])
    message("Establish", [("sender_meta", 1, 11, False, "TaskMeta"),
                          ("receiver_meta", 2, 11, False, "TaskMeta")])
    message("Packet", [("data", 1, 12, False, None), ("error", 2, 11, False, "Error")])
    message("Alive", [("available", 1, 8, False, None)])
    pool.Add(schema)

    def proto(name, raw=None):
        cls = message_factory.GetMessageClass(pool.FindMessageTypeByName(name))
        return cls() if raw is None else cls.FromString(raw)

    events, started = [], time.monotonic()

    def record(event, **details):
        entry = dict(event=event, elapsed_seconds=round(time.monotonic() - started, 6), **details)
        events.append(entry)
        (work / "events.json").write_text(json.dumps(events, indent=2) + "\n")
        print(json.dumps(entry), flush=True)

    def query(sql):
        result = subprocess.run([str(binary), "client", "--host", "127.0.0.1", "--port", str(args.tcp_port),
                                 "--format", "TabSeparated", "--query", sql],
                                text=True, capture_output=True, timeout=20)
        record("sql", sql=sql, exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    addr = f"127.0.0.1:{args.grpc_port}"
    config = work / "config.toml"
    config.write_text(f'''path = "{work / 'data'}"
listen_host = "127.0.0.1"
tcp_port = {args.tcp_port}
[logger]
level = "debug"
log = "{work / 'server.log'}"
errorlog = "{work / 'server-error.log'}"
[flash]
service_addr = "{addr}"
graceful_shutdown_probe_timeout = {args.probe_window}
graceful_wait_shutdown_timeout = 0
[profiles.default]
enable_local_tunnel = 0
''')
    with (work / "process.log").open("w") as output, grpc.insecure_channel(addr) as channel:
        probe = channel.unary_unary("/tikvpb.Tikv/IsAlive", response_deserializer=lambda raw: proto("probe.Alive", raw))
        dispatch = channel.unary_unary("/tikvpb.Tikv/DispatchMPPTask")
        establish = channel.unary_stream("/tikvpb.Tikv/EstablishMPPConnection")
        process = subprocess.Popen([str(binary), "server", "--config-file", str(config)],
                                   stdout=output, stderr=subprocess.STDOUT)
        record("server_started", pid=process.pid, binary=str(binary))
        try:
            deadline = time.monotonic() + 60
            while True:
                assert process.poll() is None, f"server exited during startup: {process.returncode}"
                try:
                    if probe(b"", timeout=1).available:
                        break
                except grpc.RpcError:
                    pass
                assert time.monotonic() < deadline, "server startup timed out"
                time.sleep(0.1)
            query("DBGInvoke __enable_schema_sync_service('true')")
            query("DBGInvoke __mock_tidb_table(default, probe_shutdown, 'v Int64')")
            query("DBGInvoke __refresh_mapped_table_schema(default, probe_shutdown)")
            query("DBGInvoke __put_region(62, 0, 100, default, probe_shutdown)")
            query("DBGInvoke __raft_insert_row(default, probe_shutdown, 62, 1, 42)")
            assert query("DBGInvoke tidb_mock_query('select sum(v) from default.probe_shutdown', "
                         "62, 18446744073709551615, 'mpp_query:true,mpp_partition_num:1')") == "42"
            # Reconstruct exactly the logged baseline DAGs, retaining their serialized exchange task metadata.
            plans = []
            pattern = r'\[executeQuery.cpp:\d+\] \[("(?:\\.|[^"\\])*")\] \[source="MPP<.*task_id:(\d+)>'
            for line in (work / "server.log").read_text().splitlines():
                match = re.search(pattern, line)
                if match:
                    logged_query = json.loads(match[1])
                    if "root_executor" not in logged_query:
                        continue
                    dag = proto("tipb.DAGRequest")
                    text_format.Parse(logged_query.split(") ", 1)[1], dag)
                    plans.append((int(match[2]), dag))
            assert len(plans) == 2, f"expected two baseline MPP tasks, got {len(plans)}"
            record("baseline_mpp_query_executed", result="42", task_ids=[task_id for task_id, _ in plans])
            last_probe = time.monotonic()
            assert probe(b"", timeout=1).available
            record("successful_probe")
            process.send_signal(signal.SIGTERM)
            record("sigterm")
            deadline = time.monotonic() + 2
            while probe(b"", timeout=0.5).available:
                assert time.monotonic() < deadline, "shutdown did not reject new probes"
                time.sleep(0.01)
            record("rejected_probe")
            assert process.poll() is None
            if args.probe_window:
                root_request = None
                for task_id, dag in plans:
                    sender = dag.root_executor.exchange_sender
                    receiver = proto("probe.TaskMeta", sender.encoded_task_meta[0])
                    request = proto("probe.Dispatch")
                    request.meta.CopyFrom(receiver)
                    request.meta.task_id = task_id
                    request.meta.address = addr
                    request.encoded_plan = dag.SerializeToString()
                    request.timeout, request.schema_ver = 10, -1
                    if receiver.task_id == -1:
                        root_request = proto("probe.Establish")
                        root_request.sender_meta.CopyFrom(request.meta)
                        root_request.receiver_meta.CopyFrom(receiver)
                    else:
                        # The only scanned table and mock region are the ones just created above.
                        table_id = sender.child.aggregation.child.tbl_scan.table_id
                        prefix = b"t" + (table_id ^ (1 << 63)).to_bytes(8, "big") + b"_r"
                        region = request.regions.add(region_id=62)
                        region.region_epoch.conf_ver, region.region_epoch.version = 6, 5
                        region.ranges.add(start=prefix + (1 << 63).to_bytes(8, "big"),
                                          end=prefix + ((1 << 63) + 100).to_bytes(8, "big"))
                    (work / f"dispatch-{task_id}.bin").write_bytes(request.SerializeToString())
                    response = proto("probe.DispatchResponse", dispatch(request.SerializeToString(), timeout=3))
                    assert not response.HasField("error"), response.error.msg
                    record("late_dispatch_accepted", task_id=task_id)
                assert root_request is not None
                chunks = []
                for raw in establish(root_request.SerializeToString(), timeout=args.probe_window):
                    packet = proto("probe.Packet", raw)
                    assert not packet.HasField("error"), packet.error.msg
                    if packet.data:
                        response = proto("tipb.SelectResponse", packet.data)
                        assert not response.HasField("error"), response.error.msg
                        chunks.extend(chunk.rows_data for chunk in response.chunks if chunk.rows_data)
                record("late_result_chunks", rows_hex=[chunk.hex() for chunk in chunks])
                assert len(chunks) == 1 and len(chunks[0]) == 9, "expected one fixed-width integer row"
                row = chunks[0]
                assert row[0] in (3, 4), "expected TiDB Int or Uint codec"
                result = int.from_bytes(row[1:], "big") ^ ((1 << 63) if row[0] == 3 else 0)
                assert result == 42, f"late MPP task returned {result}"
                assert time.monotonic() - last_probe < args.probe_window
                record("late_mpp_query_executed", result=result)
                assert not probe(b"", timeout=0.5).available
            deadline = time.monotonic() + args.probe_window + 30
            drain_seen = False
            while process.poll() is None:
                log = (work / "server.log").read_text()
                if not drain_seen and "Start to wait all MPP tasks to finish, timeout=0s" in log:
                    drain_seen = True
                    elapsed = time.monotonic() - last_probe
                    record("task_drain_started", seconds_since_probe=elapsed)
                    assert elapsed >= args.probe_window
                assert time.monotonic() < deadline, "server shutdown timed out"
                time.sleep(0.05)
            elapsed = time.monotonic() - last_probe
            record("server_exited", exit_code=process.wait(), seconds_since_probe=elapsed)
            assert process.returncode == 0
            assert elapsed >= args.probe_window + 2
            log = (work / "server.log").read_text()
            assert drain_seen
            assert ("last successful MPP probe protection" in log) == bool(args.probe_window)
            if args.probe_window:
                shutdown_log = log.split("Set unavailable for MPPTask", 1)[1]
                evidence = [line for line in shutdown_log.splitlines()
                            if "processing dispatch is over" in line or "finish MPPTask:" in line]
                assert any("processing dispatch is over" in line for line in evidence)
                assert any("finish MPPTask:" in line for line in evidence)
                record("late_task_log_evidence", lines=evidence)
            else:
                assert elapsed < 15, "disabled protection must finish without the default 60-second wait"
            assert "All MPP tasks have finished after" in log
            record("passed")
        except BaseException as error:
            record("failed", error=repr(error))
            raise
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
