from __future__ import annotations

import asyncio
import base64
import binascii
import json
from typing import Any, Self

from minikafka.core.batch import RecordBatch
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.errors import (
    FrameTooLarge,
    InvalidRequest,
    MiniKafkaError,
)
from minikafka.replication.model import AckMode, IsolationLevel


class JsonTcpServer:
    def __init__(
        self,
        cluster: object,
        server: asyncio.Server,
        *,
        max_frame_bytes: int,
    ) -> None:
        self.cluster = cluster
        self._server = server
        self.max_frame_bytes = max_frame_bytes

    @classmethod
    async def start(
        cls,
        cluster: object,
        host: str,
        port: int,
        *,
        max_frame_bytes: int = 1_048_576,
    ) -> Self:
        instance: Self | None = None

        async def handler(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            if instance is not None:
                await instance._serve(reader, writer)

        server = await asyncio.start_server(
            handler,
            host,
            port,
            limit=max_frame_bytes + 1,
        )
        instance = cls(
            cluster,
            server,
            max_frame_bytes=max_frame_bytes,
        )
        return instance

    @property
    def address(self) -> tuple[str, int]:
        socket = self._server.sockets[0]
        host, port = socket.getsockname()[:2]
        return str(host), int(port)

    async def close(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                try:
                    line = await reader.readline()
                except ValueError:
                    await self._write(writer, self._error(FrameTooLarge()))
                    break
                if not line:
                    break
                if len(line) > self.max_frame_bytes:
                    await self._write(writer, self._error(FrameTooLarge()))
                    break
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise InvalidRequest("request must be a JSON object")
                    response = await self.dispatch(request)
                except MiniKafkaError as error:
                    response = self._error(error)
                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    json.JSONDecodeError,
                    binascii.Error,
                ) as error:
                    response = self._error(InvalidRequest(str(error)))
                await self._write(writer, response)
        finally:
            writer.close()
            await writer.wait_closed()

    async def dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        operation = request.get("operation")
        if operation == "create_topic":
            topic = await self.cluster.create_topic(
                str(request["topic"]),
                int(request["partitions"]),
                int(request["replication_factor"]),
            )
            return {"ok": True, "topic": topic.name}
        if operation == "metadata":
            topic = await self.cluster.describe_topic(str(request["topic"]))
            return {
                "ok": True,
                "topic": topic.name,
                "partitions": [
                    {
                        "partition": metadata.partition,
                        "leader_id": metadata.leader_id,
                        "leader_epoch": metadata.leader_epoch,
                        "replicas": list(metadata.replicas),
                    }
                    for metadata in topic.partitions.values()
                ],
            }
        if operation == "produce":
            topic = str(request["topic"])
            partition = int(request.get("partition", 0))
            batch = RecordBatch.unassigned(
                (
                    Record(
                        self._decode_optional(request, "key_b64"),
                        self._decode_optional(request, "value_b64"),
                        self.cluster.clock.now_ms(),
                    ),
                )
            )
            result = await self.cluster.append_batch(
                TopicPartition(topic, partition),
                batch,
                AckMode.parse(request.get("acks", "1")),
            )
            return {
                "ok": True,
                "partition": partition,
                "offset": result.base_offset,
            }
        if operation == "fetch":
            tp = TopicPartition(
                str(request["topic"]),
                int(request["partition"]),
            )
            isolation = IsolationLevel(
                request.get("isolation", IsolationLevel.READ_UNCOMMITTED)
            )
            records = self.cluster.fetch(
                tp,
                int(request["offset"]),
                int(request.get("max_records", 100)),
                isolation,
            )
            return {
                "ok": True,
                "records": [
                    {
                        "offset": record.offset,
                        "key_b64": self._encode_optional(record.key),
                        "value_b64": self._encode_optional(record.value),
                    }
                    for record in records
                ],
            }
        if operation == "join_group":
            joined = await self.cluster.groups.join(
                str(request["group_id"]),
                str(request["member_id"]),
                tuple(str(topic) for topic in request["topics"]),
            )
            return {
                "ok": True,
                "generation": joined.generation,
                "assignment": [
                    {"topic": tp.topic, "partition": tp.partition}
                    for tp in joined.assignment
                ],
            }
        if operation == "heartbeat":
            await self.cluster.groups.heartbeat(
                str(request["group_id"]),
                str(request["member_id"]),
                int(request["generation"]),
            )
            return {"ok": True}
        if operation == "commit_offsets":
            offsets = {
                TopicPartition(str(item["topic"]), int(item["partition"])): int(
                    item["offset"]
                )
                for item in request["offsets"]
            }
            await self.cluster.groups.commit(
                str(request["group_id"]),
                str(request["member_id"]),
                int(request["generation"]),
                offsets,
            )
            return {"ok": True}
        raise InvalidRequest(f"unknown operation {operation!r}")

    @staticmethod
    def _decode_optional(
        request: dict[str, Any],
        name: str,
    ) -> bytes | None:
        value = request.get(name)
        if value is None:
            return None
        return base64.b64decode(str(value), validate=True)

    @staticmethod
    def _encode_optional(value: bytes | None) -> str | None:
        if value is None:
            return None
        return base64.b64encode(value).decode("ascii")

    @staticmethod
    def _error(error: MiniKafkaError) -> dict[str, Any]:
        return {
            "ok": False,
            "code": error.code,
            "message": str(error),
        }

    @staticmethod
    async def _write(
        writer: asyncio.StreamWriter,
        response: dict[str, Any],
    ) -> None:
        writer.write(
            json.dumps(response, separators=(",", ":")).encode() + b"\n"
        )
        await writer.drain()
