import zlib

from minikafka.producer.partitioner import Partitioner


def test_keyed_partition_is_stable() -> None:
    partitioner = Partitioner()

    assert partitioner.choose(3, key=b"user-42") == (
        zlib.crc32(b"user-42") % 3
    )
    assert partitioner.choose(3, key=b"user-42") == partitioner.choose(
        3,
        key=b"user-42",
    )


def test_keyless_partition_is_sticky_until_batch_closes() -> None:
    partitioner = Partitioner()

    first = partitioner.choose(3, key=None)
    assert partitioner.choose(3, key=None) == first

    partitioner.on_batch_closed(3, first)

    assert partitioner.choose(3, key=None) == (first + 1) % 3
