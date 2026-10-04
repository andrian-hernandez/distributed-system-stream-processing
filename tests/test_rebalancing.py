import pytest

from src.partitioning.rebalancer import Partition, PartitionRebalancer


def test_initial_assignment_covers_all_partitions():
    partitions = [Partition(i) for i in range(6)]

    manager = PartitionRebalancer(
        nodes=["node-a", "node-b", "node-c"],
        partitions=partitions,
    )

    assert manager.validate_ownership()
    assert sum(manager.loads().values()) == 6


def test_initial_assignment_is_balanced():
    partitions = [Partition(i) for i in range(6)]

    manager = PartitionRebalancer(
        nodes=["node-a", "node-b", "node-c"],
        partitions=partitions,
    )

    assert manager.loads() == {
        "node-a": 2,
        "node-b": 2,
        "node-c": 2,
    }

    assert manager.is_balanced()


def test_rebalance_corrects_imbalanced_distribution():
    partitions = [Partition(i) for i in range(6)]

    manager = PartitionRebalancer(
        nodes=["node-a", "node-b", "node-c"],
        partitions=partitions,
    )

    for partition_id in [1, 2, 3]:
        manager.move_partition(partition_id, "node-a")

    assert not manager.is_balanced()

    manager.rebalance()

    assert manager.loads() == {
        "node-a": 2,
        "node-b": 2,
        "node-c": 2,
    }

    assert manager.validate_ownership()


def test_rebalance_does_not_duplicate_or_drop_partitions():
    partitions = [Partition(i) for i in range(10)]

    manager = PartitionRebalancer(
        nodes=["node-a", "node-b"],
        partitions=partitions,
    )

    manager.move_partition(1, "node-a")
    manager.move_partition(3, "node-a")
    manager.rebalance()

    assigned = [
        partition_id
        for values in manager.ownership.values()
        for partition_id in values
    ]

    assert sorted(assigned) == list(range(10))
    assert len(assigned) == len(set(assigned))


def test_unknown_partition_raises_error():
    manager = PartitionRebalancer(
        nodes=["node-a"],
        partitions=[Partition(1)],
    )

    with pytest.raises(ValueError, match="Unknown partition"):
        manager.move_partition(99, "node-a")


def test_unknown_node_raises_error():
    manager = PartitionRebalancer(
        nodes=["node-a"],
        partitions=[Partition(1)],
    )

    with pytest.raises(ValueError, match="Unknown target node"):
        manager.move_partition(1, "node-x")


def test_duplicate_nodes_are_rejected():
    with pytest.raises(ValueError, match="Node names must be unique"):
        PartitionRebalancer(
            nodes=["node-a", "node-a"],
            partitions=[Partition(1)],
        )


def test_duplicate_partitions_are_rejected():
    with pytest.raises(ValueError, match="Partition IDs must be unique"):
        PartitionRebalancer(
            nodes=["node-a"],
            partitions=[Partition(1), Partition(1)],
        )
