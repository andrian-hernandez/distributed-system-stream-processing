from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Partition:
    partition_id: int


class PartitionRebalancer:
    """
    Tracks partition ownership and redistributes partitions when
    node load becomes imbalanced.
    """

    def __init__(
        self,
        nodes: List[str],
        partitions: List[Partition],
        imbalance_threshold: float = 0.20,
    ) -> None:
        if not nodes:
            raise ValueError("At least one node is required.")

        if not partitions:
            raise ValueError("At least one partition is required.")

        if imbalance_threshold < 0:
            raise ValueError("imbalance_threshold cannot be negative.")

        if len(set(nodes)) != len(nodes):
            raise ValueError("Node names must be unique.")

        partition_ids = [p.partition_id for p in partitions]

        if len(set(partition_ids)) != len(partition_ids):
            raise ValueError("Partition IDs must be unique.")

        self.nodes = list(nodes)
        self.partitions = list(partitions)
        self.imbalance_threshold = imbalance_threshold

        self.ownership: Dict[str, List[int]] = {
            node: [] for node in self.nodes
        }

        self._initial_assignment()

    def _initial_assignment(self) -> None:
        for index, partition in enumerate(self.partitions):
            node = self.nodes[index % len(self.nodes)]
            self.ownership[node].append(partition.partition_id)

    def loads(self) -> Dict[str, int]:
        return {
            node: len(partitions)
            for node, partitions in self.ownership.items()
        }

    def average_load(self) -> float:
        loads = self.loads()

        return sum(loads.values()) / len(loads)

    def is_balanced(self) -> bool:
        average = self.average_load()

        if average == 0:
            return True

        maximum = max(self.loads().values())

        return ((maximum - average) / average) <= self.imbalance_threshold

    def rebalance(self) -> Dict[str, List[int]]:
        """
        Redistribute partitions deterministically across nodes.

        Returns the resulting ownership mapping.
        """

        partition_ids = sorted(
            partition_id
            for partitions in self.ownership.values()
            for partition_id in partitions
        )

        new_ownership: Dict[str, List[int]] = {
            node: [] for node in self.nodes
        }

        for index, partition_id in enumerate(partition_ids):
            node = self.nodes[index % len(self.nodes)]
            new_ownership[node].append(partition_id)

        self.ownership = new_ownership

        return {
            node: list(partitions)
            for node, partitions in self.ownership.items()
        }

    def validate_ownership(self) -> bool:
        """
        Verify that every known partition has exactly one owner.
        """

        assigned = [
            partition_id
            for partitions in self.ownership.values()
            for partition_id in partitions
        ]

        expected = sorted(
            partition.partition_id
            for partition in self.partitions
        )

        return sorted(assigned) == expected and len(assigned) == len(set(assigned))

    def move_partition(
        self,
        partition_id: int,
        target_node: str,
    ) -> None:
        if target_node not in self.ownership:
            raise ValueError(f"Unknown target node: {target_node}")

        for node, partitions in self.ownership.items():
            if partition_id in partitions:
                partitions.remove(partition_id)
                self.ownership[target_node].append(partition_id)
                return

        raise ValueError(f"Unknown partition: {partition_id}")
