from __future__ import annotations

import unittest

from minidrive.distribution import RoundRobinDistributor


class DistributionTests(unittest.TestCase):
    def test_assign_by_index_round_robin(self) -> None:
        nodes = ["n1", "n2", "n3"]
        d = RoundRobinDistributor(nodes)

        results = [d.assign_by_index(i) for i in range(6)]
        self.assertEqual(results, ["n1", "n2", "n3", "n1", "n2", "n3"])

    def test_assign_next_sequence(self) -> None:
        nodes = ["nA", "nB"]
        d = RoundRobinDistributor(nodes)

        self.assertEqual(d.assign_next(), "nA")
        self.assertEqual(d.assign_next(), "nB")
        self.assertEqual(d.assign_next(), "nA")

    def test_empty_nodes_raises(self) -> None:
        with self.assertRaises(ValueError):
            RoundRobinDistributor([])


if __name__ == "__main__":
    unittest.main()
