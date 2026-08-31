#!/usr/bin/env python3
"""Cluster-facing OffloadingManager test entrypoint.

Today this runs against the in-memory reference cluster. Once native BRPC
DN/Store clients are wired into FalconFSOffloadingManager, this same script
should be pointed at the single-Ubuntu cluster started by
`scripts/falcon_distributed_test.sh start`.
"""

from test_offloading_manager_api import OffloadingManagerAPITest
import unittest


if __name__ == "__main__":
    unittest.main()
