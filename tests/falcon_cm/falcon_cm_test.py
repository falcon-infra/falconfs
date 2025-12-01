# Copyright (c) 2025 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: MulanPSL-2.0

import os
import unittest
import cm.falcon_cm as falcon_cm


class TestFalconCM(unittest.TestCase):
    def setUp(self):
        os.environ["cn_num"] = "3"
        os.environ["dn_num"] = "3"
        os.environ["dn_sup_num"] = "0"
        os.environ["cn_sup_num"] = "0"

    def tearDown(self):
        os.environ["cn_num"] = ""
        os.environ["dn_num"] = ""
        os.environ["dn_sup_num"] = ""
        os.environ["cn_sup_num"] = ""

    def test_falcon_cm_init_success(self):
        """
        test init_success
        """
        falcon_cm_inst = falcon_cm.FalconCM(is_cn=True)
        self.assertEqual(falcon_cm_inst._replica_server_num, 2)

    def test_falcon_cm_init_valid_rep_num(self):
        """
        test validation of replica_server_num
        """
        except_got = 0
        # replica_server_num out of range
        os.environ["replica_server_num"] = "3"
        try:
            falcon_cm_inst = falcon_cm.FalconCM(is_cn=True)
        except ValueError as e:
            error_message = str(e)
            if "replica_server_num" in error_message:
                except_got += 1

        # replica_server_num out of range
        os.environ["replica_server_num"] = "-1"
        try:
            falcon_cm_inst = falcon_cm.FalconCM(is_cn=True)
        except ValueError as e:
            error_message = str(e)
            if "replica_server_num" in error_message:
                except_got += 1

        # replica_server_num in rang [0, 2]
        os.environ["replica_server_num"] = "0"
        try:
            falcon_cm_inst = falcon_cm.FalconCM(is_cn=True)
        except ValueError as e:
            error_message = str(e)
            if "replica_server_num" in error_message:
                except_got += 1

        self.assertEqual(falcon_cm_inst._replica_server_num, 0)
        self.assertEqual(except_got, 2)
