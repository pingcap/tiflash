#!/usr/bin/python3

import unittest

from tikv_util import common


class TestTikvKey(unittest.TestCase):

    def test_pd_tikv_key(self):
        pd_key = '7480000000000000FF2D5F728000000000FF3921010000000000FA'
        ori_tikv_key = b't\x80\x00\x00\x00\x00\x00\x00\xff-_r\x80\x00\x00\x00\x00\xff9!\x01\x00\x00\x00\x00\x00\xfa'

        self.assertEqual(len(ori_tikv_key) * 2, len(pd_key))

        tikv_key = common.TikvKey(ori_tikv_key)
        self.assertEqual(len(ori_tikv_key), tikv_key.size())
        self.assertEqual(ori_tikv_key, tikv_key.to_bytes())
        self.assertEqual(pd_key, tikv_key.to_pd_key())

        p = common.decode_pd_key(pd_key, len(pd_key))
        self.assertEqual(p, ori_tikv_key)

    def test_start_end_of_table(self):
        b1, e1 = common.make_table_begin(123), common.make_table_end(123)
        self.assertEqual(b1.to_pd_key(), '7480000000000000FF7B5F720000000000FA')
        self.assertEqual(e1.to_pd_key(), '7480000000000000FF7C00000000000000F8')
        self.assertTrue(b1.compare(e1) < 0)
        b2, e2 = common.make_table_begin(124), common.make_table_end(124)
        self.assertTrue(e1.compare(b2) < 0)
        b3, e3 = common.make_whole_table_begin(124), common.make_whole_table_end(124)
        self.assertTrue(b3.compare(e1) == 0)
        self.assertTrue(e3.compare(common.make_whole_table_begin(125)) == 0)

    def test_table_handle(self):
        b, e = common.make_table_begin(123), common.make_table_end(123)
        mx = common.make_table_handle(123, 9223372036854775807)
        mn = common.make_table_handle(123, -9223372036854775808)
        self.assertTrue(b.compare(mn) < 0)
        self.assertTrue(mn.compare(mx) < 0)
        self.assertTrue(mx.compare(e) < 0)
        self.assertTrue(mx.compare(mx) == 0)
        self.assertEqual(common.get_table_id(mx), 123)
        self.assertEqual(common.get_handle(mx), 9223372036854775807)
        self.assertEqual(common.get_handle(mn), -9223372036854775808)

    def test_region_cnt(self):
        s1 = b'3\n1000 1234 7 \n'
        s2 = b'3\n1000 7 9 \n'
        c = common.CheckRegionCnt()
        c.add(s1)
        c.add(s2)
        self.assertEqual(4, c.compute(1))
        self.assertEqual(2, c.compute(2))
        self.assertEqual(0, c.compute(3))

    def test_exception(self):
        o = b'12345'
        k = common.TikvKey(o)
        try:
            common.get_table_id(k)
            self.assertTrue(False)
        except RuntimeError:
            pass
