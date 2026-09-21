#!/usr/bin/env python3
#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved.
#
"""
Unit testing for gpversion module
"""
import unittest

from gpversion import *


class GpVersionTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_main(self):
        vMain = GpVersion("main")

        self.assertTrue(vMain.isVersionCurrentRelease())
        self.assertEqual(vMain.version, MAIN_VERSION)
        self.assertEqual(vMain.getVersionBuild(), 'dev')
        self.assertEqual(str(vMain), 'main build dev')

    def test_list_argument(self):
        v = GpVersion([3, 0])

        self.assertTrue(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionBuild(), 'dev')
        self.assertEqual(v.getVersionRelease(), "3")
        self.assertTrue(v.isVersionRelease("3.0"))
        self.assertFalse(v.isVersionRelease("2.0"))
        self.assertTrue(v > "2.5.0")
        self.assertTrue(v < "4.0")

    def test_too_long(self):
        # GpVersion keeps at most major.minor.patch
        with self.assertRaisesRegex(Exception, "Version too long"):
            GpVersion([3, 3, 0, 0])

    def test_short_string(self):
        v = GpVersion("3.0 build dev")

        self.assertTrue(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionBuild(), 'dev')
        self.assertEqual(v.getVersionRelease(), "3")
        self.assertTrue(v.isVersionRelease("3.0"))
        self.assertFalse(v.isVersionRelease("2.0"))

    def test_special_build(self):
        v = GpVersion("3.4.filerep")

        self.assertTrue(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionBuild(), 'filerep')
        self.assertEqual(v.getVersionRelease(), "3")
        self.assertTrue(v.isVersionRelease("3.4"))
        self.assertFalse(v.isVersionRelease("2.4"))

    def test_version_and_build_string(self):
        v = GpVersion("postgres (Apache Cloudberry) 3.0.0 build 3")

        self.assertTrue(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionBuild(), '3')
        self.assertEqual(v.getVersionRelease(), "3")
        self.assertTrue(v.isVersionRelease("3.0"))
        self.assertFalse(v.isVersionRelease("2.0"))
        self.assertTrue(v > "2.5.0")
        self.assertTrue(v < "4.0")

    def test_full_version_string(self):
        vLong = GpVersion("PostgreSQL 16.9 (Apache Cloudberry 3.0.0 build dev) "
                          "on aarch64-unknown-linux-gnu, compiled by gcc, 64-bit")

        self.assertTrue(vLong.isVersionCurrentRelease())
        self.assertEqual(vLong.getVersionBuild(), 'dev')
        self.assertEqual(vLong.getVersionRelease(), "3")
        self.assertTrue(vLong.isVersionRelease("3.0"))
        self.assertFalse(vLong.isVersionRelease("2.0"))
        self.assertTrue(vLong > "2.5.0")
        self.assertTrue(vLong < "4.0")

    def test_older_release_is_not_current(self):
        vOld = GpVersion("PostgreSQL 9.4.20 (Apache Cloudberry 2.5.0 build dev) "
                         "on x86_64-unknown-linux-gnu, 64-bit")

        self.assertFalse(vOld.isVersionCurrentRelease())
        self.assertEqual(vOld.getVersionBuild(), 'dev')
        self.assertEqual(vOld.getVersionRelease(), "2")
        self.assertTrue(vOld.isVersionRelease("2.5"))
        self.assertFalse(vOld.isVersionRelease("3.0"))
        self.assertTrue(vOld < "3.0")

    def test_comparisons(self):
        self.assertTrue(GpVersion("3.0.0") == "3.0.0")
        self.assertTrue(GpVersion("3.0.0") != "3.0.1")
        self.assertTrue(GpVersion("3.0.0") <= "3.0.0")
        self.assertTrue(GpVersion("3.0.0") >= "3.0.0")
        self.assertTrue(GpVersion("3.1") > GpVersion("3.0"))
        self.assertTrue(GpVersion("main") > "3.0")


#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
