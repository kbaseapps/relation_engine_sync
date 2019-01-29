"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest


class TestMain(unittest.TestCase):

    def test_import_range(self):
        """Test the import_range function."""
        kbase_module.run_method('import_range', {'start': 1, 'stop': 10})
