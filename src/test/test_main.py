"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest


class TestMain(unittest.TestCase):

    def test_update_provenance(self):
        """Test the echo function."""
        # for name in _usernames:
        result = kbase_module.run_method('update_provenance', {'uid': 'kbasedata'})
        print(result)
