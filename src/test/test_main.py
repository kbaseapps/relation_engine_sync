"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest

uids = ['jayrbolton']


class TestMain(unittest.TestCase):

    def test_update_provenance(self):
        """Test the update_provenance function."""
        for uid in uids:
            result = kbase_module.run_method('update_provenance', {'user_ids': [uid]})
            print(result)
