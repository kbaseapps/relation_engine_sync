"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest


class TestMain(unittest.TestCase):

    def test_update_provenance(self):
        """Test the update_provenance function."""
        for i in [19217]:  # [38690, 38699, 38701, 39200, 39253, 39325, 39332, 39595, 39595, 39605, 39686, 39850]:
            ws_id = i
            try:
                result = kbase_module.run_method('update_provenance', {'workspace_ids': [ws_id]})
                print(result)
            except Exception as err:
                print(err)
