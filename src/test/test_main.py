"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest


class TestMain(unittest.TestCase):

    def test_import_workspace_perms(self):
        """Test the import_workspace_perms function."""
        results = kbase_module.run_method('import_workspace_perms', {'start': 1, 'stop': 10})
        print('results!', results)

    def test_import_workspaces(self):
        """Test the import_workspaces function."""
        results = kbase_module.run_method('import_workspaces', {'start': 1, 'stop': 10})
        print('results!', results)

    def test_import_range(self):
        """Test the import_range function."""
        results = kbase_module.run_method('import_range', {'start': 1, 'stop': 2})
        self.assertEqual(results['status'], 'done')

    def test_show_config(self):
        result = kbase_module.run_method('show_config', {})
        self.assertEqual(result['token_set?'], True)
        self.assertTrue(result['relation_engine_url'])
        self.assertTrue(result['workspace_url'])
