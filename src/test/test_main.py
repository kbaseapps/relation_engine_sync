"""
This file contains tests for src/main.py
"""
import kbase_module
import unittest

# TODO set up docker compose with re api
#      then we can generate test data here


class TestMain(unittest.TestCase):

    @unittest.skip('x')
    def test_import_workspace_perms(self):
        """Test the import_workspace_perms function."""
        results = kbase_module.run_method('import_workspace_perms', {'start': 1, 'stop': 10})
        self.assertEqual(results['vertex_import_count'], 14)
        self.assertEqual(results['edge_import_count'], 14)
        self.assertEqual(results['vertex_import_count'], results['edge_import_count'])

    @unittest.skip('x')
    def test_import_workspaces(self):
        """Test the import_workspaces function."""
        results = kbase_module.run_method('import_workspaces', {'start': 1, 'stop': 10})
        self.assertEqual(results['import_count'], 9)

    def test_import_objects(self):
        """Test the import_objects function."""
        results = kbase_module.run_method('import_objects', {'workspace_ids': [32152]})
        print('results', results)
        # TODO better test of results here
        # self.assertEqual(results['status'], 'done')

    @unittest.skip('x')
    def test_show_config(self):
        result = kbase_module.run_method('show_config', {})
        self.assertEqual(result['token_set?'], True)
        self.assertTrue(result['relation_engine_url'])
        self.assertTrue(result['workspace_url'])
