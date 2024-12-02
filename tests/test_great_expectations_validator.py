import unittest
import pandas as pd
from unittest.mock import MagicMock, patch

from lib.great_expectations_validator import GreatExpectationsValidator


class TestGreatExpectationsValidator(unittest.TestCase):
    def setUp(self):
        # Initialize the validator instance
        self.validator = GreatExpectationsValidator()

    @patch("great_expectations.data_context.DataContext.get_context")
    def test_register_gx_data_sources(self, mock_get_context):
        # Mock the GX data context and datasource registration
        mock_context = MagicMock()
        mock_get_context.return_value = mock_context

        # Simulate existing datasource
        mock_context.list_datasources.return_value = ["pandas_default"]
        self.validator._register_gx_data_sources()

        mock_context.data_sources.add_pandas.assert_not_called()

        # Simulate missing datasource
        mock_context.list_datasources.return_value = []
        self.validator._register_gx_data_sources()

        mock_context.data_sources.add_pandas.assert_called_with(name="pandas_default")

    def test_compare_primary_keys_between_datasets(self):
        source_data = pd.DataFrame({"id": [1, 1, 3], "value": [100, 200, 301]})
        target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300]})

        result = self.validator.compare_primary_keys_between_datasets(
            source_data, target_data, primary_key="id"
        )

        expected_result = {
            "custom_expect_primary_keys_to_match": {
                "success": False,
                "detailed_errors": {
                    "duplicate_keys_in_source": [1],
                    "duplicate_keys_in_target": [],
                },
            }
        }
        self.assertEqual(result, expected_result)

    def test_compare_row_values_between_datasets(self):
        source_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 301]})
        target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300]})

        result = self.validator.compare_row_values_between_datasets(
            source_data, target_data, primary_key="id"
        )

        expected_result = {
            "custom_expect_row_values_to_match": {
                "success": False,
                "detailed_errors": {
                    "value": {
                        "row_indices": [2, 3],
                        "source_data_values": [200, 301],
                        "target_data_values": [250, 300],
                    }
                },
            }
        }
        self.assertEqual(result, expected_result)

    def test_all_expectations_successful(self):
        results = {
            "expect_table_columns_to_match_ordered_list": {"success": True},
            "expect_table_row_count_to_equal": {"success": False},
        }

        self.assertFalse(self.validator.all_expectations_successful(results))

        results = {
            "expect_table_columns_to_match_ordered_list": {"success": True},
            "expect_table_row_count_to_equal": {"success": True},
        }

        self.assertTrue(self.validator.all_expectations_successful(results))

    @patch("great_expectations.validator.validator.Validator")
    def test_apply_standard_expectations_to_datasets(self, mock_validator):
        # Mock validator with a dummy dataframe
        mock_validator.active_batch.data.dataframe = pd.DataFrame(
            {"id": [1, 2, 3], "value": [100, 200, 300]}
        )
        target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300]})

        expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list"},
            {"expectation_type": "expect_table_row_count_to_equal"},
        ]

        result = self.validator.apply_standard_expectations_to_datasets(
            validator=mock_validator, expectations=expectations, target_data=target_data
        )

        expected_result = {
            "expect_table_columns_to_match_ordered_list": {
                "success": True,
            },
            "expect_table_row_count_to_equal": {
                "success": False,
                "detailed_errors": {
                    "source_data_row_count": 3,
                    "target_data_row_count": 2,
                },
            },
        }

        self.assertEqual(result, expected_result)

    def test_apply_custom_expectations_to_datasets(self):
        source_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300]})
        target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300]})

        result = self.validator.apply_custom_expectations_to_datasets(
            source_data=source_data, target_data=target_data, primary_key="id"
        )

        expected_result = {
            "custom_expect_primary_keys_to_match": {
                "success": True,
            },
            "custom_expect_row_values_to_match": {
                "success": False,
                "detailed_errors": {
                    "value": {
                        "row_indices": [2],
                        "source_data_values": [200],
                        "target_data_values": [250],
                    }
                },
            },
        }
        self.assertEqual(result, expected_result)
