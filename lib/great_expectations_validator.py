import pandas as pd
import great_expectations as gx
import logging


class GreatExpectationsValidator:

    _datasource_initialized = False  # Class-level flag to track datasource initialization

    def __init__(self):
        self.context = gx.get_context()
        self._register_gx_data_sources()

        # note: right now we support only the following 'standard' great expectations (need to more to config later)
        self.supported_standard_expectations = {
            "expect_table_columns_to_match_ordered_list",
            "expect_table_row_count_to_equal"
        }
        self.supported_custom_expectations = [
            "custom_expect_row_values_to_match"
        ]

    def _register_gx_data_sources(self, datasource_name="pandas_default"):
        """
        Check and/or register Great Expectation FluentDatasource (right now only pandas_default). This is only done
        once per session to avoid redundant registrations.

        Args:
            datasource_name: data source (FluentDatasource) name

        Returns:
            None
        """
        if not GreatExpectationsValidator._datasource_initialized:
            if datasource_name not in self.context.list_datasources():
                self.context.data_sources.add_pandas(name=datasource_name)
                logging.info(f"datasource '{datasource_name}' has been added successfully")
            else:
                logging.info(f"datasource '{datasource_name}' already been registered")

            GreatExpectationsValidator._datasource_initialized = True
        else:
            logging.info(f"datasource '{datasource_name}' already initialised")


    def apply_standard_expectations_to_datasets(self, validator: gx.validator.validator.Validator, expectations: list, target_data: pd.DataFrame):
        """
        Apply standard Great Expectations and provide detailed differences for failed checks.

        Args:
            validator: gx validator instance that has details about source dataset
            expectations: list of datasets comparison expectations
            target_data: target dataset

        Returns:
            results (dict) detailed results about two datasets validations
        """
        results = {}
        for expectation in expectations:
            expectation_type = str(expectation["expectation_type"])

            if expectation_type.startswith("custom_"):
                # logging.warning("trying to execute custom expectation, please use appropriate function for that (aborting)")
                continue

            if expectation_type not in self.supported_standard_expectations:
                logging.warning(f"expectation {expectation_type} is not supported at the moment (aborting)")
                continue

            kwargs = expectation.get("kwargs", {})

            # handle dynamic references like "source" for columns or row counts (other expectations should slot in
            # automatically
            if expectation_type == "expect_table_columns_to_match_ordered_list" and "column_list" in kwargs and kwargs["column_list"] == "source":
                kwargs["column_list"] = list(target_data.columns)
            if expectation_type == "expect_table_row_count_to_equal" and "value" in kwargs and kwargs["value"] == "source":
                kwargs["value"] = len(target_data)

            # Apply the expectation
            result = getattr(validator, expectation_type)(**kwargs)
            if result.success:
                results[expectation_type] = {"success": True}
            else:
                # add detailed differences for specific checks
                if expectation_type == "expect_table_columns_to_match_ordered_list":
                    expected_columns = validator.active_batch.data.dataframe.columns
                    actual_columns = list(target_data.columns)
                    results[expectation_type] = {
                        "success": False,
                        "detailed_errors": {
                            "missing_in_target_data": list(set(expected_columns) - set(actual_columns)),
                            "missing_in_source_data": list(set(actual_columns) - set(expected_columns))
                        }
                    }
                elif expectation_type == "expect_table_row_count_to_equal":
                    results[expectation_type] = {
                        "success": False,
                        "detailed_errors": {
                            "source_data_row_count": validator.active_batch.data.dataframe.shape[0],
                            "target_data_row_count": len(target_data)
                        }
                    }
                else:
                    results[expectation_type] = {"success": False, "result": result.result}

        return results

    def apply_custom_expectations_to_datasets(
            self, source_data: pd.DataFrame, target_data: pd.DataFrame, primary_key: str
    ):
        """
        Apply custom expectations: validate primary keys and compare row values for two datasets.

        Args:
            source_data: Source dataset.
            target_data: Target dataset.
            primary_key: Primary key of both datasets.

        Returns:
            results (dict): Combined results of primary key and row value validations.
        """
        results = {}

        # validate primary keys
        primary_key_results = self.compare_primary_keys_between_datasets(
            source_data, target_data, primary_key
        )
        results.update(primary_key_results)

        # If primary keys don't match, stop further processing
        if not primary_key_results["custom_expect_primary_keys_to_match"]["success"]:
            return results

        # compare row values for aligned datasets
        row_comparison_results = self.compare_row_values_between_datasets(
            source_data, target_data, primary_key
        )
        results["custom_expect_row_values_to_match"] = row_comparison_results

        return results

    @staticmethod
    def compare_primary_keys_between_datasets(source_data: pd.DataFrame, target_data: pd.DataFrame, primary_key: str):
        """
        Validate that primary keys match between source and target datasets.

        Args:
            source_data: source dataset
            target_data: target dataset
            primary_key: primary key of both datasets

        Returns:
            results (dict) detailed results about two datasets validations
        """
        results = {
            "custom_expect_primary_keys_to_match": {
                "success": True
            }
        }

        # first of all heck for duplicates in the primary key
        source_duplicates = source_data[source_data.duplicated(subset=[primary_key], keep=False)]
        target_duplicates = target_data[target_data.duplicated(subset=[primary_key], keep=False)]

        if not source_duplicates.empty or not target_duplicates.empty:
            results["custom_expect_primary_keys_to_match"]["success"] = False
            results["custom_expect_primary_keys_to_match"]["detailed_errors"] = {
                "duplicate_keys_in_source": source_duplicates[primary_key].tolist(),
                "duplicate_keys_in_target": target_duplicates[primary_key].tolist(),
            }
            # return immediately as another further checks will be ambiguous
            return results

        # only then compare unique primary keys
        source_keys = set(source_data[primary_key])
        target_keys = set(target_data[primary_key])

        missing_in_target = source_keys - target_keys
        missing_in_source = target_keys - source_keys

        results = {
            "custom_expect_primary_keys_to_match": {
                "success": True
            }
        }
        if missing_in_target and missing_in_source:
            results["custom_expect_row_values_to_match"]["detailed_errors"] = {}
            results["custom_expect_primary_keys_to_match"]["success"] = False
            results["custom_expect_row_values_to_match"]["detailed_errors"] = {
                "missing_in_target": list(missing_in_target),
                "missing_in_source": list(missing_in_source)
            }
        return results

    @staticmethod
    def compare_row_values_between_datasets(source_data: pd.DataFrame, target_data: pd.DataFrame, primary_key: str):
        """
        Compare two dataset (must have same number of rows, columns, and primary key as a pre-requisite) values row by
        row.

        NOTE: unique keys should match before this check is executed, otherwise it will fail due to only identical
        labeled series objects could be compared together.

        Args:
            source_data: source dataset
            target_data: target dataset
            primary_key: primary key of both datasets

        Returns:
            results (dict) detailed results about two datasets validations
        """
        # align rows
        source_data = source_data.set_index(primary_key, inplace=False)
        target_data = target_data.set_index(primary_key, inplace=False)

        results = {
            "custom_expect_row_values_to_match": {
                "success": True
            }
        }
        for column in source_data.columns:
            if column in target_data.columns:
                diff_rows = source_data[column] != target_data[column]
                if diff_rows.any():
                    results["custom_expect_row_values_to_match"]["detailed_errors"] = {}
                    results["custom_expect_row_values_to_match"]["success"] = False
                    results["custom_expect_row_values_to_match"]["detailed_errors"][column] = {
                        "row_indices": source_data.index[diff_rows].tolist(),
                        "source_data_values": source_data.loc[diff_rows, column].tolist(),
                        "target_data_values": target_data.loc[diff_rows, column].tolist(),
                    }

        return results

    # TODO: move to utilities function
    @staticmethod
    def all_expectations_successful(results):
        """
        Check if all expectations in the results are successful.

        Args:
            results (dict): Results of executed expectations.

        Returns:
            bool: True if all expectations are successful, False otherwise.
        """
        return all(result["success"] for result in results.values())