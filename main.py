import logging
import sys

import pandas as pd

from lib.great_expectations_validator import GreatExpectationsValidator

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    config = {
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": "source"
                }
            },
            {
                "expectation_type": "expect_table_row_count_to_equal",
                "kwargs": {
                    "value": "source"
                }
            },
            {
                "expectation_type": "custom_expect_primary_keys_to_match",
                "kwargs": {
                    "check_duplicates": True
                }
            },
            {
                "expectation_type": "custom_expect_row_values_to_match",
                "kwargs": {
                    "threshold": 0.0  # future use
                }
            }
        ]
    }

    # Initialize the validator
    gx_validator = GreatExpectationsValidator()

    # Example datasets
    # source_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300], "dude": [10, 20, 30]})
    # target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300, 10]})
    # source_data = pd.DataFrame({"id": [1, 2, 3, 4], "value": [100, 200, 300, 4], "dude": [10, 20, 30, 4]})
    # target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300], "extra": [10, 20, 30]})

    source_data = pd.DataFrame({"id": [1, 1, 3], "value": [100, 200, 301]})
    target_data = pd.DataFrame({"id": [1, 2, 3], "value": [100, 250, 300]})

    expectations_list = config["expectations"]
    batch = gx_validator.context.data_sources.pandas_default.read_dataframe(
        dataframe=source_data, asset_name="source_data"
    )
    validator = gx_validator.context.get_validator(batch_list=[batch])

    # TODO: need to think about execution logic here
    # 1. run standard expectations to compare number of rows and columns
    standard_expectation_results = gx_validator.apply_standard_expectations_to_datasets(
        validator=validator, expectations=expectations_list, target_data=target_data
    )
    logging.info(f"standard expectations results: {standard_expectation_results}")

    if not gx_validator.all_expectations_successful(results=standard_expectation_results):
        logging.warning("one or more standard expectations failed (aborting)")
        sys.exit(1)

    # 2. then check of primary keys match
    custom_expectation_results = gx_validator.apply_custom_expectations_to_datasets(
        source_data=source_data, target_data=target_data, primary_key="id"
    )
    logging.info(f"custom expectations results: {custom_expectation_results}")
    if not gx_validator.all_expectations_successful(results=custom_expectation_results):
        logging.warning("one or more custom expectations failed (aborting)")
        sys.exit(1)


    # results = gx_validator.apply_standard_expectations(validator=validator, expectations=expectations_list, target_data=target_data)

    # results_2 = gx_validator.compare_primary_keys_between_datasets(source_data=source_data, target_data=target_data, primary_key="id")
    # print(results_2)
    #
    #
    # results_2 = gx_validator.compare_row_values_between_datasets(source_data=source_data, target_data=target_data, primary_key="id")
    # print(results_2)
    # exit(1)
    # print(results)
