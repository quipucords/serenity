from pathlib import Path
import os
import json
import pandas as pd
import numpy as np
from .v_center import compare_dfs


def read_raw_input(file_path: Path):
    """
    Reads the raw input JSON (deployments, details) files and appends (if multiple JSON files are present)
    into a single input JSON file

    Parameters:
    - file_path (Path): File path location of the raw datasets

    Returns:
    - JSON file: A new JSON file for deployments and details respectively in the file path
    """

    merged_deployments_data = []
    merged_details_data = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            # Check if the file is a deployments.json file
            if file == "deployments.json":
                # Read the deployments.json file data
                with open(os.path.join(root, file), "r") as f:
                    deployments_data = json.load(f)

                # Append the deployments.json file data to the merged deployments data list
                merged_deployments_data.append(deployments_data)

            # Check if the file is a details.json file
            elif file == "details.json":
                # Read the details.json file data
                with open(os.path.join(root, file), "r") as f:
                    details_data = json.load(f)

                # Append the details.json file data to the merged details data list
                merged_details_data.append(details_data)

    # Write the merged JSON data to two new files
    with open(os.path.join(file_path, "merged_deployments.json"), "w") as f:
        json.dump(merged_deployments_data, f, indent=4)

    with open(os.path.join(file_path, "merged_details.json"), "w") as f:
        json.dump(merged_details_data, f, indent=4)


def normalize_data(df_details, df_deployments):
    """
    Normalize the details.json, deployments.json file contents

    Parameters:
    - df_details (dataframe): Dataframe wih the details.json contents
    - df_deployments (dataframe): Dataframe with the deployments.json contents

    Returns:
    - details dataframe: New dataframe with normalized columns
    - deployments dataframe: New dataframe with normalized columns
    """

    df_norm_details = pd.json_normalize(df_details["sources"])

    # extra step needs to be done if there are multiple raw json files
    df_norm_details = pd.json_normalize(df_norm_details[0])

    df_norm_details_facts = pd.json_normalize(df_norm_details["facts"])
    df_norm_details_facts_det = pd.DataFrame()

    # Detect all dictionaries that have facts data.
    for i in range(len(df_norm_details_facts.columns.to_list())):
        df_norm_details_facts_det = pd.concat(
            [df_norm_details_facts_det, pd.json_normalize(df_norm_details_facts[i])],
            ignore_index=True,
        )

    final_details_df = df_norm_details_facts_det.fillna("-")

    # Deployment data
    df_norm_system_fingerprints = pd.json_normalize(
        df_deployments["system_fingerprints"]
    )
    df_norm_deployment = df_norm_system_fingerprints.fillna("-")

    return final_details_df, df_norm_deployment


def drop_empty_rows_columns(df, r_or_c):
    """
    Get the dataframe where empty rows/columns need to be dropped.

    Parameters:
    - df: pandas DataFrame
    - r_or_c: a string, either 'rows' or 'columns', indicating whether to drop empty rows or columns

    Returns:
    A modified DataFrame with empty rows or columns removed.
    """

    value = np.float64(np.nan)

    if r_or_c == "rows":
        # Find rows with only empty values across all columns
        rows_to_drop = df.index[
            df.apply(
                lambda row: np.all(
                    (row == "-")
                    | (row == "N")
                    | (row == value)
                    | pd.isna(row)
                    | pd.isnull(row)
                    | (row is pd.NaT)
                ),
                axis=1,
            )
        ].tolist()

        # Drop the identified rows
        df = df.drop(index=rows_to_drop)
        df.reset_index(inplace=True, drop=True)

    elif r_or_c == "columns":
        # Find columns with only empty values across all rows
        columns_to_drop = df.columns[
            df.apply(
                lambda col: np.all(
                    (col == "-")
                    | (col == "N")
                    | (col == value)
                    | pd.isna(col)
                    | pd.isnull(col)
                    | (col is pd.NaT)
                )
            )
        ].tolist()

        # Drop the identified columns
        df = df.drop(columns=columns_to_drop)
        df.reset_index(inplace=True, drop=True)

    else:
        raise ValueError("Invalid value for 'r_or_c'. Use 'rows' or 'columns'.")

    return df


def process_scans(file_path: Path, scan_type, gbd_keys, auto_keys, gbd_cols, auto_cols):
    """
    Comparison of GBD generated vs automated intermediate data when running in
    validation mode.

    Parameters:
    - file_path: File path where intermediate data is located
    - scan_type: Type of scan being validated
    - gbd_keys: Unique columns from the GBD dataset to reorder the rows in the data
    - auto_keys: Unique columns from the automated dataset to reorder the rows in the data
    - comparison_on: Columns to compare

    Returns:
    The accuracy results (in percentage) for columns specified
    """

    print(f"In validation mode and checking for accuracy for {scan_type} scans.")
    gbd_df = pd.read_csv(
        file_path
        / "intermediate"
        / scan_type
        / f"{scan_type}_intermediate_gbd_generated.csv"
    )
    auto_df = pd.read_csv(
        file_path
        / "intermediate"
        / scan_type
        / f"{scan_type}_intermediate_automated.csv"
    )

    if len(auto_df) != len(gbd_df):
        raise ValueError(
            f"Number of rows in 'gbd' DataFrame ({len(gbd_df)}) is different from 'auto' DataFrame ({len(auto_df)})."
        )

    if len(gbd_cols) != len(auto_cols):
        raise ValueError(
            f"Number of columns to compare in gbd dataset ({len(gbd_cols)}) is different from number of columns to compare in the automated dataset ({len(auto_cols)})."
        )

    if file_path == "test-data-recon1":
        gbd_df["index"] = gbd_df.index
    # Call for specified columns
    for i in range(len(auto_cols)):
        compare_dfs(gbd_keys, auto_keys, gbd_cols[i], auto_cols[i], gbd_df, auto_df)
