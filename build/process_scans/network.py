import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

def prep_network_data(network_data, network_deployment_data):
    
    """
    Gets the Network  details and deployment data in a column format from the nested json

    Parameters:
    - network_data (dataframe): raw Network data

    Returns:
    - (dataframe): Normalized Network data
    """
    # Network Details Data
    df_norm_n_details = pd.json_normalize(network_data['sources'])
    df_norm_n_details_facts = pd.json_normalize(df_norm_n_details['facts'])
    df_norm_n_details_facts_det = pd.DataFrame()
    # Detect all dictionaries that have facts data.
    for i in range(len(df_norm_n_details_facts.columns.to_list())):
        df_norm_n_details_facts_det = pd.concat([df_norm_n_details_facts_det, pd.json_normalize(df_norm_n_details_facts[i])], ignore_index=True)
    
    final_details_df = df_norm_n_details_facts_det.fillna("-")

    #Network Deployment Data
    df_norm_system_fingerprints = pd.json_normalize(network_deployment_data['system_fingerprints'])
    df_norm_n_deployment = df_norm_system_fingerprints.fillna("-")
    
    return final_details_df, df_norm_n_deployment

def check_for_errors(final_details_df, target_string):

    """
    This function looks for the target_string that is the error message within each cell of the DataFrame 
    and update two additional columns, "Comment" and "Considered ?", based on whether the target string is found in each cell.

    Parameters: Takes a pandas DataFrame final_details_df and a target string target_string as input.

    Returns: Modified Dataframe with the comment and considered value.
    """
    # Add a new column named "Comment" with default values
    final_details_df['Comment'] = ''
    # Add a new column named "Considered ?" with default values
    final_details_df['Considered ?'] = ''

    # Iterate through each row and column of the DataFrame
    for row_idx, row in final_details_df.iterrows():
        for col in final_details_df.columns:
            cell_value = str(row[col])

            # Check if the target string exists in the cell value
            if target_string in cell_value:
                # Update the "comment" column to "Error Line"
                final_details_df.at[row_idx, 'Comment'] = 'Error Line'
                final_details_df.at[row_idx, 'Considered ?'] = 'N'
                break  # Break the inner loop to move to the next row

    # Display the modified DataFrame
    return final_details_df

def new_host_names(final_details_df, vcenter_intermediate):
    """
    Process host names and update DataFrame with "Comment" and "Considered ?" columns.

    Parameters:
    - final_details_df: pandas DataFrame containing host details
    - vcenter_intermediate: pandas DataFrame with intermediate data

    Returns:
    Updated DataFrame with new columns "Comment" and "Considered ?"
    """
    
    # Extract the hostname part from 'uname_hostname' column
    final_details_df['uname_hostname'] = final_details_df['uname_hostname'].apply(lambda x: x.split('.', 1)[0])

    # Check if the hostname is in 'vm.dns_name_trimmed' column of vcenter_intermediate
    final_details_df['Considered ?'] = final_details_df['uname_hostname'].isin(vcenter_intermediate['vm.dns_name_trimmed']).map({True: 'N', False: 'Y'})

    # Create a mapping for the "Comment" column values
    comment_mapping = final_details_df['uname_hostname'].isin(vcenter_intermediate['vm.dns_name_trimmed']).map({True: 'Already Considered in Vcenter'})

    # Define the target string for error detection
    target_string = "Could not"
    
    # Call the error detection function with the target string
    check_for_errors(final_details_df, target_string)
    
    # Add the "Comment" column to the DataFrame based on the mapping
    final_details_df['Comment'] = comment_mapping

    return final_details_df


def add_date_column(final_details_df, df_n_deployment):
    """
    Add a 'Creation Date / Install date' column to final_details_df.

    Parameters:
    - final_details_df: pandas DataFrame containing host details
    - df_n_deployment: pandas DataFrame with deployment data

    Returns:
    Updated final_details_df with the 'Creation Date / Install date' column.
    """
    
    # Define the prefix to search for
    prefix = 'date_'

    # Use list comprehension to find columns starting with the prefix
    values_with_date = [column for column in final_details_df.columns if column.startswith(prefix)]

    # Convert columns with date values to datetime type
    for column in values_with_date:
        final_details_df[column] = final_details_df[column].apply(pd.to_datetime, errors="coerce")

    # Calculate the minimum date across specified columns
    final_details_df['Creation Date / Install date'] = final_details_df[values_with_date].min(axis=1)

    # Convert the 'Creation Date / Install date' column to datetime format
    final_details_df['Creation Date / Install date'] = pd.to_datetime(final_details_df['Creation Date / Install date'])

    # Format the 'Creation Date / Install date' column in MM/DD/YYYY format
    final_details_df['Creation Date / Install date'] = final_details_df['Creation Date / Install date'].dt.strftime('%-m/%-d/%-Y')

    # Merge the deployments and details based on the common host name columns
    merged_df = pd.merge(df_n_deployment, final_details_df, left_on='name', right_on='uname_hostname', how='inner')

    # Convert 'system_creation_date' to datetime format and reformat it
    merged_df['system_creation_date'] = pd.to_datetime(merged_df['system_creation_date'], format='%Y/%m/%d')
    merged_df['system_creation_date'] = merged_df['system_creation_date'].dt.strftime('%-m/%-d/%-Y')

    # Check if the corresponding time columns match
    merged_df['time_match'] = merged_df['system_creation_date'] == merged_df['Creation Date / Install date']

    # Print the merged DataFrame with the time match flag
    if not merged_df['time_match'].all():
        raise ValueError("Error: False value found in 'time_match' column.")

    return final_details_df


def check_num_of_packages(final_details_df):
    """
    Check the 'redhat_packages_gpg_num_rh_packages' column and mark duplicates in the DataFrame.

    Parameters:
    - final_details_df: pandas DataFrame containing host details

    Returns:
    Updated final_details_df with marked duplicates in the 'Considered ?' column.
    """
    # Convert the 'redhat_packages_gpg_num_rh_packages' column to numeric type
    final_details_df["redhat_packages_gpg_num_rh_packages"] = pd.to_numeric(final_details_df["redhat_packages_gpg_num_rh_packages"], errors='coerce')

    # Check if values in "redhat_packages_gpg_num_installed_packages" column are within the range of 0 to 10
    if final_details_df["redhat_packages_gpg_num_rh_packages"].between(0, 10).all():
        
        duplicates = final_details_df.duplicated(subset=["uname_hostname"], keep=False)
        final_details_df.loc[duplicates, "Considered ?"] = True  # Mark duplicates as True in the "Considered ?" column

        if duplicates.any():
            duplicated_names = final_details_df.loc[duplicates, "uname_hostname"].unique()
            print("Duplicate names found:", duplicated_names)
        else:
            print("No duplicate names found.")
    else:
        print("Not all values in 'redhat_packages_gpg_num_rh_packages' column are within the range of 0 to 10.")

    return final_details_df

        
