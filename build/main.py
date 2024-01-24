import pandas as pd
import argparse
import os
from process_scans.pre_process import read_raw_input, normalize_data, drop_empty_rows_columns, process_scans
from process_scans.v_center import trim_cm_dns_name, add_considered_comment, split_rh_nonrh, ignore_template_and_discovery, check_duplicates, merge_rn_nonrh, product_name_version, compare_dfs
from process_scans.network import  prep_network_data, check_for_errors, new_host_names, add_date_column, check_num_of_packages
from process_scans.satellite import trim_hostname, get_install_date, virtwho_check, check_server_type, check_missing_servers, check_nonrhel_servers, check_duplicates_satellite, identify_physical_servers
from process_scans.create_deployment_details import create_dd_df, read_csv_file, fill_deployment_from_vcenter, fill_deployment_from_network, fill_deployment_from_satellite
import warnings
warnings.filterwarnings("ignore")
import inputs

def main(validate=None, foldername=None, scan=None):
    
    
    #Creating intermediate scans
    if scan and foldername:
        for scan_type in scan:
            if scan_type == 'vcenter':
                print("Start creating vcenter scan")
                file_path = foldername
                read_raw_input(os.path.join(f"../data/{file_path}", "raw", "vcenter"))
                df_v_details = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "vcenter", "merged_details.json"))
                df_v_deployment = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "vcenter", "merged_deployments.json"))
                v_center, v_deployment = normalize_data(df_v_details, df_v_deployment)
                final_vcenter = drop_empty_rows_columns(v_center, 'rows')
                v_center_trimmed = trim_cm_dns_name(final_vcenter)
                v_center_cc = add_considered_comment(v_center_trimmed)
                rh_vcenter, nonrh_vcenter = split_rh_nonrh(v_center_cc)
                rh_vcenter_temp_disc = ignore_template_and_discovery(rh_vcenter)
                rh_vcenter_dupes = check_duplicates(rh_vcenter_temp_disc)
                v_center_merged = merge_rn_nonrh(rh_vcenter_dupes, nonrh_vcenter)
                v_center_prod_split = product_name_version(v_center_merged)
                v_center_prod_split.to_csv(os.path.join(f"../data/{file_path}", "intermediate", "vcenter", "vcenter_intermediate_automated.csv"))
                print("Finished Vcenter scan successfully!!!!")

            if scan_type == 'network':
                print("Start creating Network scan")
                file_path = foldername
                read_raw_input(os.path.join(f"../data/{file_path}", "raw", "network"))
                df_n_details = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "network", "details.json"))
                df_n_deployment = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "network", "deployments.json"))
        
                network_details, network_deployment = prep_network_data(df_n_details, df_n_deployment)
                
                network_details = drop_empty_rows_columns(network_details, 'rows')
                target_string = "Could not"    
                network_details = check_for_errors(network_details, target_string) 
                 
                network_details = new_host_names(network_details, v_center_merged)
                network_details = add_date_column(network_details, network_deployment)
                network_details = check_num_of_packages(network_details)
                network_details.to_csv(os.path.join(f"../data/{file_path}", "intermediate", "network", "network_intermediate_automated.csv"))
                print("Finished Network scan successfully!!!!")

            if scan_type == 'satellite':
                print("Start creating satellite scan")
                file_path = foldername
                read_raw_input(os.path.join(f"../data/{file_path}", "raw", "satellite"))
                df_s_details = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "satellite", "merged_details.json"))
                df_s_deployment = pd.read_json(os.path.join(f"../data/{file_path}", "raw", "satellite", "merged_deployments.json"))
                s_details, s_deployment = normalize_data(df_s_details, df_s_deployment)
                final_satellite = drop_empty_rows_columns(s_details, 'rows')
                satellite_df = trim_hostname(final_satellite)
                satellite_df = get_install_date(satellite_df)
                satellite_df = virtwho_check(satellite_df)
                satellite_df = check_server_type(satellite_df)
                satellite_df = check_missing_servers(satellite_df)
                satellite_df = check_nonrhel_servers(satellite_df)
                satellite_df = check_duplicates_satellite(satellite_df, v_center_prod_split)
                satellite_df = identify_physical_servers(satellite_df)
                satellite_df.to_csv(os.path.join(f"../data/{file_path}", "intermediate", "satellite", "satellite_intermediate_automated.csv"))
                print("Finished Satellite scan successfully!!!!")
    
        # Creating deployment details 
        # Initialize the deployment_details_df
        deployment_details_df = create_dd_df()
        combined_deployment_df = pd.DataFrame()
        print("Start creating deployment details scan")
        file_path=foldername
        
        for scan_type in scan:
            if scan_type == 'vcenter':
                # from vcenter scan
                vcenter_path = (os.path.join(f"../data/{file_path}", "intermediate", "vcenter", "vcenter_intermediate_automated.csv"))
                vcenter_data = read_csv_file(vcenter_path)
                processed_data = fill_deployment_from_vcenter(deployment_details_df, vcenter_data)

            elif scan_type == 'network':
                # from network scan
                network_path = (os.path.join(f"../data/{file_path}", "intermediate", "network", "network_intermediate_automated.csv")) 
                network_data = read_csv_file(network_path)
                processed_data = fill_deployment_from_network(deployment_details_df, network_data)
    
            elif scan_type == 'satellite':
                # from satellite
                satellite_path = (os.path.join(f"../data/{file_path}", "intermediate", "satellite", "satellite_intermediate_automated.csv"))
                satellite_data = read_csv_file(satellite_path)
                processed_data = fill_deployment_from_satellite(deployment_details_df, satellite_data)
    
            else:
                print(f"Unsupported scan type: {scan_type}")
            
            # Append the processed data to the combined DataFrame
            combined_deployment_df = pd.concat([combined_deployment_df, processed_data], ignore_index=True)
    
        # Check for duplicates and update comments
        if not combined_deployment_df.empty:
            combined_deployment_df['Duplicate?(Y/N)'] = combined_deployment_df.duplicated(subset=['VM_Name'], keep=False).map({True: 'Y', False: 'N'})
            combined_deployment_df.loc[combined_deployment_df['Duplicate?(Y/N)'] == 'Y', 'Comments'] = 'Duplicate entries'
            
            # Save the deployment details
            combined_deployment_df.to_csv(os.path.join(f"../data/{file_path}", "final_report", "deployment_details_auto_generated.csv"), index=False)
            print("Deployment_details scan created")
    
    # Validation Mode
    if validate and scan and foldername:
        for scan_type in scan:
            file_path = foldername
            if scan_type == 'vcenter':
                v_center_prod_split['Duplicate'].fillna('N', inplace=True)
                process_scans(file_path, scan_type, inputs.gbd_keys_vcenter, inputs.auto_keys_vcenter, inputs.gbd_cols_vcenter, inputs.auto_cols_vcenter)
                        
            if scan_type == 'network':
                process_scans(file_path, scan_type, inputs.gbd_keys_network, inputs.auto_keys_network, inputs.gbd_cols_network, inputs.auto_cols_network) 
                  
            if scan_type == 'satellite':
                process_scans(file_path, scan_type, inputs.gbd_keys_satellite, inputs.auto_keys_satellite, inputs.gbd_cols_satellite, inputs.auto_cols_satellite)


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Arguments needed to process the scripts")

    # Define command-line arguments
    parser.add_argument("--validate", action='store_true', default=None, help="Executes the scripts in validation mode")
    parser.add_argument("--foldername", required=True, default=None, help="Folder location of the raw datasets")
    parser.add_argument("--scan", nargs='*', default=None, help="Specify which scans to process (vcenter, network, satellite)") #python main.py --scan vcenter network satellite 

    # Parse the arguments
    args = parser.parse_args()
    main(args.validate, args.foldername, args.scan)
