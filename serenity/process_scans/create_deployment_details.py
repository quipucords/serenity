import pandas as pd
import numpy as np
import os
from datetime import date


def create_dd_df():
    # Define the deployment details columns
    deployment_details_columns = [
        "Source",
        "Product_Installed",
        "Product_Version",
        "RH_Packages_Installed",
        "Operating_System_Hostname",
        "VM_Name",
        "Physical/Virtual",
        "Physical_Sockets",
        "Socket_Pair",
        "Cores/vCPU_Count",
        "If_Virtual,Hosted_by",
        "If_Virtual,Sockets_on_Host",
        "If_Virtual,_Host_Cluster_name",
        "Install_Date/Profile_Creation_Date",
        "Decomm_Date/Last_Active_date",
        "RHEL_ELS?",
        "Jboss_Type",
        "Hyperthreading(True/False)",
        "Ansible/OpenShift",
        "RHEL_Group_(Instance/Host based)",
        "Duplicate?(Y/N)",
        "Considered?(Y/N)",
        "Comments",
    ]

    # Create an empty DataFrame for deployment details
    deployment_details_df = pd.DataFrame(columns=deployment_details_columns)

    return deployment_details_df


# Function to read data from a CSV file if it exists, else return an empty DataFrame
def read_csv_file(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        return pd.DataFrame()


# Function to fill deployment details from VCenter data
def fill_deployment_from_vcenter(deployment_df, vcenter_data):
    """
    This function copies the relevant columns in from VCenter intermediate scan
    to deployment details. If the intermediate scan is not present, then the process is ignored.
    """
    print("Start copying from vcenter intermediate scan")
    if not vcenter_data.empty:
        deployment_df["Product_Installed"] = vcenter_data.get("Product Name", "-")
        deployment_df["Product_Version"] = vcenter_data.get("Version", "-")
        deployment_df["RH_Packages_Installed"] = "-"
        deployment_df["Operating_System_Hostname"] = vcenter_data.get(
            "vm.dns_name", "-"
        )
        deployment_df["VM_Name"] = vcenter_data.get("vm.name", "-")
        deployment_df["Physical/Virtual"] = "Virtual"
        deployment_df["Physical_Sockets"] = "-"
        deployment_df["Socket_Pair"] = "-"
        deployment_df["Cores/vCPU_Count"] = "-"
        deployment_df["If_Virtual,Hosted_by"] = vcenter_data.get("vm.host.name", "-")
        deployment_df["If_Virtual,Sockets_on_Host"] = vcenter_data.get(
            "vm.host.socket_count", "-"
        )
        deployment_df["If_Virtual,_Host_Cluster_name"] = vcenter_data.get(
            "vm.cluster", "-"
        )
        deployment_df["Install_Date/Profile_Creation_Date"] = "-"
        deployment_df["Decomm_Date/Last_Active_date"] = date.today()
        deployment_df["RHEL_ELS?"] = "-"
        deployment_df["Jboss_Type"] = "-"
        deployment_df["Hyperthreading(True/False)"] = "-"
        deployment_df["Ansible/OpenShift"] = "-"
        deployment_df["RHEL_Group_(Instance/Host based)"] = " "
        deployment_df["Duplicate?(Y/N)"] = " "
        deployment_df["Considered?(Y/N)"] = " "
        deployment_df["Comments"] = " "
        deployment_df["Source"] = "vcenter"

        deployment_df.drop_duplicates(inplace=True)
    return deployment_df


# Function to fill deployment details from Network data
def fill_deployment_from_network(deployment_df, network_data):
    """
    This function copies the relevant columns in from Network intermediate scan
    to deployment details. If the intermediate scan is not present, then the process is ignored.
    """
    print("Start copying from Network intermediate scan")
    if not network_data.empty:
        deployment_df["Product_Installed"] = network_data.get("etc_release_name", "-")
        deployment_df["Product_Version"] = network_data.get("etc_release_version", "-")
        deployment_df["RH_Packages_Installed"] = network_data.get(
            "redhat_packages_gpg_num_rh_packages", "-"
        )
        deployment_df["Operating_System_Hostname"] = network_data.get(
            "uname_hostname", "-"
        )
        deployment_df["VM_Name"] = network_data.get("uname_hostname", "-")
        deployment_df["Physical/Virtual"] = network_data.get("virt_virt", "-")
        deployment_df["Physical_Sockets"] = network_data.get("cpu_socket_count", "-")
        deployment_df["Socket_Pair"] = np.ceil(
            deployment_df["Physical_Sockets"].replace("-", "0").astype(float) / 2
        )
        deployment_df["Cores/vCPU_Count"] = network_data.get("cpu_core_count", "-")
        deployment_df["If_Virtual,Hosted_by"] = "-"
        deployment_df["If_Virtual,Sockets_on_Host"] = "-"
        deployment_df["If_Virtual,_Host_Cluster_name"] = "-"
        deployment_df["Install_Date/Profile_Creation_Date"] = network_data.get(
            "Creation Date / Install date", "-"
        )
        deployment_df["Decomm_Date/Last_Active_date"] = date.today()
        deployment_df["RHEL_ELS?"] = "-"
        deployment_df["Jboss_Type"] = "-"
        deployment_df["Hyperthreading(True/False)"] = "-"
        deployment_df["Ansible/OpenShift"] = "-"
        deployment_df["RHEL_Group_(Instance/Host based)"] = " "
        deployment_df["Duplicate?(Y/N)"] = " "
        deployment_df["Considered?(Y/N)"] = network_data.get("Considered ?", "-")
        deployment_df["Comments"] = network_data.get("Comment", "-")
        deployment_df["Source"] = "Network"

        deployment_df.drop_duplicates(inplace=True)
    return deployment_df


# Function to fill deployment details from Satellite data
def fill_deployment_from_satellite(deployment_df, satellite_data):
    """
    This function copies the relevant columns in from Satellite intermediate scan
    to deployment details. If the intermediate scan is not present, then the process is ignored.
    """
    print("Start copying from Satellite intermediate scan")
    if not satellite_data.empty:
        deployment_df["Product_Installed"] = satellite_data.get("os_name", "-")
        deployment_df["Product_Version"] = satellite_data.get("os_version", "-")
        deployment_df["RH_Packages_Installed"] = "-"
        deployment_df["Operating_System_Hostname"] = satellite_data.get("hostname", "-")
        deployment_df["VM_Name"] = satellite_data.get("hostname", "-")
        deployment_df["Physical/Virtual"] = satellite_data.get("phy_vir", "-")
        deployment_df["Physical_Sockets"] = satellite_data.get("num_sockets", "-")
        deployment_df["Socket_Pair"] = np.ceil(
            deployment_df["Physical_Sockets"].replace("-", "0").astype(float) / 2
        )
        deployment_df["Cores/vCPU_Count"] = satellite_data.get("cores", "-")
        deployment_df["If_Virtual,Hosted_by"] = satellite_data.get(
            "virtual_host_name", "-"
        )
        deployment_df["If_Virtual,Sockets_on_Host"] = "-"
        deployment_df["If_Virtual,_Host_Cluster_name"] = "-"
        deployment_df["Install_Date/Profile_Creation_Date"] = satellite_data.get(
            "install_date", "-"
        )
        deployment_df["Decomm_Date/Last_Active_date"] = date.today()
        deployment_df["RHEL_ELS?"] = "-"
        deployment_df["Jboss_Type"] = "-"
        deployment_df["Hyperthreading(True/False)"] = "-"
        deployment_df["Ansible/OpenShift"] = "-"
        deployment_df["RHEL_Group_(Instance/Host based)"] = " "
        deployment_df["Duplicate?(Y/N)"] = satellite_data.get("Duplicate", "-")
        deployment_df["Considered?(Y/N)"] = satellite_data.get("Considered", "-")
        deployment_df["Comments"] = satellite_data.get("Comment", "-")
        deployment_df["Source"] = "Satellite"

        deployment_df.drop_duplicates(inplace=True)
    return deployment_df
