import pandas as pd


def trim_hostname(satellite_df):
    """
    Trims the hostname name in Satellite data

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data with trimmed hostname name
    """

    satellite_df = satellite_df.query('hostname != "-"')
    satellite_df["hostname_trimmed"] = satellite_df["hostname"].apply(
        lambda x: x.split(".", 1)[0]
    )

    return satellite_df


def get_install_date(satellite_df):
    """
    Converts the registration time in Satellite data to datetime format
    and creates a new column 'install_date'

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data with install date column in datetime format
    """

    satellite_df["install_date"] = pd.to_datetime(
        satellite_df["registration_time"],
        format="%Y-%m-%d %H:%M:%S UTC",
        errors="coerce",
    )
    satellite_df["install_date"] = satellite_df["install_date"].dt.date
    satellite_df["install_date"] = satellite_df["install_date"].fillna("-")

    return satellite_df


def virtwho_check(satellite_df):
    """
    Checks for servers having 'virt-who' as hostname in Satellite data
    and creates the considered/comment columns to mark them as hypervisors

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data marked with hypervisor servers
    """

    # Create the 'Considered' column
    satellite_df["Considered"] = satellite_df["hostname_trimmed"].map(
        lambda x: "N" if "virt-who" in x else ""
    )
    # Create the 'Comment' column
    satellite_df["Comment"] = satellite_df["hostname_trimmed"].map(
        lambda x: "hypervisor" if "virt-who" in x else ""
    )

    return satellite_df


def check_server_type(satellite_df):
    """
    Check if a server in Satellite data is Physical, Virtual or Hypervisor

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data marked with server type
    """

    # convert virt_type column to lower case
    satellite_df["virt_type"] = satellite_df["virt_type"].str.lower()
    satellite_df["phy_vir"] = satellite_df.apply(
        lambda x: "Physical"
        if x["virt_type"] in ["not applicable", "-"]
        else "Virtual",
        axis=1,
    )
    satellite_df["phy_vir"] = satellite_df.apply(
        lambda x: "hypervisor" if "virt-who" in x["hostname_trimmed"] else x["phy_vir"],
        axis=1,
    )

    return satellite_df


def check_missing_servers(satellite_df):
    """
    Check for servers in Satellite data having missing values

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data marked with servers having missing values
    """

    satellite_df["Comment"] = satellite_df.apply(
        lambda x: "details missing for server"
        if (x["uuid"] == "-" and x["os_name"] == "-")
        else x["Comment"],
        axis=1,
    )
    satellite_df["Considered"] = satellite_df.apply(
        lambda x: "N"
        if x["Comment"] == "details missing for server"
        else x["Considered"],
        axis=1,
    )

    return satellite_df


def check_nonrhel_servers(satellite_df):
    """
    Check for non-RHEL servers in Satellite data

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data marked with non-RHEL servers
    """

    satellite_df["Comment"] = satellite_df.apply(
        lambda x: "Non-RHEL"
        if x["os_name"] not in ("RHEL", "RedHat", "RedHat_Workstation", "-", "Redhat")
        else x["Comment"],
        axis=1,
    )
    satellite_df["Considered"] = satellite_df.apply(
        lambda x: "N" if x["Comment"] == "Non-RHEL" else x["Considered"], axis=1
    )

    return satellite_df


def check_duplicates_satellite(satellite_df, intermediate_vcenter_df):
    """
    Check for duplicates in Satellite data

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data
    - intermediate_vcenter_df (dataframe): Intermediate/processed Vcenter data

    Returns:
    - (dataframe): Satellite data with duplicate entries marked
    """

    # checking for satellite server names already present in vcenter data
    satellite_df["Duplicate"] = (
        satellite_df["hostname_trimmed"]
        .isin(intermediate_vcenter_df["vm.dns_name_trimmed"])
        .map({True: "Y", False: "N"})
    )

    satellite_df["Comment"] = satellite_df.apply(
        lambda x: "Duplicate (reported in vcenter)"
        if x["Duplicate"] == "Y"
        else x["Comment"],
        axis=1,
    )

    satellite_df["Considered"] = satellite_df.apply(
        lambda x: "N" if x["Duplicate"] == "Y" else x["Considered"], axis=1
    )

    # Rest all are considered servers
    satellite_df["Considered"].replace("", "Y", inplace=True)

    return satellite_df


def identify_physical_servers(satellite_df):
    """
    Check for servers with missing server type and identify if they are Physical servers

    Parameters:
    - satellite_df (dataframe): Normalized Satellite data

    Returns:
    - (dataframe): Satellite data with servers marked and assumed as Physical
    """

    satellite_df["Comment"] = satellite_df.apply(
        lambda x: "Assumed as Physical (2-Socket) "
        if x["Considered"] == "Y"
        and (x["num_sockets"] in ("1", "-"))
        and (x["is_virtualized"] in ("Unknown", "-"))
        else x["Comment"],
        axis=1,
    )

    return satellite_df
