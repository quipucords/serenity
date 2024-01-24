import pandas as pd


def trim_cm_dns_name(v_center_df):
    """
    Trims the DNS name in VCenter data

    Parameters:
    - v_center_df (dataframe): Normalized Vcenter data

    Returns:
    - (dataframe): Vcenter data with trimmed DNS name
    """

    v_center_df["vm.dns_name_trimmed"] = v_center_df["vm.dns_name"].apply(
        lambda x: x.split(".", 1)[0]
    )

    return v_center_df


def add_considered_comment(v_center_df):
    """
    Create the considered and comment columns to select which servers to consider
    Consider only Red Hat Servers.

    Parameters:
    - v_center_df (dataframe): Normalized Vcenter data

    Returns:
    - (dataframe): Vcenter data marked with Red Hat and Non Red Hat Servers
    """

    # Create the 'Considered' column
    v_center_df["Considered"] = v_center_df["vm.os"].apply(
        lambda x: "Y" if "Red Hat Enterprise Linux" in x else "N"
    )

    # Create the 'Comment' column
    v_center_df["Comment"] = v_center_df.apply(
        lambda row: "Non Red Hat Server" if row["Considered"] == "N" else "", axis=1
    )

    return v_center_df


def split_rh_nonrh(v_center_df):
    """
    Splits Vcenter data into 2 dataframes Red hat and Non Red Hat based on the server type

    Parameters:
    - v_center_df (dataframe): Normalized Vcenter data

    Returns:
    - (dataframe), (dataframe) : Dataframes with Red Hat and Non Red Hat Servers
    """

    red_hat_servers = v_center_df[v_center_df["Comment"] != "Non Red Hat Server"].copy()
    non_red_hat_servers = v_center_df[
        v_center_df["Comment"] == "Non Red Hat Server"
    ].copy()

    return red_hat_servers, non_red_hat_servers


def ignore_template_and_discovery(red_hat_servers):
    """
    Check for Template or Discovery servers and ignore them

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe

    Returns:
    - (dataframe): Red Hat servers dataframe with discovery and template type servers ignored
    """

    # Update the 'Considered' column
    red_hat_servers["Considered"] = red_hat_servers.apply(
        lambda row: "N"
        if "template" in row["vm.name"].lower()
        or "template" in row["vm.dns_name_trimmed"].lower()
        or "discovery" in row["vm.name"].lower()
        or "discovery" in row["vm.dns_name_trimmed"].lower()
        else row["Considered"],
        axis=1,
    )

    # Update the 'Comment' column
    red_hat_servers["Comment"] = red_hat_servers.apply(
        lambda row: "Template or Discovery server"
        if "template" in row["vm.name"].lower()
        or "template" in row["vm.dns_name_trimmed"].lower()
        or "discovery" in row["vm.name"].lower()
        or "discovery" in row["vm.dns_name_trimmed"].lower()
        else row["Comment"],
        axis=1,
    )

    return red_hat_servers


def check_duplicates(red_hat_servers):
    """
    This does a multi step processing to check for duplicates in Red Hat servers

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe

    Returns:
    - (dataframe): Red Hat servers dataframe with duplicates marked
    """

    red_hat_servers = duplicate_strategy_1(red_hat_servers)
    red_hat_servers = duplicate_strategy_2(red_hat_servers)
    red_hat_servers = duplicate_strategy_3(red_hat_servers)

    return red_hat_servers


def duplicate_strategy_1(red_hat_servers):
    """
    Marks Duplicates in vm.dns_name_trimmed

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe

    Returns:
    - (dataframe): Red Hat servers duplicates marked for same trimmed DNS name (ignore localhost)
    """

    red_hat_servers["Same"] = red_hat_servers.duplicated(
        subset="vm.dns_name_trimmed"
    ) & (red_hat_servers["vm.dns_name_trimmed"] != "localhost")

    return red_hat_servers


def duplicate_strategy_2(red_hat_servers):
    """
    If Duplicate vm.dns_name_trimmed, check Powered state

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe

    Returns:
    - (dataframe): Red Hat servers marked with duplicates based on Powered state
    """
    # Filter rows marked as "True" in the "Same" column
    duplicate_rows = red_hat_servers[red_hat_servers["Same"]]

    # Iterate over the filtered duplicate rows to update the 'Considered' and 'Comment' columns
    for _, row in duplicate_rows.iterrows():
        dns_name = row["vm.dns_name_trimmed"]

        # for a potentially duplicate trimmed dns name, what are the other same trimmed dns name rows
        same_dns_rows = red_hat_servers[
            red_hat_servers["vm.dns_name_trimmed"] == dns_name
        ]

        # First, we check whether the servers with the same trimmed DNS name and also VM name are powered on
        # If all are "poweredOn", we update consider all

        # If there are multiple rows, some with "poweredOn" and some with "poweredOff" state
        # we consider the one which is Powered on

        # If there are multiple rows, all are "poweredOff" state
        # we consider any

        # Servers have exactly the same trimmed DNS name and also same VM name
        if same_dns_rows["vm.name"].nunique() == 1:
            # For these servers, what are the powered states
            states = same_dns_rows["vm.state"].unique()

            p_on_index_list = list(
                same_dns_rows[same_dns_rows["vm.state"] == "poweredOn"].index
            )
            p_off_index_list = set(same_dns_rows.index) - set(p_on_index_list)

            # If both are powered on consider all
            if len(states) == 1 and states == "poweredOn":
                same_dns_rows.loc[same_dns_rows.index, "Considered"] = "Y"

            # If both are powered off consider any
            elif len(states) == 1 and states == "poweredOff":
                same_dns_rows.loc[same_dns_rows.index[0], "Considered"] = "Y"
                same_dns_rows.loc[
                    same_dns_rows.index[0], "Comment"
                ] = "Duplicate Considering any one"
                same_dns_rows.loc[same_dns_rows.index[0] + 1 :, "Considered"] = "N"
                same_dns_rows.loc[same_dns_rows.index[0] + 1 :, "Duplicate"] = "Y"

            # If one of them is powered on and others are powered off, consider the one that is powered on
            elif len(states) == 2:
                same_dns_rows.loc[p_on_index_list[0], "Considered"] = "Y"
                same_dns_rows.loc[
                    p_on_index_list[0], "Comment"
                ] = "Duplicate Considering only Powered On"
                same_dns_rows.loc[list(p_off_index_list), "Considered"] = "N"
                same_dns_rows.loc[list(p_off_index_list), "Duplicate"] = "Y"

            else:
                print("Unknown states. vm.state must be poweredOn or poweredOff")

            # Update the 'Considered' and 'Comment' columns in the original DataFrame
            red_hat_servers.update(same_dns_rows[["Considered", "Comment"]])

    return red_hat_servers


def duplicate_strategy_3(red_hat_servers):
    """
    If servers have slightly different names, with only a slight difference like test, clone, new
    mark duplicates based on powered state

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe

    Returns:
    - (dataframe): Red Hat servers marked with duplicates based on Powered state for slight name differences
    """

    # If servers have slightly different names, with only a slight difference like test, clone, new

    duplicates = pd.DataFrame()

    for index, row in red_hat_servers.iterrows():
        name = row["vm.name"]

        if "test" in name or "clone" in name or "new" in name:
            original_name = (
                name.replace("_test", "")
                .replace("_clone", "")
                .replace("_new", "")
                .replace("_old", "")
                .replace("old", "")
                .replace("test", "")
                .replace("clone", "")
                .replace("new", "")
            )
            similar_rows = red_hat_servers[
                red_hat_servers["vm.name"].str.contains(original_name)
            ]

            if not similar_rows.empty:
                duplicates = pd.concat([duplicates, similar_rows], ignore_index=False)
                # Mark duplicate rows by the root original name
                duplicates.loc[similar_rows.index, "temp_group_marker"] = original_name

    if not duplicates.empty:
        grouped = duplicates.groupby("temp_group_marker")

        for group_name, group_df in grouped:
            # There has to be atleast 2 similar names
            # Coz previous loop saves even those rows which have
            # clone, test, new in name
            # But there is not neccesarily a copy of that name without
            # the clone, test, new

            if len(group_df) > 1:
                states = group_df["vm.state"].unique()

                if len(states) == 1 and states[0] == "poweredOn":
                    # All rows have 'vm.state' equal to 'PoweredOn'
                    # Keep all servers
                    print(
                        f"Group '{group_name}' has {len(group_df)} rows with 'vm.state' as 'poweredOn'"
                    )
                    print("Keeping all servers since Powered On")
                    red_hat_servers.loc[group_df.index, "Same"] = True
                    red_hat_servers.loc[
                        group_df.index, "Comment"
                    ] = "Duplicate test, new, clone"
                    red_hat_servers.loc[group_df.index, "Considered"] = "Y"

                elif "poweredOn" in states:
                    # At least one row has 'vm.state' as 'poweredOn'
                    # Keep the one which is Powered On
                    print(
                        f"Group '{group_name}' has at least one row with 'vm.state' as 'poweredOn'"
                    )
                    print("Keeping the server which is Powered On")
                    p_on_index_list = list(
                        group_df[group_df["vm.state"] == "poweredOn"].index
                    )
                    p_off_index_list = list(set(group_df.index) - set(p_on_index_list))

                    red_hat_servers.loc[group_df.index, "Same"] = True
                    red_hat_servers.loc[
                        group_df.index, "Comment"
                    ] = "Duplicate test, new, clone"
                    red_hat_servers.loc[p_on_index_list, "Considered"] = "Y"
                    red_hat_servers.loc[p_off_index_list, "Considered"] = "N"
                    red_hat_servers.loc[p_on_index_list, "Duplicate"] = "Y"
                    red_hat_servers.loc[p_off_index_list, "Duplicate"] = "Y"

                elif len(states) == 1 and states[0] == "poweredOff":
                    # All rows have 'vm.state' equal to 'PoweredOff'
                    # Consider any one server rest dont consider

                    # There is finer logic around which server to consider when both are powered off
                    # based on the newer host which solution architects would be able to suggest
                    # For now we will just consider the first one until we have more clarity on newness of hosts
                    print(
                        f"Group '{group_name}' has all rows with 'vm.state' as 'poweredOff'"
                    )
                    print("Keeping one of the servers")
                    red_hat_servers.loc[group_df.index, "Same"] = True
                    red_hat_servers.loc[
                        group_df.index, "Comment"
                    ] = "Duplicate test, new, clone"
                    red_hat_servers.loc[group_df.index[0], "Considered"] = "Y"
                    red_hat_servers.loc[p_off_index_list[1:], "Considered"] = "N"
                    red_hat_servers.loc[p_off_index_list[1:], "Duplicate"] = "Y"

                else:
                    print("Unknown states. vm.state must be poweredOn or poweredOff")

    return red_hat_servers


def merge_rn_nonrh(red_hat_servers, non_red_hat_servers):
    """
    Merge the Red Hat servers (after processing) with the Non Red Hat server data and aggregate into signle dataframe

    Parameters:
    - red_hat_servers (dataframe): Red Hat server dataframe
    - non_red_hat_servers (dataframe): Non Red Hat server dataframe

    Returns:
    - (dataframe): Concatenated dataframe with both Red Hat and Non Red Hat servers
    """

    final_details_df = pd.concat([red_hat_servers, non_red_hat_servers])

    return final_details_df


def product_name_version(v_center_df):
    """
    Split "vm.os" column into "Product Name" and "Version" columns

    Parameters:
    - v_center_df (dataframe): Vcenter combined dataframe

    Returns:
    - (dataframe): Vcenter data with OS split into product name and version
    """

    # Split "vm.os" column into "Product Name" and "Version" columns

    v_center_df[["Product Name", "Version"]] = v_center_df["vm.os"].str.extract(
        r"^(.*?)\s*(\(.*\)|\d.*)?$"
    )

    # Apply the following regex pattern only to rows containing "Microsoft Windows XP"
    mask1 = v_center_df["vm.os"].str.contains("Microsoft Windows XP")

    # Extract the parts using regex for the masked rows
    v_center_df.loc[mask1, "Product Name"] = v_center_df.loc[
        mask1, "vm.os"
    ].str.extract(r"(Microsoft Windows).*?(XP.*)", expand=False)[0]
    v_center_df.loc[mask1, "Version"] = v_center_df.loc[mask1, "vm.os"].str.extract(
        r"(Microsoft Windows).*?(XP.*)", expand=False
    )[1]

    # Mask for rows starting with "Other"
    mask2 = v_center_df["vm.os"].str.startswith("Other")

    # Extract the parts using regex for the masked rows
    v_center_df.loc[mask2, "Product Name"] = "Other"
    v_center_df.loc[mask2, "Version"] = v_center_df.loc[mask2, "vm.os"].str.extract(
        r"Other(.*)"
    )[0]

    # Strip leading/trailing spaces in the columns
    v_center_df["Product Name"] = v_center_df["Product Name"].str.strip()
    v_center_df["Version"] = v_center_df["Version"].str.strip()

    # Where Version is None save it as '-'
    v_center_df["Version"].fillna("-", inplace=True)

    return v_center_df


def reorder_based_on(gbd_keys, auto_keys, gbd, auto):
    """
    Sort GBD and Automatically generated dataframes based on primary keys

    Parameters:
    - gbd_keys (list): List of keys in GBD result that will be used for re-ordering
    - auto_keys (list): List of keys in Automated result that will be used for re-ordering
    - gbd (dataframe): GBD generated dataframe
    - auto (dataframe): Automatically generated dataframe

    Returns:
    - (dataframe), (dataframe): GBD and Automatically generated dataframes sorted so that their rows match
    """

    gbd_keys = [key + "_gbd" for key in gbd_keys]
    auto_keys = [key + "_auto" for key in auto_keys]
    auto = auto.add_suffix("_auto")
    gbd = gbd.add_suffix("_gbd")

    merged_df = pd.merge(gbd, auto, left_on=gbd_keys, right_on=auto_keys)

    if merged_df.empty:
        raise ValueError(
            "Rows in G data and Auto generated data cannot be ordered the same way. Comparison results are invalid"
        )

    return merged_df


def compare_dfs(gbd_keys, auto_keys, gbd_col, auto_col, gbd, auto):
    """
    Compares the GBD and Automatically generated dataframes and prints out the accuracy

    Parameters:
    - gbd_keys (list): List of keys in GBD result that will be used for re-ordering
    - auto_keys (list): List of keys in Automated result that will be used for re-ordering
    - gbd_col (string): Column from the GBD dataframe that you want to compare with Automated dataset
    - auto_col (string): Column from the Automated dataframe that you want to compare with GBD dataset
    - gbd (dataframe): GBD generated dataframe
    - auto (dataframe): Automatically generated dataframe

    """

    merged = reorder_based_on(gbd_keys, auto_keys, gbd, auto)

    mismatched_rows_auto = merged[
        merged[auto_col + "_auto"] != merged[gbd_col + "_gbd"]
    ][auto_col + "_auto"]
    mismatched_rows_gbd = merged[
        merged[auto_col + "_auto"] != merged[gbd_col + "_gbd"]
    ][gbd_col + "_gbd"]

    accuracy = (len(merged) - len(mismatched_rows_auto)) * 100 / len(merged)
    print("For ", gbd_col, "accuracy in % is ", accuracy)

    if accuracy != 100:
        print(mismatched_rows_auto)
        print(mismatched_rows_gbd)
