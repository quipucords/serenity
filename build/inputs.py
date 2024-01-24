# Test-Data-Recon1: Vcenter scan variables
# gbd_keys_vcenter = ["index"]
# auto_keys_vcenter = ["Unnamed: 0"]
# gbd_cols_vcenter = ["Considered ? (Y/N)", "Duplicate ? (Y/N)"]
# auto_cols_vcenter = ["Considered", "Duplicate"]

# Test-Data-Recon2: Vcenter scan variables
gbd_keys_vcenter = ["vm.dns_name", "vm.host.uuid", "vm.uuid"]
auto_keys_vcenter = ["vm.dns_name", "vm.host.uuid", "vm.uuid"]
gbd_cols_vcenter = ["Considered ?", "Comment"]
auto_cols_vcenter = ["Considered", "Comment"]

# Test-Data-Recon3: Vcenter scan variables
# gbd_keys_vcenter = ["vm.dns_name", "vm.host.uuid", "vm.uuid"]
# auto_keys_vcenter = ["vm.dns_name", "vm.host.uuid", "vm.uuid"]
# gbd_cols_vcenter = ["Considered?", "Duplicate?"]
# auto_cols_vcenter = ["Considered", "Duplicate"]


# Test-Data-Recon2 Network scan variables
gbd_keys_network = ["connection_uuid", "etc_machine_id"]
auto_keys_network = ["connection_uuid", "etc_machine_id"]
gbd_cols_network = ["Creation Date / Install date", "Comment"]
auto_cols_network = ["Creation Date / Install date", "Comment"]

# Test-Data-Recon3 Satellite scan variables
# gbd_keys_satellite = ["uuid"]
# auto_keys_satellite = ["uuid"]
# gbd_cols_satellite = ["OS Hostname Trimmed","Considered?","Comment","Phy Or Vir", "Duplicate?"]
# auto_cols_satellite = ["hostname_trimmed","Considered","Comment","phy_vir","Duplicate"]
