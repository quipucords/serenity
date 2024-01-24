# Serenity

ET DS (Emerging Technology Data Science) develops a vehicle of transforming Discovery output automatically to GBD processing templates reports.

## Run Instructions

### Step 1: Install Packages & Python Version

```
pyenv install $(cat .python-version)
poetry env use $(pyenv which python)
poetry install
```

### Step 2: Upload raw datasets to data/ folder

Sample directory structure. Add the raw jsons to the path within the `data/raw` folder such as `data/test-data-recon2/raw/vcenter/details.json`. The structure for the folder is as follows:

- data
    - test-data-recon1
      - raw
          - vcenter
              - folder1
                  - details.json
                  - deployment.json
          - network
              - folder1
                  - details.json
                  - deployments.json
              - folder2
                  - details.json
                  - deployments.json
          - satellite
              - details.json
              - deployments.json
      - intermediate
          - network
          - vcenter
      - final report

Please also note that inside the folder of a particular scan example, there can either be a single report or multiple reports each having `details.json` and `deployments.json` files. The folder structure can take either of the following structures as follows:

A scan having **multiple reports**:
- network
    - folder1
        - details.json
        - deployment.json
    - folder2
        - details.json
        - deployments.json

A scan having a **single report**:
- network
    - details.json
    - deployment.json

- data
    - test-data-recon1
      - raw
        - vcenter
            - details.json
            - deployment.json
        - network
        - candlepin
      - intermediate
      - final report

### Step 2.1: Uploading intermediate test data when running in validation mode

If running in `--validate` mode, you will need to upload the intermediate test data for the corresponding scans in the `data/intermediate` folder. You can upload the intermediate test data such as the manually generated GBD data `vcenter_intermediate_gbd_generated.csv` to the folder as follows:

- data
    - test-data-recon2
        - raw
        - intermediate
          - vcenter
            - vcenter_intermediate_gbd_generated.csv
          - network
              - network_intermediate_gbd_generated.csv
        - final report

***Please Note*: If you are not running in validation mode you can skip this step and directly proceed to Step 3.**

### Step 3: Run script

Running in **automation mode**:

`poetry run python -m serenity --foldername <raw data folder name> --scan <type of scans>`

OR

Running in **validation mode** (only when we have test data to validate against):

`poetry run python -m serenity --foldername <raw data folder name> --scan <type of scans> --validate`

Make sure to run the above by passing the correct set of parameters as defined below:

* **foldername**
    * Specify the recon folder name you would want to run this code on
    * Example:
    `--foldername test-data-recon1`

* **scan**
    * Specify the Discovery scan type(s) to be processed. If there are more than one scans, add them with a space separation.
    * Example:
    `--scan vcenter`, `--scan vcenter network`, `--scan network`, `--scan vcenter satellite`

* **validate**
    * Optional argument only to be specified when running in validation mode i.e. if there is correponding test data that we would want to validate our automated outputs against.
    * Example:
    `--validate`

### Example Output:

This example runs the tool for:
* *folder* - `test-data-recon2`
* *scans* - **vcenter, network**
* *Execution mode* - Running in **validation mode**

```
$ poetry run python -m serenity --foldername test-data-recon2 --scan vcenter network --validate
Start creating vcenter scan
Group 'cddmtylin' has 8 rows with 'vm.state' as 'poweredOn'
Keeping all servers since Powered On
Group 'mtylinNessus' has at least one row with 'vm.state' as 'poweredOn'
Keeping the server which is Powered On
Finished Vcenter scan successfully!!!!
Start creating Network scan
Not all values in 'redhat_packages_gpg_num_rh_packages' column are within the range of 0 to 10.
Finished Network scan successfully!!!!
Start creating deployment details scan
Start copying from vcenter intermediate scan
Start copying from Network intermediate scan
Deployment_details scan created
In validation mode and checking for accuracy for vcenter scans.
For  Considered ? accuracy in % is  99.64285714285714
230    Y
Name: Considered_auto, dtype: object
230    N
Name: Considered ?_gbd, dtype: object
For  Comment accuracy in % is  0.0
0      Non Red Hat Server
1      Non Red Hat Server
2      Non Red Hat Server
3      Non Red Hat Server
4      Non Red Hat Server
              ...
275    Non Red Hat Server
276    Non Red Hat Server
277    Non Red Hat Server
278    Non Red Hat Server
279                   NaN
Name: Comment_auto, Length: 280, dtype: object
0      Non Redhat
1      Non Redhat
2      Non Redhat
3      Non Redhat
4      Non Redhat
          ...
275    Non Redhat
276    Non Redhat
277    Non Redhat
278    Non Redhat
279              -
Name: Comment_gbd, Length: 280, dtype: object
In validation mode and checking for accuracy for network scans.
For  Creation Date / Install date accuracy in % is  96.55172413793103
24    3/30/2023
Name: Creation Date / Install date_auto, dtype: object
24    12/30/1899
Name: Creation Date / Install date_gbd, dtype: object
For  Comment accuracy in % is  100.0

```


This will run all the pre-processing of the scan data, save the intermediate datasets in the data folder and create the final reconciliation report.

The processed data is saved in the intermediate and final report folders as follows:

- data
    - test-data-recon2
      - raw
      - intermediate
          - vcenter
              - vcenter_intermediate_automated.csv
          - network
              - network_intermediate_automated.csv
      - final report
          - deployment_details_auto_generated.csv
