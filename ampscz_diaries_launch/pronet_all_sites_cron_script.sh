#!/bin/bash

# This script is currently on cron at 9am ET on root account for Pronet
# - it manages running all of the components needed for audio journals pipeline

### SERVER SPECIFIC COMPONENTS

# hard coded settings for Pronet server specifically to be used below - can change as needed
server_label="ProNET Production"
repo_path=/opt/software/daily_journal_dataflow_qc
configs_path="$repo_path"/ampscz_diaries_launch/pronet_configs
permissions_group_name=pronet
data_root=/mnt/ProNET/Lochness/PHOENIX
# part of settings are the list of addresses for two main types of email alerts
# first list of addresses that will get high level summary email with embedded highlights
basic_email="mennis2@mgb.org,mennis@g.harvard.edu" 
basic_email_from="mennis2@mgb.org" # sendmail supports easy from address specification
# then list of addresses that will receive email with attachments that give much more detailed QC/accounting
detailed_email="mennis2@mgb.org" 
mailx_attach="A" # lookup flag that mailx command on your server uses for attachments
# finally just a flag to determine whether server-wide emails get sent daily or only weekly on Mondays
daily_testing_mode=1

# python env setup as well
. /opt/software/miniconda3/etc/profile.d/conda.sh 
conda activate audio_process


### MORE GENERAL COMPONENTS

# here use above arguments, generally could copy to use for any other server with different settings too
# (though may want to reevaluate some of the permissions-related stuff at the end
#	or add new permissions-related or high level folder structure commands at beginning as needed)

# first call main wrapper that executes full pipeline using all available site-level configs
#bash "$repo_path"/run_full_pipeline_all_sites.sh "$configs_path"

# now call the server-wide summary function portion using needed arguments, when it is applicable
if [[ ${daily_testing_mode} == 1 ]]; then
	bash "$repo_path"/site_wide_summary_generation.sh "$server_label" "$data_root" "$basic_email" "$basic_email_from" "$detailed_email" "$mailx_attach"
elif [[ $(date +%u) == 1 ]]; then
	bash "$repo_path"/site_wide_summary_generation.sh "$server_label" "$data_root" "$basic_email" "$basic_email_from" "$detailed_email" "$mailx_attach"
fi

# at the end of the processing, make sure that all outputs have correct permissions
sudo chgrp -R "$permissions_group_name" "$data_root"/*/*/processed/*/phone/audio_journals
sudo chmod -R 770 "$data_root"/*/*/processed/*/phone/audio_journals
# and make sure logs are readable too
sudo chgrp -R "$permissions_group_name" "$repo_path"/logs
sudo chmod -R 770 "$repo_path"/logs
# obviously this part assumes account running it is root, would have to remove suod otherwise



