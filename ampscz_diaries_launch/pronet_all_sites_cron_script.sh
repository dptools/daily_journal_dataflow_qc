#!/bin/bash

# top level cron script for Pronet specifically - hard coded paths for this server

# activate python environment
. /opt/software/miniconda3/etc/profile.d/conda.sh 
conda activate audio_process

# if permissions ever need to be handled for higher level or raw folders systematically 
# - that could be put in this script on a server by server basis too

# call main wrapper that executes full pipeline using all site configs for pronet
pronet_repo_path=/opt/software/daily_journal_dataflow_qc
bash "$pronet_repo_path"/run_full_pipeline_all_sites.sh "$pronet_repo_path"/ampscz_diaries_launch/pronet_configs

# at the end of the processing, make sure that all outputs have correct permissions
sudo chgrp -R pronet /mnt/ProNET/Lochness/PHOENIX/*/*/processed/*/phone/audio_journals
sudo chmod -R 770 /mnt/ProNET/Lochness/PHOENIX/*/*/processed/*/phone/audio_journals
# and make sure logs are readable too
sudo chgrp -R pronet /opt/software/daily_journal_dataflow_qc/logs
sudo chmod -R 770 /opt/software/daily_journal_dataflow_qc/logs
# make this go on a cron under the root account for now so it has full permissions

# TODO once server-wide emails are created by the pipeline to be sent, the actual sending can occur here too
# - different settings for Pronet versus Prescient for email sending!
