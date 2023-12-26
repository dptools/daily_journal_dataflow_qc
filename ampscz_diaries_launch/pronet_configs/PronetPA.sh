#!/bin/bash

# basic path settings
data_root="/mnt/ProNET/Lochness/PHOENIX"
site="PronetPA"
export data_root
export site

# auto transcription settings
auto_send_on="Y" # if "N", QC will be calculated without uploading anything to TranscribeMe
export auto_send_on
# rest of settings in this section are only relevant when auto transcription is on
# start with transcribeme username, password setup via separate process described in next section
transcribeme_username="zailyn.tamayo@yale.edu"
export transcribeme_username
# next settings are thresholds for acceptable quality level of a given audio file
length_cutoff=1 # minimum number of seconds for an audio file to be uploaded to TranscribeMe
db_cutoff=50 # minimum overall decibel level for an audio file to be uploaded to TranscribeMe 
export length_cutoff
export db_cutoff

# need site timezone for estimating true diary submission time (and adjusting date if needed) from provided UTC timestamp
timezone="America/New_York"
# language marker setting to add to the files that are uploaded to TranscribeMe, to alert them of what language the audio will be in
transcription_language="ENGLISH"
export timezone
export transcription_language

# finally setup the secure passwords
# provide path to a hidden .sh file that should be viewable only be the user calling the pipeline
# if put inside repository folder, make sure the filename is in the gitignore
# by default, .passwords.sh is already in the gitignore
passwords_path="$repo_root"/.passwords.sh
# passwords_path will be called to set the passwords as temporary environment variables within the pipeline
# the script should contain the following lines, with correct passwords subbed in, and uncommented
# (do not do so in this file, but in the script at passwords_path)
# only password needed as of now is the one for TranscribeMe SFTP account
# transcribeme_password="password"
# export transcribeme_password
source "$passwords_path"
