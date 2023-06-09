#!/bin/bash

# server-level pipeline script for all daily journal data flow/QC processing (audio and transcript sides)
# should be called with path to folder that contains a config file for each site to be processed
if [[ -z "${1}" ]]; then
	echo "Please provide a path to folder of site configs"
	exit
fi
configs_path=$1

# get path this script is in to find repo root
full_path=$(realpath $0)
repo_root=$(dirname $full_path)

# run audio side code for each config
for file in "$configs_path"/*.sh; do
	bash "$repo_root"/site_level_pipeline_branches/audio_side.sh "$file"
	# add spacing for when monitoring logs in real time
	echo ""
	echo ""
	echo ""
done

# run transcript side code for each config
for file in "$configs_path"/*.sh; do
	bash "$repo_root"/site_level_pipeline_branches/transcript_side.sh "$file"
	# add spacing for when monitoring logs in real time
	echo ""
	echo ""
	echo ""
done

# TODO will add calling of final server-wide summary stats modules once those are written

# note that any setup of basic dependencies and permissions will be done for each server separately in the cron scripts under ampscz_diaries_launch
# within that script it will simply call this high level wrapper to do the core pipeline work