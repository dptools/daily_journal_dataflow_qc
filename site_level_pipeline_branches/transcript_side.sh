#!/bin/bash

# site-level pipeline script for all transcript side diary file processing
# should be called with path to config file for that site's settings as main argument
if [[ -z "${1}" ]]; then
	echo "Please provide a path to settings file"
	exit
fi
config_path=$1

# start by getting the absolute path to the directory this script is in, which will be within folder site_level_pipeline_branches of the repo
# this way script will work even if the repo is downloaded to a new location, rather than relying on hard coded paths to where I put it 
full_path=$(realpath $0)
pipelines_root=$(dirname $full_path)
# all of the helpers to be called here can be found in a subfolder of the site-level wrappers folder
func_root="$pipelines_root"/subject_level_functions
# also get root of repository for logging output purposes
repo_root="$pipelines_root"/..

# running config file will set up necessary environment variables
source "$config_path"

# make directory for logs if needed
if [[ ! -d ${repo_root}/logs ]]; then
	mkdir "$repo_root"/logs
fi
# keep logs in individual directories per site
if [[ ! -d ${repo_root}/logs/${site} ]]; then
	mkdir "$repo_root"/logs/"$site"
fi
# save with unique timestamp (unix seconds)
log_timestamp=`date +%s`
# if running manually, print to console and log files simultaneously
exec >  >(tee -ia "$repo_root"/logs/"$site"/transcript_process_logging_"$log_timestamp".txt)
exec 2> >(tee -ia "$repo_root"/logs/"$site"/transcript_process_logging_"$log_timestamp".txt >&2)

# confirm highest level folder structure exists as it should for given site
# (note it may not because of an issue or just because a planned site hasn't actually started enrolling yet - or if writing your own config, a settings issue)
cd "$data_root"/PROTECTED
if [[ ! -d $site ]]; then
	echo "ERROR: invalid data root path ${data_root} or site ID ${site}, as necessary base folder structure does not exist"
	exit
fi
if [[ ! -d ../GENERAL/$site ]]; then
	echo "ERROR: invalid data root path ${data_root} or site ID ${site}, as necessary base folder structure does not exist"
	exit
fi
if [[ ! -d $site/raw ]]; then
	echo "ERROR: site folder ${site} improperly or not yet completely setup"
	exit
fi
if [[ ! -d $site/processed ]]; then
	echo "ERROR: site folder ${site} improperly or not yet completely setup"
	exit
fi
# don't care if there is a raw in GENERAL as I will never use that part, so excluding that check - but do need processed!
if [[ ! -d ../GENERAL/$site/processed ]]; then
	echo "ERROR: site folder ${site} improperly or not yet completely setup"
	exit
fi
if [[ ! -e ../GENERAL/${site}/${site}_metadata.csv ]]; then
	echo "ERROR: site ${site} missing necessary subject enrollment metadata"
	exit
fi

# let user know script is starting and give basic settings info for reference
echo ""
echo "Beginning site-level daily journal transcript processing pipeline run for:"
echo "$site"
echo "with data root:"
echo "$data_root"
echo "Automatically pulling all new transcripts from TranscribeMe username:"
echo "$transcribeme_username"
echo ""

# add current time for runtime tracking purposes
now=$(date +"%T")
echo "Current time: ${now}"
echo ""
                                               
cd "$data_root"/PROTECTED/"$site"/processed
echo "Looping through available subject IDs"
echo ""
for p in *; do
	# only bother with participants that have ever had some audio uploaded to TranscribeMe
	# obviously without running the audio side pipeline first there will be nothing to pull   
	if [[ ! -d $p/phone/audio_journals/pending_audio ]]; then
		# pending_audio directory will not be deleted by the pipeline once it exists
		echo "${p} has no record of audios ever pushed to TranscribeMe, continuing to next subject"
		echo ""
		continue
	fi
	cd "$p"/phone/audio_journals

	# set up folder for pulled transcripts if not there already
	if [[ ! -d transcripts ]]; then
		mkdir transcripts
		# note no manual review will occur here like does for interviews, so all transcripts can just be pulled and then processed immediately
	fi
	# also setup folder to move audio of transcripts that are done
	# keeping these WAVs so can be used directly for feature extraction script on AV server later
	if [[ ! -d completed_audio ]]; then
		mkdir completed_audio
	fi

	# when pending audios exist within pending_audio folder, then need to run TranscribeMe pull script
	if [ ! -z "$(ls -A pending_audio)" ]; then
		echo "Starting TranscribeMe pull attempt for subject ${p} as pending audios were detected"
		now=$(date +"%T")
		echo "Current time: ${now}"
		python "$func_root"/journal_transcribeme_sftp_pull.py "$data_root" "$site" "$p" "$transcribeme_username" "$transcribeme_password" "$transcription_language"
		echo "TranscribeMe SFTP pull attempts complete for subject ${p}"
		now=$(date +"%T")
		echo "Current time: ${now}"
	else
		echo "No audio journals currently pending transcription for ${p}, will now check if any existing transcripts require further processing"
	fi

	# TODO implement rest of steps - can do this in parallel to starting to get transcripts back from TranscribeMe though
	echo "Transcript processing steps not yet implemented - will run for all pulled transcripts on future iteration"
	
	cd "$data_root"/PROTECTED/"$site"/processed # at end of loop go back to start spot
	echo "${p} has completed new diary transcript processing!"
	now=$(date +"%T")
	echo "Current time: ${now}"
	echo ""
done

echo "Transcript side journals script completed for all current subjects of site ${site}!"
# note doing email alerts for this pipeline only at the server-wide level
