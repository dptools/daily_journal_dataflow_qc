#!/bin/bash

# site-level pipeline script for all audio side diary file processing
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
exec >  >(tee -ia "$repo_root"/logs/"$site"/audio_process_logging_"$log_timestamp".txt)
exec 2> >(tee -ia "$repo_root"/logs/"$site"/audio_process_logging_"$log_timestamp".txt >&2)

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
echo "Beginning site-level daily journal audio processing pipeline run for:"
echo "$site"
echo "with data root:"
echo "$data_root"
echo ""
if [ $auto_send_on = "Y" ] || [ $auto_send_on = "y" ]; then
	echo "Configuration is to automatically send all qualifying audio to TranscribeMe, at username:"
	echo "$transcribeme_username"
	echo "qualifying audio have a duration (in seconds) of at least:"
	echo "$length_cutoff"
	echo "and db level of at least:"
	echo "$db_cutoff"
	echo "Note all newly processed journals that pass QC and that represent the first upload by a given subject on a given study day will be uploaded for professional transcription."
else
	echo "Audio will not be automatically sent to TranscribeMe in the present pipeline run -"
	echo "all newly identified journal MP3s that are approved by QC will instead have their renamed WAVs left in the corresponding audio_to_send subfolder within PROTECTED side processed."
fi
echo ""

# add current time for runtime tracking purposes
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

cd "$data_root"/PROTECTED/"$site"/raw
echo "Looping through available subject IDs"
echo ""
for p in *; do
	# only bother with participants registered with phone data and also who have some active data registration
	if [[ ! -d $p/phone ]]; then
		echo "${p} has no PROTECTED phone data found at all, continuing to next subject"
		echo ""
		continue
	fi
	cd "$p"/phone
	app_count=$(ls -1 *activity*.json | wc -l)
	if [[ $app_count == 0 ]]; then
		cd "$data_root"/PROTECTED/"$site"/raw # return to top before continuing
		echo "${p} has no active-type app-based data registered, continuing to next subject"
		echo ""
		continue
	fi

	# now setup up processed folders on both sides as needed for subjects that do have relevant data
	if [[ ! -d "$data_root"/PROTECTED/"$site"/processed/"$p"/phone ]]; then
		mkdir "$data_root"/PROTECTED/"$site"/processed/"$p"/phone
	fi
	if [[ ! -d "$data_root"/GENERAL/"$site"/processed/"$p"/phone ]]; then
		mkdir "$data_root"/GENERAL/"$site"/processed/"$p"/phone
	fi
	if [[ ! -d "$data_root"/PROTECTED/"$site"/processed/"$p"/phone/audio_journals ]]; then
		mkdir "$data_root"/PROTECTED/"$site"/processed/"$p"/phone/audio_journals
	fi
	if [[ ! -d "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals ]]; then
		mkdir "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals
	fi

	echo "Starting JSON accounting update for subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"
	python "$func_root"/mindlamp_accounting.py "$data_root" "$site" "$p" "$timezone" "$transcription_language"
	echo "Accounting done for subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"

	# next need to do the actual conversion to WAV!
	# first confirm that any journals were actually recognized before bothering to proceed (may be EMA only)
	if [[ ! -d "$data_root"/PROTECTED/"$site"/processed/"$p"/phone/audio_journals/raw_file_tracking_system ]]; then
		echo "${p} has no audio journal submission records found, continuing to next subject"
		echo ""
		cd "$data_root"/PROTECTED/"$site"/raw # return to top before continuing
		continue
	fi
	cd "$data_root"/PROTECTED/"$site"/processed/"$p"/phone/audio_journals
	if [[ ! -d temp_audio ]]; then
		mkdir temp_audio
	fi
	cd raw_file_tracking_system
	new_audio_count=$(ls -1 TODO+*.txt | wc -l)
	# also will be nothing more to do on audio side of pipeline if there are no new filepaths set aside for processing
	if [[ $new_audio_count == 0 ]]; then
		echo "${p} has not submitted any new audio journals since last processing time, continuing to next subject"
		echo ""
		cd "$data_root"/PROTECTED/"$site"/raw # return to top before continuing
		continue
	fi

	echo "Starting conversion of newly detected journal MP3s to renamed WAVs for subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"
	for file in TODO+*.txt; do
		# get raw path from filename, get processed path from file contents, convert it to wav in correct location (temp for now), rename txt file to remove TODO once that is confirmed
		cur_raw_file=$(echo "$file" | awk -F 'ODO[+]' '{print $2}') 
		cur_raw_name=$(echo "$cur_raw_file" | awk -F '.txt' '{print $1}') 
		raw_mp3_name="$cur_raw_name".mp3
		input_path="$data_root"/PROTECTED/"$site"/raw/"$p"/phone/"$raw_mp3_name"
		output_name=$(cat ${file})
		ffmpeg -i "$input_path" ../temp_audio/"$output_name" &> /dev/null

		if [[ -e "$data_root"/PROTECTED/"$site"/processed/"$p"/phone/audio_journals/temp_audio/"$output_name" ]]; then
			# confirm the converted file was created to then remove this diary from TODO
			mv "$file" "$cur_raw_file"
		else
			echo "WARNING: issue with WAV conversion detected for ${cur_raw_name}"
		fi
	done
	echo "Setup of new audio files on processed side has now been completed for subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"

	cd ..
	# now need to do the processing steps on the set aside diary WAVs
	echo "Beginning audio QC on new diaries from subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"

	# the QC function call here will also involve some file movement as part of selection for transcription upload
	if [[ ! -d audio_to_send ]]; then
		mkdir audio_to_send
	fi
	if [[ ! -d rejected_audio ]]; then
		mkdir rejected_audio
	fi
	# additionally need to make a folder under PROTECTED for maintenance of DPDash source data
	# (protection against accidental GENERAL side deletion as all other datatypes have GENERAL outputs removed from aggregation after push to predict)
	if [[ ! -d dpdash_source_csvs ]]; then
		mkdir dpdash_source_csvs
	fi

	# main audio QC function 
	# - also handles the selection process for transcript upload when the db and length cutoff arguments are provided like they are here
	python "$func_root"/audio_diary_qc.py "$data_root" "$site" "$p" "$db_cutoff" "$length_cutoff"

	echo "Audio QC finished for new diaries from subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"

	# if anything left in temp_audio at this point signals a potential problem, handle that file management
	if [ ! -z "$(ls -A temp_audio)" ]; then
		echo "WARNING: audio QC appears to have crashed for some diaries from subject ${p} - they have been moved to a new crashed_audio subfolder."
		if [[ ! -d crashed_audio ]]; then
			mkdir crashed_audio
		fi
		cd temp_audio
		for left_file in *; do
			mv "$left_file" ../crashed_audio/
		done
		cd ..
	fi
	rm -rf temp_audio

	# also check that to_send isn't empty before proceeding further (though it should be rare to find that at this stage)
	if [ -z "$(ls -A audio_to_send)" ]; then 
		rm -rf audio_to_send # if it is empty, clear it out!
		echo "${p} had no new audio journals acceptable for transcription upload, continuing to next subject"
		echo ""
		cd "$data_root"/PROTECTED/"$site"/raw # return to top before continuing
		continue
	fi

	# remove this when confirmed to look okay
	echo "skipping transcribeme send for first test"
	echo ""
	cd "$data_root"/PROTECTED/"$site"/raw
	continue

	if [ $auto_send_on = "Y" ] || [ $auto_send_on = "y" ]; then
		# if needed still, create a folder to put audios that have been sent to TranscribeMe, and are waiting on result
		if [[ ! -d pending_audio ]]; then
			mkdir pending_audio
		fi
	
		echo "Sending accepted audio journals from subject ${p} to TranscribeMe"
		now=$(date +"%T")
		echo "Current time: ${now}"

		# next script will go through the files in to_send and send them to transcribeme, moving them to pending_audio if push was successful
		python "$func_root"/journal_transcribeme_sftp_push.py "$data_root" "$site" "$p" "$transcribeme_username" "$transcribeme_password" "$transcription_language"

		echo "TranscribeMe SFTP push complete for subject ${p}"
		now=$(date +"%T")
		echo "Current time: ${now}"

		# check if to_send is empty now - if so delete it, if not print an error message
		if [ -z "$(ls -A audio_to_send)" ]; then
			rm -rf audio_to_send
		else
			echo "WARNING: some diaries from subject ${p} meant to be pushed to TranscribeMe failed to upload. Check ${data_root}/PROTECTED/${site}/processed/${p}/phone/audio_journals/audio_to_send for more info."
		fi
	else
		echo "TranscribeMe SFTP push turned off for this run, leaving all approved audio in corresponding audio_to_send folder for subject ${p}"
	fi

	cd "$data_root"/PROTECTED/"$site"/raw # at end of loop go back to start spot
	echo "${p} has completed new audio journal processing!"
	now=$(date +"%T")
	echo "Current time: ${now}"
	echo ""
done

echo "Audio side journals script completed for all current subjects of site ${site}!"
# note doing email alerts for this pipeline only at the server-wide level
