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
if [[ ! -e ../PROTECTED/${site}/${site}_metadata.csv ]]; then
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

	# now move on to processing any transcripts available
	cd transcripts
	# first confirm there are some transcripts available
	if [ -z "$(ls -A *.txt 2> /dev/null)" ]; then 
		echo "${p} has no transcript text files available yet, continuing to next subject"
		echo ""
		cd "$data_root"/PROTECTED/"$site"/processed # back out of folder before skipping over pt
		continue
	fi

	# make redacted copies of the transcript text files for any not done yet
	# maintain in PROTECTED processed for file safety reasons (small size anyway)
	# but will copy redacted versions to GENERAL downstream
	echo "Making redacted copies of any new transcripts for ${p}, using TranscribeMe's {PII} marking convention"
	now=$(date +"%T")
	echo "Current time: ${now}"

	if [[ ! -d redacted_copies ]]; then
		mkdir redacted_copies
		# in addition to going under redacted_copies, they will be named [name]_REDACTED.txt
		# - for later copy to GENERAL transcripts folder
	fi
	# note this python function runs on subject level to make redacted copy for any that don't have it yet 
	# - unlike the per file version used for AMPSCZ interview pipeline
	python "$func_root"/phone_transcript_redaction.py "$data_root" "$site" "$p"

	# csv conversion of newly redacted transcripts
	echo "Converting any new redacted transcript texts for ${p} to CSV"
	now=$(date +"%T")
	echo "Current time: ${now}"

	cd redacted_copies
	if [[ ! -d csv ]]; then
		mkdir csv
	fi
	
	# just do CSV part within the pipeline wrapper here
	for file in *.txt; do	
		name=$(echo "$file" | awk -F '.txt' '{print $1}')
		[[ -e csv/"${name}".csv ]] && continue # if already have a formatted copy skip
		echo "New transcript to convert detected: ${file}"
		
		# begin conversion for this applicable file
		# check encoding type - and force at least UTF8
		typecheck=$(file -n "$file" | grep ASCII | wc -l)
		if [[ $typecheck == 0 ]]; then
			# ASCII is subset of UTF8, so now need to check UTF8 since we know it isn't ASCII
			iconv -f utf8 "$file" -t utf8 -o /dev/null
			if [ $? = 0 ]; then # if exit code of above command is 0, the file is UTF8, otherwise not
				# if it's not ASCII but is UTF8, just make a note
				echo "(note transcript is not ASCII encoded, but is UTF-8)"
			else
				# if it isn't UTF8 we have a problem, completely skip it 
				echo "WARNING: found transcript that is not UTF-8 encoded, this may cause issues with the automatic redaction! It will be completely skipped for now, please review in main transcripts folder manually"
				# now remove the file from redacted copies to be safe in case marked PII was not removed due to encoding discrepancy
				rm "$file" 
				# then skip to the next transcript
				continue 
			fi			
		fi
		# now good to continue

		# substitute tabs with single space for easier parsing (returned by TranscribeMe we see a mix)
		# this file will be kept only temporarily
		sed 's/\t/ /g' "$file" > "$name"_noTABS.txt

		# prep CSV with column headers
		# (no reason to have DPDash formatting for a transcript CSV, so I choose these columns)
		# (some of them are just for ease of future concat/merge operations)
		echo "site,subject,filename,speakerID,timefromstart,text" > csv/"$name".csv

		# check speaker ID number format, as TranscribeMe has used a few different delimiters in the past
		# (this does assume they are consistent with one format throughout a single file though, which should be fine)
		subcheck=$(cat "$file" | grep S1: | wc -l) # speaker ID S1 is guaranteed to appear at least once as it is the initial ID they assign 

		# read in transcript line by line to convert to CSV rows
		while IFS='' read -r line || [[ -n "$line" ]]; do
			if [[ $subcheck == 0 ]]; then # speaker ID always comes first, is sometimes followed by a colon
				sub=$(echo "$line" | awk -F ' ' '{print $1}') 
			else
				sub=$(echo "$line" | awk -F ': ' '{print $1}')
			fi
			time=$(echo "$line" | awk -F ' ' '{print $2}') # timestamp always comes second
			text=$(echo "$line" | awk -F '[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9] ' '{print $2}') # get text based on what comes after timestamp in expected format (MM:SS.milliseconds)
			# the above still works fine if hours are also provided, it just hinges on full minute digits and millisecond resolution being provided throughout the transcript
			if [[ -z "$text" ]]; then
				# text variable could end up empty because ms resolution not provided, so first try a different way of splitting, looking for space after the SS part of the timestamp
				text=$(echo "$line" | awk -F '[0-9][0-9]:[0-9][0-9] ' '{print $2}')
				if [[ -z "$text" ]]; then
					# if text is still empty safe to assume this is an empty line, which do occur - skip it!
					continue
				fi
			fi
			text=$(echo "$text" | tr -d '"') # remove extra characters at end of each sentence
			text=$(echo "$text" | tr -d '\r') # remove extra characters at end of each sentence
			echo "${site},${p},${name},${sub},${time},\"${text}\"" >> csv/"$name".csv # add the line to CSV
		done < "$name"_noTABS.txt

		# remove the temporary file
		rm "$name"_noTABS.txt
	done

	echo "New CSV conversion for ${p} done"
	now=$(date +"%T")
	echo "Current time: ${now}"
	cd .. # return to main transcripts folder

	# now proceed with transcript QC 
	# - no folders to make for this actually as can use what was already created for audio side
	# (fair to assume audio side must have been run to get to this point for subject anyway)
	echo "Updating transcript QC for subject ${p}"
	now=$(date +"%T")
	echo "Current time: ${now}"

	python "$func_root"/transcript_diary_qc.py "$data_root" "$site" "$p"

	echo "Transcript QC for new ${p} data done - moving on to final early processing step (basic per sentence stats for expanded transcript CSVs)"
	now=$(date +"%T")
	echo "Current time: ${now}"

	if [[ ! -d redacted_csvs_with_stats ]]; then
		mkdir redacted_csvs_with_stats
	fi

	python "$func_root"/phone_transcript_sentence_stats.py "$data_root" "$site" "$p"

	echo "Transcript sentences check for new ${p} data done - will now do final file management, sending copies of new shareable transcripts to GENERAL side"
	now=$(date +"%T")
	echo "Current time: ${now}"

	# setup folders in GENERAL if needed
	if [[ ! -d ${data_root}/GENERAL/${site}/processed/${p}/phone/audio_journals/transcripts ]]; then
		mkdir "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals/transcripts
	fi
	if [[ ! -d ${data_root}/GENERAL/${site}/processed/${p}/phone/audio_journals/transcripts/csvs_with_per_sentence_stats ]]; then
		mkdir "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals/transcripts/csvs_with_per_sentence_stats
	fi

	# first copy over the redacted txts as needed
	cd redacted_copies
	for file in *.txt; do
		if [[ ! -e ${data_root}/GENERAL/${site}/processed/${p}/phone/audio_journals/transcripts/"$file" ]]; then
			cp "$file" "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals/transcripts/"$file"
			echo "Copied new redacted transcript ${file} to GENERAL processed for downstream transfer"
		fi
	done

	# now copy over the CSVs with the sentence-level counts as needed
	cd ../redacted_csvs_with_stats
	for file in *.csv; do
		if [[ ! -e ${data_root}/GENERAL/${site}/processed/${p}/phone/audio_journals/transcripts/csvs_with_per_sentence_stats/"$file" ]]; then
			cp "$file" "$data_root"/GENERAL/"$site"/processed/"$p"/phone/audio_journals/transcripts/csvs_with_per_sentence_stats/"$file"
		fi
	done

	echo "Updated transfers done for ${p}"
	
	cd "$data_root"/PROTECTED/"$site"/processed # at end of loop go back to start spot
	echo "${p} has completed new diary transcript processing!"
	now=$(date +"%T")
	echo "Current time: ${now}"
	echo ""
done

echo "Transcript side journals script completed for all current subjects of site ${site}!"
# note doing email alerts for this pipeline only at the server-wide level
