#!/usr/bin/env python

import os
import pandas as pd
import numpy as np
import sys

# python function separates out rejected audio QC table for given subject
# also separates out successful audio table and have it merged with stats from various other tables
def diary_qc_compilation(data_root, site, subject):
	success_csv_name = site + "_" + subject + "_successfulJournals_allQC_withMetadata.csv"
	reject_csv_name = site + "_" + subject + "_audioQCRejectedJournals_dataLog.csv"

	# check necessary input folder exists in order to change directories into it
	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no diary pipeline outputs yet for input subject " + subject + ", or problem with input arguments") 
		return

	# specify input component columns for reference (to be loaded next)
	audio_qc_headers=["reftime","day","timeofday","weekday","site","subject","daily_submission_number","submit_hour_int",
		 			  "length_minutes","overall_db","mean_flatness","subject_consent_month","audio_approved_bool"]
	transcript_qc_headers=["reftime","day","timeofday","weekday","site","subject","daily_submission_number",
		 				   "speakerID_count","S1_sentence_count","total_sentence_count","word_count",
		 				   "min_words_in_sen","max_words_in_sen","inaudible_count","questionable_count",
		 				   "other_bracketed_notation_count","redacted_count",
		 				   "final_timestamp_minutes","min_timestamp_space_seconds","max_timestamp_space_seconds",
		 				   "min_timestamp_space_per_word","max_timestamp_space_per_word",
		 				   "txt_encoding_type","redacted_csv_filename","date_transcript_processed"]
	disfluencies_headers = ["transcript_csv_name", "day", "daily_submission_number", "length_minutes", "total_word_count", 
		   					"nonverbal_edits_count", "verbal_edits_count", "repeats_count", "restarts_count", "total_disfluencies",
		   					"nonverbal_edits_per_word", "verbal_edits_per_word", "repeats_per_word", "restarts_per_word", 
		   					"disfluencies_per_minute"] 
	json_headers = ["diary_root_name", "json_logged_sound_number", "unix_timestamp", "mindlamp_naming_datestamp", 
		   			"matching_mp3_absolute_raw_path", "mp3_existence_check", "local_time_converted", "assigned_study_day", 
		   			"assigned_day_of_week", "adjusted_submission_hour", "submission_minute", "timezone_used", 
		   			"consent_date_at_accounting", "expected_language", "adjusted_sound_number", "proposed_processed_name"]
	mp3_headers = ["date_first_detected", "found_mp3_name", "json_record_existence_check", "mp3_name_structure_validation"]
	# some will also be useful in handling empty merge so columns can be consistent for downstream pipeline cross-site concat

	# also specify a few merge subsets for later use
	dpdash_merge = ["reftime","day","timeofday","weekday","site","subject","daily_submission_number"] # will drop reftime from both afterwards
	disfluencies_merge = ["day","daily_submission_number"] # should be unique!
	disfluencies_drop = ["length_minutes", "total_word_count"] # drop these before trying merge, as they are redundant
	# need to rename a few columns to make merge work between metadata and QC
	json_headers_rename = {"assigned_study_day":"day","adjusted_sound_number":"daily_submission_number",
						   "submission_minute":"submit_minute_int"} # final one just for convention consistency
	mp3_headers_rename = {"date_first_detected":"date_mp3_first_detected"} # for clarity
	# going to merge json and mp3 metadata first and then merge that with rest of combined CSV
	# - will do so by making temporary filename-only column matching found_mp3_name in json headers
	metadata_merge_self = ["found_mp3_name"]
	# for metadata will have some filtering to do before attempting merge with QC then
	metadata_headers_keep = ["day", "daily_submission_number", "submit_minute_int", "local_time_converted", "timezone_used", 
							 "consent_date_at_accounting", "expected_language", "proposed_processed_name", 
							 "matching_mp3_absolute_raw_path", "date_mp3_first_detected"]
	metadata_merge_qc = ["day","daily_submission_number"] # again should be unique!

	# now try to load component CSVs, first required ones
	try:
		audio_qc = pd.read_csv(os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryAudioQC.csv"))
	except:
		# probably a concern - but note if running from main pipeline errors will have been logged in previous step, can defer to that
		print("WARNING: audio QC failed to load for subject " + subject)
		# regardless, with this missing it defeats purpose of this function, so going to exit
		print("Skipping subject " + subject + " here then as no way to identify and merge stats for successful diaries only!")
		return
	try:
		audio_json = pd.read_csv(os.path.join("file_accounting_details",site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"))
	except:
		# should really not reach this point coming from main pipeline unless something weird is going on
		# (but again will have been logged previosuly)
		print("WARNING: diary JSON accounting failed to load for subject " + subject + " - could signal a major issue!")
		# regardless, with this missing it defeats purpose of this function, so going to exit
		print("Skipping subject " + subject + " here then as no way to identify and merge stats for successful diaries only!")
		return
	try:
		audio_mp3 = pd.read_csv(os.path.join("file_accounting_details",site + "_" + subject + "_availablePhoneMP3sAccounting.csv"))
	except:
		# should really not reach this point coming from main pipeline unless something weird is going on
		# (but again will have been logged previosuly)
		print("WARNING: diary MP3 accounting failed to load for subject " + subject + " - could signal a major issue!")
		# regardless, with this missing it defeats purpose of this function, so going to exit
		print("Skipping subject " + subject + " here then as no way to identify and merge stats for successful diaries only!")
		return

	# now try to load in more optional ones
	try:
		transcript_qc = pd.read_csv(os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryTranscriptQC.csv"))
	except:
		# this may just be normal for subject early on (pending transcripts) - defer to errors log
		print("WARNING: transcript QC failed to load for subject " + subject)
		transcript_qc = pd.DataFrame(columns=transcript_qc_headers)
	try:
		disfluencies = pd.read_csv(os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryDisfluencies.csv"))
	except:
		# this may just be normal for subject early on (pending transcripts) - defer to errors log
		print("WARNING: disfluencies CSV failed to load for subject " + subject)
		disfluencies = pd.DataFrame(columns=disfluencies_headers)
	
	# also make sure audio QC CSV was appropriately set up by pipeline, not run outside of scope with selection option disabled
	if "audio_approved_bool" not in audio_qc.columns:
		print("WARNING: audio QC not run with quality audio selection enabled, so nothing could have been rejected regardless of stats")
		# with this missing it defeats purpose of this function, so going to exit
		print("Skipping subject " + subject + " here then as no way to identify and merge stats for successful diaries only!")
		return

	# now grab rejected audio subset since that is easy
	audio_rejected = audio_qc[audio_qc["audio_approved_bool"] != 1].reset_index(drop=True)
	# if anything detected, then will save this CSV
	if audio_rejected.shape[0] > 0:
		# filter down to core most important columns for easy logging here:
		#   first 4 identify file, and daily_submission_number also gives rejection reason if > 1
		#   next 2 give other possible reasons for rejection (or if blank means corrupted WAV)
		#   final 2 are extra metadata of potential use in tracking rejections (and also considering concat across IDs later)
		audio_rejected = audio_rejected[["site","subject","day","daily_submission_number",
										 "overall_db","length_minutes","submit_hour_int","subject_consent_month"]]
		audio_rejected.sort_values(by="day",inplace=True)
		audio_rejected.to_csv(reject_csv_name, index=False)
		print("Created updated audio rejection CSV for " + subject)
	else:
		print("No rejected audios at this time for " + subject)

	# now need to deal with successful audios 
	audio_accepted = audio_qc[audio_qc["audio_approved_bool"] == 1].reset_index(drop=True)
	# check if this is empty to do warning if so
	if audio_accepted.shape[0] == 0:
		print("WARNING: no acceptable audios found for subject " + subject + " - nothing to merge so exiting function now")
		return
	# also double check that the metadata CSVs aren't empty - they should absolutely not be by this point in normal operation
	if audio_json.empty or audio_mp3.empty:
		print("WARNING: accepted audios somehow missing metadata! Definitely an unexpected problem, exiting function and please check manually")
		return

	# now can proceed with merge of full stats, starting with transcript QC
	if not transcript_qc.empty:
		combined_qc = audio_accepted.merge(transcript_qc, on=dpdash_merge, how="left")	
	else:
		combined_qc = audio_accepted
		cur_columns = combined_qc.columns
		cur_rows_count = combined_qc.shape[0]
		for col in transcript_qc.columns:
			if col not in cur_columns:
				combined_qc[col] = [np.nan for x in range(cur_rows_count)]
	combined_qc.drop(columns=["reftime"],inplace=True)

	# next prep and then merge disfluencies
	disfluencies.drop(columns=disfluencies_drop,inplace=True)
	if not disfluencies.empty:
		combined_qc_stats = combined_qc.merge(disfluencies, on=disfluencies_merge, how="left")
	else:
		combined_qc_stats = combined_qc
		cur_columns = combined_qc_stats.columns
		cur_rows_count = combined_qc_stats.shape[0]
		for col in disfluencies.columns:
			if col not in cur_columns:
				combined_qc_stats[col] = [np.nan for x in range(cur_rows_count)]

	# finally will add in other metadata, first merging the json and mp3 logs with each other
	audio_json.rename(columns=json_headers_rename,inplace=True)
	audio_mp3.rename(columns=mp3_headers_rename,inplace=True)
	# create column that json can match mp3 in, first making sure any nans removed
	audio_json.dropna(subset=["matching_mp3_absolute_raw_path"],inplace=True)
	audio_json["found_mp3_name"] = [x.split("/")[-1] for x in audio_json["matching_mp3_absolute_raw_path"]]
	combined_meta = audio_json.merge(audio_mp3, on=metadata_merge_self, how="inner")
	combined_meta = combined_meta[metadata_headers_keep]

	# final combination!
	combined_all = combined_qc_stats.merge(combined_meta, on=metadata_merge_qc, how="left").reset_index(drop=True)

	# now do a bit of cleanup, also involving sanity checks that really shouldn't trigger in pipeline
	combined_all_filtered1 = combined_all.dropna(subset=["day","daily_submission_number"],how="any")
	if not combined_all_filtered1.equals(combined_all):
		print("WARNING: some unexpected empty day/submission number records found in final merged accepted diaries CSV for subject " + subject + " - dropping them here, but please manually review")
	combined_all_filtered2 = combined_all_filtered1[combined_all_filtered1["daily_submission_number"]==1]
	if not combined_all_filtered2.equals(combined_all_filtered1):
		print("WARNING: some unexpected non-first daily submission numbers found in final merged accepted diaries CSV for subject " + subject + " - dropping them here, but please manually review")
	combined_all_filtered3 = combined_all_filtered2.drop_duplicates(subset=["day"])
	if not combined_all_filtered3.equals(combined_all_filtered2):
		print("WARNING: some unexpected duplicate day numbers found in final merged accepted diaries CSV for subject " + subject + " - dropping them here, but please manually review")
	combined_all_filtered3.sort_values(by="day",inplace=True)

	# now it is safe to save - and for this it is good to just overwrite if anything did already exist
	combined_all_filtered3.to_csv(success_csv_name,index=False)

	return

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	diary_qc_compilation(sys.argv[1], sys.argv[2], sys.argv[3])

