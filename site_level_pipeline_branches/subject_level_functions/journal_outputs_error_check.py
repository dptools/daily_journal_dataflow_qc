#!/usr/bin/env python

import os
import pandas as pd
import numpy as np
import sys
import datetime

# check for final set of possible errors to log in the main warnings CSV for given subject
# merge with existing warnings CSV if anything found
# clean up any existing warnings CSV regardless
# see subject_summaries_update.sh pipeline wrapper for more info on types of errors
# technically could call this standalone, but really only makes sense in context of larger pipeline
# - and the subject_summaries_update.sh does part of the checks that make this exhaustive in that context
def finalize_diary_pipeline_error_logs(data_root, site, subject):
	issue_csv_name = site + "_" + subject + "_audioJournalMajorIssuesLog.csv"
	# intended headers of issue CSV
	headers = ["date_detected","site","subject","filename","file_stage","error_message"]
	# first 3 columns will have same entry in every row for any new discoveries
	# for other 3 initialize tracking lists
	new_names = []
	new_stages = []
	new_errors = []
	# get current date for pending_audio timeline check plus issue date_detected later
	cur_date_str = datetime.date.today().strftime("%Y-%m-%d")
	cur_date = datetime.datetime.strptime(cur_date_str,"%Y-%m-%d")

	# check necessary input folder exists in order to change directories into it
	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no diary pipeline outputs yet for input subject " + subject + ", or problem with input arguments") 
		return
	# now try to load component CSVs
	try:
		audio_qc = pd.read_csv(os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryAudioQC.csv"))
	except:
		# if audio QC expected based on other file presence this will be logged shortly
		print("WARNING: audio QC failed to load for subject " + subject)
		audio_qc = pd.DataFrame()
	try:
		transcript_qc = pd.read_csv(os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryTranscriptQC.csv"))
	except:
		# if transcript QC expected based on other file presence this will be logged shortly
		# but also this may just be normal for subject early on (pending transcripts)
		print("WARNING: transcript QC failed to load for subject " + subject)
		transcript_qc = pd.DataFrame()
	try:
		audio_json = pd.read_csv(os.path.join("file_accounting_details",site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"))
	except:
		# should really not reach this point coming from main pipeline unless something weird is going on
		print("WARNING: diary JSON accounting failed to load for subject " + subject + " - could signal a major issue!")
		audio_json = pd.DataFrame()
	try:
		audio_mp3 = pd.read_csv(os.path.join("file_accounting_details",site + "_" + subject + "_availablePhoneMP3sAccounting.csv"))
	except:
		# should really not reach this point coming from main pipeline unless something weird is going on
		print("WARNING: diary MP3 accounting failed to load for subject " + subject + " - could signal a major issue!")
		audio_mp3 = pd.DataFrame()
	
	if os.path.isdir("completed_audio"):
		completed_files = os.listdir("completed_audio")
		if len(completed_files) > 0:
			for cf in completed_files:
				if not cf.endswith(".wav"): 
					# ensure actually WAV first
					continue
				cur_day = int(cf.split("_day")[-1].split("_")[0])
				cur_sub_num = int(cf.split("_submission")[-1].split(".wav")[0])
				if audio_qc.empty:
					# issue
					print("Missing audio QC entirely, but " + cf + " is considered a completed audio")
					new_names.append(cf)
					new_stages.append("post-audioQC")
					new_errors.append("File appears to have fully completed pipeline but can not find a unique entry in audio QC for it now")
				else:
					cur_check = audio_qc[(audio_qc["day"]==cur_day)&(audio_qc["daily_submission_number"]==cur_sub_num)]
					if cur_check.shape[0] != 1:
						# also issue - either not found or duplicated
						print(cf + " is marked as completed audio, but could not find single record for it in audio QC (either missing or duplicate day + submission number error)")
						new_names.append(cf)
						new_stages.append("post-audioQC")
						new_errors.append("File appears to have fully completed pipeline but can not find a unique entry in audio QC for it now")
		else:
			# this can be normal, if it is an issue should be logged via other means in wrapping script
			print("Note there is no completed_audio yet for subject " + subject)
	else:
		# this can be normal, if it is an issue should be logged via other means in wrapping script
		print("Note there is no completed_audio yet for subject " + subject)

	if os.path.isdir("pending_audio"):
		pending_files = os.listdir("pending_audio")
		if len(pending_files) > 0:
			for pf in pending_files:
				if not pf.endswith(".wav"): 
					# ensure actually WAV first
					continue
				cur_day = int(pf.split("_day")[-1].split("_")[0])
				cur_sub_num = int(pf.split("_submission")[-1].split(".wav")[0])
				if audio_qc.empty:
					# issue
					print("Missing audio QC entirely, but " + pf + " is considered a pending audio")
					new_names.append(pf)
					new_stages.append("post-audioQC")
					new_errors.append("File appears to have been sent to TranscribeMe but can not find a unique entry in audio QC for it now")
				else:
					cur_check = audio_qc[(audio_qc["day"]==cur_day)&(audio_qc["daily_submission_number"]==cur_sub_num)]
					if cur_check.shape[0] != 1:
						# also issue - either not found or duplicated
						print(pf + " is marked as pending audio, but could not find single record for it in audio QC (either missing or duplicate day + submission number error)")
						new_names.append(pf)
						new_stages.append("post-audioQC")
						new_errors.append("File appears to have been sent to TranscribeMe but can not find a unique entry in audio QC for it now")
					else:
						# for pending audio, while I already have stuff loaded in this loop can check for elapsed time potential issue
						try:
							cur_record_json = audio_json[(audio_json["assigned_study_day"]==cur_day)&(audio_json["adjusted_sound_number"]==cur_sub_num)]
							cur_record_mp3 = audio_mp3[audio_mp3["found_mp3_name"] == cur_record_json["matching_mp3_absolute_raw_path"].tolist()[0].split("/")[-1]]
							detect_date_str = cur_record_mp3["date_first_detected"].tolist()[0]
							detect_date = datetime.datetime.strptime(detect_date_str,"%Y-%m-%d")
							elapsed_days = (cur_date - detect_date).days
							if elapsed_days >= 14:
								print(pf + " has been pending return from TranscribeMe for over 2 weeks - this may indicate an issue in the way they have returned the file, or something that fell through the cracks. If not awaiting a large batch please follow up.")
								new_names.append(pf)
								new_stages.append("pre-transcript")
								new_errors.append("File has been pending return from TranscribeMe for over 2 weeks now")
						except:
							print("WARNING: unable to look up pending file metadata info in JSON/MP3 accounting CSVs. This may indicate a larger pipeline issue!")
		else:
			# this can be normal, if it is an issue should be logged via other means in wrapping script
			print("Note there is no pending_audio yet for subject " + subject)
	else:
		# this can be normal, if it is an issue should be logged via other means in wrapping script
		print("Note there is no pending_audio yet for subject " + subject)

	if os.path.isdir("transcripts/redacted_copies/csv"):
		csv_files = os.listdir("transcripts/redacted_copies/csv")
		if len(csv_files) > 0:
			for cf2 in csv_files:
				if not cf2.endswith(".csv"): 
					# ensure actually CSV first
					continue
				cur_day = int(cf2.split("_day")[-1].split("_")[0])
				cur_sub_num = int(cf2.split("_submission")[-1].split("_")[0])
				if transcript_qc.empty:
					# issue
					print("Missing transcript QC entirely, but " + cf2 + " CSV (direct input) exists")
					new_names.append(cf2)
					new_stages.append("pre-transcriptQC")
					new_errors.append("File has redacted CSV version available but transcript QC that should have been computed from that is not found")
				else:
					cur_check = transcript_qc[(transcript_qc["day"]==cur_day)&(transcript_qc["daily_submission_number"]==cur_sub_num)]
					if cur_check.shape[0] != 1:
						# also issue - either not found or duplicated
						print(cf2 + " exists, but could not find single record of transcript QC produced from it in QC CSV (either missing or duplicate day + submission number error)")
						new_names.append(cf2)
						new_stages.append("pre-transcriptQC")
						new_errors.append("File has redacted CSV version available but transcript QC that should have been computed from that is not found")
		else:
			# this can be normal, if it is an issue should be logged via other means in wrapping script
			print("Note there are no redacted transcript CSVs yet for subject " + subject)
	else:
		# this can be normal, if it is an issue should be logged via other means in wrapping script
		print("Note there are no redacted transcript CSVs yet for subject " + subject)

	if not audio_json.empty:
		missing_mp3_list = audio_json[audio_json["mp3_existence_check"] != True]
		if missing_mp3_list.shape[0] > 0:
			print("Found JSON records for subject " + subject + " corresponding to a diary, but no matching MP3 for that submission")
			for mp3f in [x.split("/")[-1] for x in missing_mp3_list["matching_mp3_absolute_raw_path"].tolist()]:
				new_names.append(mp3f)
				new_stages.append("pre-wav")
				new_errors.append("File should exist based on MindLAMP JSON data but no MP3 actually found")
	if not audio_mp3.empty:
		missing_json_list = audio_mp3[audio_mp3["json_record_existence_check"] != True]
		if missing_json_list.shape[0] > 0:
			print("Found MP3 uploads for subject " + subject + " that did not having a matching JSON record for obtaining timestamp")
			for jf in missing_json_list["found_mp3_name"].tolist():
				new_names.append(jf)
				new_stages.append("pre-wav")
				new_errors.append("MP3 found without a matching JSON record")

	# construct DF if there were any new issues
	new_issues = pd.DataFrame()
	if len(new_errors) > 0:
		sites = [site for x in range(len(new_errors))]
		subjects = [subject for x in range(len(new_errors))]
		detect_dates = [cur_date for x in range(len(new_errors))]

		values = [detect_dates, sites, subjects, new_names, new_stages, new_errors]
		
		for i in range(len(headers)):
			h = headers[i]
			vals = values[i]
			new_issues[h] = vals

	# now that done compiling any new errors here, see what already exists
	if not os.path.isfile(issue_csv_name):
		# if nothing, can just save new_issues as long as it contains something
		if not new_issues.empty:
			new_issues.to_csv(issue_csv_name, index=False)
	else:
		# if something will need to concat any new issues if they do exist
		# and regardless will have some cleanup to do
		existing_issues = pd.read_csv(issue_csv_name)
		if not new_issues.empty:
			final_issues = pd.concat([existing_issues,new_issues]).reset_index(drop=True)
		else:
			final_issues = existing_issues
		final_issues.sort_values(by="date_detected",inplace=True) # make sure in ascending chronological order
		# drop duplicates keeps the first entry by default, so this means the first detection date is what will be kept
		final_csv = final_issues.drop_duplicates(subset=["site","subject","filename","file_stage","error_message"],ignore_index=True)
		final_csv.to_csv(issue_csv_name, index=False)

	return

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	finalize_diary_pipeline_error_logs(sys.argv[1], sys.argv[2], sys.argv[3])

