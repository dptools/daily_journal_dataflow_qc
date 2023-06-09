#!/usr/bin/env python

import os
import sys
import glob
import json
import datetime
import pytz
import pandas as pd
import numpy as np

def mindlamp_json_accounting(data_root, site, subject, assumed_timezone, assumed_language="ENGLISH"):
	# start by obtaining valid consent date for input subject ID
	try:
		study_metadata_path = os.path.join(data_root,"GENERAL",site,site + "_metadata.csv")
		study_metadata = pd.read_csv(study_metadata_path)
		patient_metadata = study_metadata[study_metadata["Subject ID"] == subject]
		consent_date_str = patient_metadata["Consent"].tolist()[0]
		consent_date = datetime.datetime.strptime(consent_date_str,"%Y-%m-%d")
		consent_date = pytz.timezone(assumed_timezone).localize(consent_date)
	except:
		# occasionally we encounter issues with the study metadata file, so adding a check here to skip processing if day number not calculable
		# shouldn't really happen for AMPSCZ though based on the requirements Lochness enforces
		print("WARNING: no consent date information in the site metadata CSV for input patient " + subject + ", or problem with input arguments")
		return

	# check in advance that a few necessary things are already set up before proceeding with the actual function
	if not os.path.isdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals")):
		# shouldn't really get this warning if calling from main pipeline, set up to assist with running as individual module
		print("WARNING: necessary output folder hierarchy not set up for these input arguments! please review documentation")
		return

	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "raw", subject, "phone"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no PROTECTED phone data available for subject ID " + subject + ", or problem with input arguments") 
		return

	cur_files = glob.glob("*activity*.json")
	if len(cur_files) == 0:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no active app data files available for subject ID " + subject)
		return

	if os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_appActivitiesJSONAccounting.csv")):
		old_json_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_appActivitiesJSONAccounting.csv"))
		prev_json_paths = old_json_df["json_filename"].tolist()
	else:
		prev_json_paths = []
	if os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_audioJournalJSONRecordsInfo.csv")):
		old_diary_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"))
		prev_diary_paths = old_diary_df["matching_mp3_absolute_raw_path"].tolist()
	else:
		prev_diary_paths = []
	if os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_availablePhoneMP3sAccounting.csv")):
		old_mp3_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals","file_accounting_details",site + "_" + subject + "_availablePhoneMP3sAccounting.csv"))
		prev_mp3_paths = old_mp3_df["found_mp3_name"].tolist()
	else:
		prev_mp3_paths = []

	# initialize lists for high level active mindlamp json accounting
	new_files = []
	object_count_list = []
	diary_count_list = []
	ema_count_list = []
	# initialize lists for more detailed audio journal accounting
	diary_root_filenames = []
	sound_numbers = []
	unix_timestamps = []
	for file in cur_files:
		if file in prev_json_paths:
			continue
		else:
			new_files.append(file)
		diary_count = 0
		ema_count = 0
		with open(file) as f:
			cur_json = json.load(f)
			obj_count = len(cur_json)
			for obj in cur_json:
				if "timestamp" not in obj:
					# in testing every activity JSON had the timestamp field
					# - but just to be safe so it doesn't crash for an entire subject due to a single file error
					print("WARNING: no timestamp for object in JSON file " + file + ", skipping this entry")
					continue
				if "static_data" in obj:
					stat = obj["static_data"]
					if "url" in stat:
						diary_count = diary_count + 1
						diary_root_filenames.append(file.split(".json")[0])
						sound_numbers.append(stat["url"])
						unix_timestamps.append(obj["timestamp"])
					else:
						ema_count = ema_count + 1
				else:
					# in testing every activity JSON had the static_data field
					# - but just to be safe so it doesn't crash for an entire subject due to a single file error
					print("WARNING: unrecognized object in JSON file " + file)
		object_count_list.append(obj_count)
		diary_count_list.append(diary_count)
		ema_count_list.append(ema_count)

	basic_json_logs = pd.DataFrame()
	basic_json_logs["json_filename"] = new_files
	basic_json_logs["number_of_records"] = object_count_list
	basic_json_logs["diary_records_count"] = diary_count_list
	basic_json_logs["ema_records_count"] = ema_count_list

	mindlamp_dates = [x.split("_")[3] + "-" + x.split("_")[4] + "-" + x.split("_")[5] if len(x.split("_"))==6 else np.nan for x in diary_root_filenames]
	expected_absolute_paths = [os.path.join(data_root,"PROTECTED", site, "raw", subject, "phone", x + "_" + y.lower() + ".mp3") for x,y in zip(diary_root_filenames,sound_numbers)]
	existence_boolean = [os.path.isfile(x) for x in expected_absolute_paths]
	converted_times = [datetime.datetime.fromtimestamp(x/1000.0,tz=pytz.timezone(assumed_timezone)) for x in unix_timestamps]
	converted_times_str = [x.strftime("%Y-%m-%d %H:%M:%S") for x in converted_times]
	literal_day_numbers = [(x - consent_date).days + 1 for x in converted_times] # study day starts at 1 on day of consent, here not yet adjusting for late night
	literal_submission_hours = [x.hour for x in converted_times]
	submission_minute_stamp = [x.minute for x in converted_times]
	# now push back day number by 1 if that submission took place before 4 am
	# also make midnight 24, 1 am 25, 2 am 26, and 3 am 27 as far as hours, so that 4 is lowest
	adjusted_day_numbers = [x - 1 if y < 4 else x for x,y in zip(literal_day_numbers,literal_submission_hours)]
	adjusted_hour_numbers = [x + 24 if x < 4 else x for x in literal_submission_hours]
	week_days = [((x.weekday() + 2) % 7) + 1 for x in converted_times] # use dpdash weekday convention for later steps
	# need to adjust weekdays for late night submissions now similarly
	week_days_adjusted = [x - 1 if (y < 4 and x > 1) else (7 if y < 4 else x) for x,y in zip(week_days,literal_submission_hours)]
	consent_account = [consent_date_str for x in range(len(expected_absolute_paths))]
	timezone_account = [assumed_timezone for x in range(len(expected_absolute_paths))]
	language_setting = [assumed_language for x in range(len(expected_absolute_paths))]

	diary_json_logs = pd.DataFrame()
	diary_json_logs["diary_root_name"] = diary_root_filenames
	diary_json_logs["json_logged_sound_number"] = sound_numbers
	diary_json_logs["unix_timestamp"] = unix_timestamps
	diary_json_logs["mindlamp_naming_datestamp"] = mindlamp_dates
	diary_json_logs["matching_mp3_absolute_raw_path"] = expected_absolute_paths
	diary_json_logs["mp3_existence_check"] = existence_boolean
	diary_json_logs["local_time_converted"] = converted_times_str
	diary_json_logs["assigned_study_day"] = adjusted_day_numbers
	diary_json_logs["assigned_day_of_week"] = week_days_adjusted
	diary_json_logs["adjusted_submission_hour"] = adjusted_hour_numbers
	diary_json_logs["submission_minute"] = submission_minute_stamp
	diary_json_logs["timezone_used"] = timezone_account
	diary_json_logs["consent_date_at_accounting"] = consent_account
	diary_json_logs["expected_language"] = language_setting
	# get the diary number within the day using the new adjusted date numbers, and also order df chronologically
	diary_json_logs.sort_values(by="local_time_converted",inplace=True)
	diary_json_logs["adjusted_sound_number"] = diary_json_logs.groupby("assigned_study_day").cumcount()+1
	diary_json_logs["proposed_processed_name"] = [site + "_" + subject + "_audioJournal_day" + format(x, '04d') + "_submission" + str(y) + ".wav" for x,y in zip(diary_json_logs["assigned_study_day"].tolist(),diary_json_logs["adjusted_sound_number"].tolist())]

	if len(prev_diary_paths) > 0:
		diary_json_logs_full = pd.concat([old_diary_df,diary_json_logs]).reset_index(drop=True)
	else:
		diary_json_logs_full = diary_json_logs.copy() # will modify diary_json_logs later

	cur_date = datetime.date.today().strftime("%Y-%m-%d")
	cur_mp3s = glob.glob("*.mp3")
	new_mp3s = [x for x in cur_mp3s if x not in prev_mp3_paths]
	known_mp3_names = [x.split("/")[-1] for x in diary_json_logs_full["matching_mp3_absolute_raw_path"].tolist()]
	mp3_found_boolean = [x in known_mp3_names for x in new_mp3s]
	# in case an mp3 isn't found, also add a column checking for various mindlamp assumptions
	mp3_gen_structure_check = [x[0]=="U" and len(x.split("_"))==8 and x.split("_")[1]==site and x.split("_")[2]=="activity" and x.split("_")[6]=="sound" and len(x.split("activity_")[-1].split("_sound")[0])==10 for x in new_mp3s]

	basic_diary_logs = pd.DataFrame()
	basic_diary_logs["date_first_detected"] = [cur_date for x in range(len(new_mp3s))]
	basic_diary_logs["found_mp3_name"] = new_mp3s
	basic_diary_logs["json_record_existence_check"] = mp3_found_boolean
	basic_diary_logs["mp3_name_structure_validation"] = mp3_gen_structure_check

	os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals"))
	if not os.path.isdir("file_accounting_details"):
		os.mkdir("file_accounting_details")

	if len(new_files) > 0:
		if len(prev_json_paths) > 0:
			basic_json_logs = pd.concat([old_json_df,basic_json_logs]).reset_index(drop=True)
		basic_json_logs.to_csv(os.path.join("file_accounting_details",site + "_" + subject + "_appActivitiesJSONAccounting.csv"),index=False)

	if len(new_mp3s) > 0:
		if len(prev_mp3_paths) > 0:
			basic_diary_logs = pd.concat([old_mp3_df,basic_diary_logs]).reset_index(drop=True)
		basic_diary_logs.to_csv(os.path.join("file_accounting_details",site + "_" + subject + "_availablePhoneMP3sAccounting.csv"),index=False)

	if not diary_json_logs.empty:
		diary_json_logs_full.to_csv(os.path.join("file_accounting_details",site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"),index=False)
		print("Found new diaries to be processed for subject " + subject)
		if not os.path.isdir("raw_file_tracking_system"):
			os.mkdir("raw_file_tracking_system")
		# in addition to saving newly updated main JSON CSV, also update file tracking system for next steps accordingly
		
		# make sure that the found diaries via JSON record check out with mp3 path before adding them to list for next step
		diary_json_logs = diary_json_logs[diary_json_logs["mp3_existence_check"]==True]
		for raw_path,processed_path in zip(diary_json_logs["matching_mp3_absolute_raw_path"].tolist(),diary_json_logs["proposed_processed_name"].tolist()):
			raw_name_check = raw_path.split(".mp3")[0].split("/")[-1]
			tracking_file_name = os.path.join("raw_file_tracking_system","TODO+" + raw_name_check + ".txt")
			tracking_file_later_rename = os.path.join("raw_file_tracking_system",raw_name_check + ".txt")
			# confirm that this file has definitely not already been processed
			if os.path.isfile(tracking_file_name) or os.path.isfile(tracking_file_later_rename):
				print("WARNING: previously processed filename (" + tracking_file_later_rename + ") was detected as new today, skipping this one")
				continue
			# if wav files are deleted we can still easily track what has already been processed
			with open(tracking_file_name,'w') as txt:
				txt.write(processed_path) # put processed name inside the text file like I do for tracking interview pipeline
			
if __name__ == '__main__':
	# Map command line arguments to function arguments.
	try:
		mindlamp_json_accounting(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], assumed_language=sys.argv[5])
	except:
		mindlamp_json_accounting(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])