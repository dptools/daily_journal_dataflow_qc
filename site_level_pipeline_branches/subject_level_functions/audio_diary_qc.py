#!/usr/bin/env python

# prevent librosa from logging a warning every time it is imported
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# actual imports
import os
import pandas as pd
import numpy as np
import soundfile as sf
import librosa
import sys
import glob

# function that does basic QC on new audio journals (via presence in current temp_audio folder where initial file conversions go)
# pipeline calls this for all subjects, but this function itself is written for individual subject
# adds outputs to existing QC spreadsheet for given ID if there is one
# expects files to already be renamed to match our conventions, as is done by default when this is called by main pipeline
# also when called by main pipeline the db_cutoff and length_cutoff arguments will be provided
# those optional arguments cause the code to do file movement as well to fully prep for sending to TranscribeMe
def audio_diary_qc(data_root, site, subject, db_cutoff=None, length_cutoff=None):
	# specify column headers that will be used for every CSV
	# make it DPDash formatted, but will leave reftime columns blank. others will look up
	headers=["reftime","day","timeofday","weekday","site","subject","daily_submission_number","submit_hour_int","length_minutes","overall_db","mean_flatness","subject_consent_month"]
	# initialize lists to fill in df
	site_days = []
	times = []
	week_days = []
	# site and subject list will be same thing n times, so just do at end
	sub_nums=[]
	sub_hours = []
	lengths=[]
	db=[]
	# spectral flatness is between 0 and 1, 1 being closer to white noise. it is calculated in short windows by the package used
	# max and min of the flatness were never really informative, so just reporting the mean
	mean_flats=[]
	pt_consents=[] # for getting a sense in GENERAL of participant enrollment time without fully revealing dates in DPDash
	if db_cutoff not None and length_cutoff not None:
		headers.append("audio_approved_bool")
		audio_bools = []
		# audio would be rejected because file failed to load, db or duration not above cutoff amount, or second+ submission from same pt day
		# reason will be obvious from other factors in the CSV, but want easy sorting for all rejections at once
		# alerting to earlier stage issues or audio QC entirely crashing is handled separately

	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "temp_audio"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: haven't converted any new audio files yet for input subject " + subject + ", or problem with input arguments") 
		return

	cur_files = os.listdir(".")
	if len(cur_files) == 0:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: haven't converted any new audio files yet for input subject " + subject)
		return

	# next will prep to get additional info from accounting JSON 
	# - not strictly necessary for python script but should always exist in context of broader pipeline
	if not os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "file_accounting_details", site + "_" + subject + "_audioJournalJSONRecordsInfo.csv")):
		# shouldn't have this issue when called from pipeline
		print("WARNING: JSON file accounting missing for input subject " + subject + ", so some metadata details will be missing from audio QC output here")
		accounting_df = pd.DataFrame()
	else:
		accounting_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "file_accounting_details", site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"))

	# constant used to convert RMS to decibels
	ref_rms=float(2*(10**(-5)))

	cur_files.sort() # go in order, although can also always sort CSV later.
	wav_files = []
	for filename in cur_files:
		if not filename.endswith(".wav"): 
			# skip any non-audio files (and folders) - though using main pipeline there shouldn't be any
			continue
		wav_files.append(filename) # for later check of filenames in order

		# handle the DPDash and other metadata columns here
		# first pulling directly from filename for core info which we will always require
		cur_day = int(filename.split("_day")[-1].split("_")[0])
		cur_num = int(filename.split("_submission")[-1].split(".wav")[0])
		site_days.append(cur_day)
		sub_nums.append(cur_num)
		# next get additional info from accounting JSON where available
		if accounting_df.empty:
			times.append(np.nan)
			week_days.append(np.nan)
			sub_hours.append(np.nan)
			pt_consents.append(np.nan)
		else:
			# match exactly that record in the accounting JSON
			cur_account = accounting_df[(accounting_df["assigned_study_day"]==cur_day) & (accounting_df["adjusted_sound_number"]==cur_num)]
			if cur_account.shape[0] != 1:
				# shouldn't really have this happen in scope of main pipeline if run correctly with no manual interference
				# - but catch this to be safe, will print this warning but also will find obvious due to placement of NaNs in resulting CSV
				print("WARNING: " + filename + " had issue with lookup in JSON accounting CSV, some metadata will be missing for it - shape of filtered DF was actually " + str(cur_account.shape[0]))
				times.append(np.nan)
				week_days.append(np.nan)
				sub_hours.append(np.nan)
				pt_consents.append(np.nan)
			else:
				times.append(cur_account["local_time_converted"].tolist()[0].split(" ")[-1])
				week_days.append(cur_account["assigned_day_of_week"].tolist()[0])
				sub_hours.append(cur_account["adjusted_submission_hour"].tolist()[0])
				full_consent_str = cur_account["consent_date_at_accounting"].tolist()[0]
				pt_consents.append(full_consent_str.split("-")[1] + "/" + full_consent_str.split("-")[0])

		try:
			data, fs = sf.read(filename)
		except:
			# if audio file can't be loaded obviously needs to be skipped
			print("WARNING: " + filename + " appears to be a corrupted audio file, QC values will be empty for it")
			# put nans in so they are still included fully in DF!
			lengths.append(np.nan)
			db.append(np.nan)
			mean_flats.append(np.nan)
			if db_cutoff not None and length_cutoff not None:
				audio_bools.append(0)
			continue 

		# get length info
		ns = data.shape[0]
		if ns == 0:
			# ignore entirely empty audio as well
			print("WARNING: " + filename + " is an empty file, QC values will be empty for it")
			# put nans in so they are still included fully in DF!
			lengths.append(np.nan)
			db.append(np.nan)
			mean_flats.append(np.nan)
			if db_cutoff not None and length_cutoff not None:
				audio_bools.append(0)
			continue
		
		try: 
			# may not be a second shape number when file is mono, but should check for it in case we encounter stereo here
			cs = data.shape[1] 
			if cs == 2: 
				print("WARNING: " + filename + " is stereo audio but expected mono - collapsing for QC calculation using mean")
				data = np.mean(data, axis=1)
			data = data.flatten() # drop the axis entirely once its shape is down to 1
		except: 
			pass
			
		# append length info
		sec = float(ns)/fs
		mins = sec/float(60)
		# use round so values are reasonably viewable on DPDash
		lengths.append(round(mins,3))

		# get other audio props
		gain = np.sqrt(np.mean(np.square(data)))
		vol = round(20 * np.log10(vol/ref_rms),2)
		db.append(vol)
		spec_flat = librosa.feature.spectral_flatness(y=data)
		mean_flats.append(round(np.mean(spec_flat),4))

		# finally add transcript push approval column info when needed
		if db_cutoff not None and length_cutoff not None:
			if vol < db_cutoff or sec < length_cutoff or cur_num > 1:
				print("WARNING: " + filename + " rejected by audio QC script (db=" + str(db) + ", seconds=" + str(sec) + ", number=" + str(cur_num) + ")")
				audio_bools.append(0)
			else:
				audio_bools.append(1)

	# get pt and site lists, and also a list of nans for reftime column (values optional but column must exist per DPDash)
	sites = [site for x in range(len(db))]
	subjects = [subject for x in range(len(db))]
	ref_times = [np.nan for x in range(len(db))]

	# construct current CSV
	values = [ref_times, site_days, times, week_days, sites, subjects, sub_nums, sub_hours, lengths, db, mean_flats, pt_consents]
	if db_cutoff not None and length_cutoff not None:
		values.append(audio_bools)
	new_csv = pd.DataFrame()
	for i in range(len(headers)):
		h = headers[i]
		vals = values[i]
		new_csv[h] = vals

	# go back to top level of PROTECTED processed for subject - by this point in code know it must exist
	os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals"))
	# check for prior audio diary QC records
	source_path = os.path.join("dpdash_source_csvs", site + "_" + subject + "_" + "diaryAudioQC.csv")
	if os.path.isfile(source_path):
		old_df = pd.read_csv(source_path)
		final_save = pd.concat([old_df,new_csv]).reset_index(drop=True)
	else:
		final_save = new_csv
	final_save.to_csv(source_path,index=False)

	# now that main audio QC stats CSV has been updated, move files if audio selection included
	if db_cutoff not None and length_cutoff not None:
		for audio_name,approval in zip(wav_files,audio_bools):
			if approval == 1:
				os.rename(os.path.join("temp_audio",audio_name),os.path.join("audio_to_send",audio_name))
			else:
				os.rename(os.path.join("temp_audio",audio_name),os.path.join("rejected_audio",audio_name))

	# finally also deal with saving copy to be pushed to predict under GENERAL, matching expectations for DPDash
	final_save.sort_values(by="day",inplace=True) # should already be sorted but ensure before grabbing day number for name
	start_day = final_save["day"].dropna().tolist()[0]
	end_day = final_save["day"].dropna().tolist()[-1]
	# also drop any duplicates in case audio got processed a second time - shouldn't happen via pipeline though!
	final_save.drop_duplicates(subset=["day", "timeofday"],inplace=True)
	dpdash_name = site[-2:] + "-" + subject + "-diaryAudioQC-day" + str(start_day) + "to" + str(end_day) + ".csv"
	dpdash_folder = os.path.join(data_root, "GENERAL", site, "processed", subject, "phone", "audio_journals")
	try:
		os.chdir(dpdash_folder)
	except:
		print("WARNING: correct folder structure does not exist for push of DPDash CSV to predict server, please fix for future monitoring updates")
		return
	for old_dpdash in glob.glob(site[-2:] + "-" + subject + "-diaryAudioQC-day*.csv"):
		os.remove(old_dpdash)
	final_save.to_csv(dpdash_name,index=False)

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	try:
		audio_diary_qc(sys.argv[1], sys.argv[2], sys.argv[3], db_cutoff=sys.argv[4], length_cutoff=sys.argv[5])
	except:
		audio_diary_qc(sys.argv[1], sys.argv[2], sys.argv[3])

