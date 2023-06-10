#!/usr/bin/env python

import os
import pandas as pd
import numpy as np
import sys
import glob
import datetime

# compute latest transcript QC for newly available redacted transcript QC CSVs for given subject ID
# when applicable updates existing master CSV and creates GENERAL side version for DPDash integration
# analogous to audio_diary_qc function from audio side, but now relevant metrics for quality assessment of diary transcripts
def transcript_diary_qc(data_root, site, subject):
	# specify column headers that will be used for every CSV
	# make it DPDash formatted, but will leave reftime columns blank. others will look up
	headers=["reftime","day","timeofday","weekday","site","subject","daily_submission_number",
			 "speakerID_count","S1_sentence_count"."total_sentence_count","word_count",
			 "min_words_in_sen","max_words_in_sen","inaudible_count","questionable_count",
			 "other_bracketed_notation_count","redacted_count",
			 "final_timestamp_minutes","min_timestamp_space_seconds","max_timestamp_space_seconds",
			 "min_timestamp_space_per_word","max_timestamp_space_per_word",
			 "txt_encoding_type","redacted_csv_filename","date_transcript_processed"]
	# note: 
	# inaudibles can occur once to mark a long stretch of continuing inaudible, or could just mean a single word was
	# questionable is single uncertain word for each
	# redacted similarly is count of each word that is redacted (number of words split on spaces within curly brace markings)
	# for the other bracketed notation, that will count words that are bracketed besides inaudible and uncertain words (could be laughter,crying,coughing,etc.)
	# no need for crosstalk here as a specific metric like in interviews
	# (also not having the punctuation ones here as adding a basic actual disfluency calculation in downstream step)

	# initialize lists to fill in df
	site_days = []
	times = []
	week_days = []
	sub_nums=[]
	speaker_counts = []
	s1_sens = []
	total_sens = []
	words = []
	min_words = []
	max_words = []
	inauds = []
	questions = []
	brackets = []
	redactions = []
	fin_times = []
	min_spaces = []
	max_spaces = []
	min_spaces_ratio = []
	max_spaces_ratio = []
	encodings = []
	fnames = []
	trans_dates = [] # should generally be same as transcript return date - note I will only save this on PROTECTED side source and not DPDash GENERAL copy
	# site and subject list will be same thing n times, so just do at end

	# check necessary input folder exists in order to change directories into it
	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", "redacted_copies", "csv"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: haven't generated any redacted transcript CSVs yet for input subject " + subject + ", or problem with input arguments") 
		return

	# need output folder setup before proceeding as well - again this is just check for someone calling outside the scope of main pipeline
	if not os.path.isdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs")):
		print("WARNING: output folder not correctly set up yet for input subject " + subject + " - please address in order to save transcript QC outputs")
		return

	# now get list of CSV files to actually potentially process!
	cur_files = os.listdir(".")
	if len(cur_files) == 0:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: haven't generated any redacted transcript CSVs yet for input subject " + subject)
		return
	cur_files.sort() # go in order, although can also always sort CSV later.

	# next will prep to get additional info from accounting JSON 
	# - not strictly necessary for python script but should always exist in context of broader pipeline
	if not os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "file_accounting_details", site + "_" + subject + "_audioJournalJSONRecordsInfo.csv")):
		# shouldn't have this issue when called from pipeline
		print("WARNING: JSON file accounting missing for input subject " + subject + ", so some metadata details will be missing from transcript QC output here")
		accounting_df = pd.DataFrame()
	else:
		accounting_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "file_accounting_details", site + "_" + subject + "_audioJournalJSONRecordsInfo.csv"))

	# also see if any of these transcripts have already been processed
	if os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs", site + "_" + subject + "_" + "diaryTranscriptQC.csv")):
		old_df = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs", site + "_" + subject + "_" + "diaryTranscriptQC.csv"))
		prev_trans_list = old_df["redacted_csv_filename"].tolist()
	else:
		prev_trans_list = []

	for filename in cur_files:
		if not filename.endswith(".csv"): 
			# skip any non-CSV files (and folders) - though using main pipeline there shouldn't be any
			continue
		if filename in prev_trans_list:
			# also skip anything that has already been processed
			continue
		# now good to proceed
		fnames.append(filename) 

		# handle the DPDash and other metadata columns here
		# first pulling directly from filename for core info which we will always require
		cur_day = int(filename.split("_day")[-1].split("_")[0])
		cur_num = int(filename.split("_submission")[-1].split("_")[0])
		site_days.append(cur_day)
		sub_nums.append(cur_num)
		# next get additional info from accounting JSON where available
		if accounting_df.empty:
			times.append(np.nan)
			week_days.append(np.nan)
		else:
			# match exactly that record in the accounting JSON
			cur_account = accounting_df[(accounting_df["assigned_study_day"]==cur_day) & (accounting_df["adjusted_sound_number"]==cur_num)]
			if cur_account.shape[0] != 1:
				# shouldn't really have this happen in scope of main pipeline if run correctly with no manual interference
				# - but catch this to be safe, will print this warning but also will find obvious due to placement of NaNs in resulting CSV
				print("WARNING: " + filename + " had issue with lookup in JSON accounting CSV, some metadata will be missing for it - shape of filtered DF was actually " + str(cur_account.shape[0]))
				times.append(np.nan)
				week_days.append(np.nan)
			else:
				times.append(cur_account["local_time_converted"].tolist()[0].split(" ")[-1])
				week_days.append(cur_account["assigned_day_of_week"].tolist()[0])
		
		# now set to get the actual transcript QC values for this transcript CSV
		# load in CSV and clear any rows where there is a missing value (should always be a speakerID, timestamp, and text)
		try:
			cur_trans = pd.read_csv(filename)
			cur_trans = cur_trans[["speakerID", "timefromstart", "text"]]
			cur_trans.dropna(how='any',inplace=True)
		except:
			# ignore bad CSV
			print("WARNING: " + filename + " appears to be an incorrectly formatted CSV, QC values will be empty")
			speaker_counts.append(np.nan)
			s1_sens.append(np.nan)
			total_sens.append(np.nan)
			words.append(np.nan)
			min_words.append(np.nan)
			max_words.append(np.nan)
			inauds.append(np.nan)
			questions.append(np.nan)
			brackets.append(np.nan)
			redactions.append(np.nan)
			fin_times.append(np.nan)
			min_spaces.append(np.nan)
			max_spaces.append(np.nan)
			min_spaces_ratio.append(np.nan)
			max_spaces_ratio.append(np.nan)
			encodings.append(np.nan)
			continue 

		# ensure transcript is not empty as well
		if cur_trans.empty:
			print("WARNING: " + filename + " is an empty transcript, QC values will be empty")
			speaker_counts.append(np.nan)
			s1_sens.append(np.nan)
			total_sens.append(np.nan)
			words.append(np.nan)
			min_words.append(np.nan)
			max_words.append(np.nan)
			inauds.append(np.nan)
			questions.append(np.nan)
			brackets.append(np.nan)
			redactions.append(np.nan)
			fin_times.append(np.nan)
			min_spaces.append(np.nan)
			max_spaces.append(np.nan)
			min_spaces_ratio.append(np.nan)
			max_spaces_ratio.append(np.nan)
			encodings.append(np.nan)
			continue
		
		# now can get actual QC values to append - starting with basic counts of structure
		speaker_counts.append(len(set(cur_trans["speakerID"].tolist())))
		cur_trans_S1 = cur_trans[cur_trans["speakerID"]=="S1"]
		s1_sens.append(cur_trans_S1.shape[0])
		# get actual text from full transcript DF to use in all the QC metrics calc
		all_sens = [x.lower() for x in cur_trans["text"].tolist()] # case shouldn't matter in what we are doing here
		total_sens.append(len(all_sens))
		words_per = [len(x.split(" ")) for x in all_sens] # define words here as always space delimitted
		words.append(np.nansum(words_per))
		min_words.append(np.nanmin(words_per))
		max_words.append(np.nanmax(words_per))

		# now counting of TranscribeMe special notations
		inaud_per = [x.count("[inaudible]") for x in all_sens]
		quest_per = [x.count("?]") for x in all_sens] # assume bracket should never follow a ? unless the entire word is bracketed in
		redact_per = [x.count("redacted") for x in all_sens] # should be fine to not bother checking for curly brace here anymore, as we are ones who plugged in REDACTED already
		bracket_per = [x.count("]") - x.count("?]") - x.count("[inaudible]") for x in all_sens]
		inauds.append(np.nansum(inaud_per))
		questions.append(np.nansum(quest_per))
		brackets.append(np.nansum(bracket_per))
		redactions.append(np.nansum(redact_per))

		# now timestamps related metrics
		cur_times = cur_trans["timefromstart"].tolist()
		# convert all timestamps to a float value indicating number of minutes
		try:
			cur_minutes = [float(int(x.split(":")[0]))*60.0 + float(int(x.split(":")[1])) + float(x.split(":")[2])/60.0 for x in cur_times]
		except:
			cur_minutes = [float(int(x.split(":")[0])) + float(x.split(":")[1])/60.0 for x in cur_times] # format sometimes will not include an hours time, so need to catch that
		# get last timestamp - note this will be for the time *before* the last sentence
		fin_times.append(round(cur_minutes[-1],3))
		# convert the minutes to a number of seconds for spacing features
		cur_seconds = [m * 60.0 for m in cur_minutes] 
		differences_list = [j - i for i, j in zip(cur_seconds[: -1], cur_seconds[1 :])]
		if len(differences_list) == 0:
			# current transcript is of minimal length (1 sentence), so no valid timestamp differences, append nan
			min_spaces.append(np.nan)
			max_spaces.append(np.nan)
			min_spaces_ratio.append(np.nan)
			max_spaces_ratio.append(np.nan)
		else:
			# use round so values are reasonably viewable on DPDash
			min_spaces.append(round(np.nanmin(differences_list),3))
			max_spaces.append(round(np.nanmax(differences_list),3))
			weighted_list = [j/float(i) for i, j in zip(words_per[: -1], differences_list)]
			min_spaces_ratio.append(round(np.nanmin(weighted_list),3))
			max_spaces_ratio.append(round(np.nanmax(weighted_list),3))

		# finally just get the transcript text encoding type
		txt_name = os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", "redacted_copies", filename.split(".csv")[0] + ".txt")
		if not os.path.isfile(txt_name):
			# should not happen in scope of main pipeline
			# but would be a problem if so as the redacted txts are main thing planned to go to NDA
			print("WARNING: " + filename + " does not have expected corresponding txt file!")
			encodings.append(np.nan)
			continue
			
		with open(txt_name, 'rb') as fd:
			ascii_bool = fd.read().isascii()
		if not ascii_bool:
			try:
				ftest = open(txt_name, encoding="utf-8", errors="strict")
				encodings.append("UTF-8")
			except:
				# shouldn't really get to this if going through main pipeline, as non-UTF8 is supposed to be "rejected" earlier than this step
				print("WARNING: " + filename + " may have an encoding problem!")
				encodings.append(np.nan)
		else:
			encodings.append("ASCII")

	# get pt and site lists, and also a list of nans for reftime column (values optional but column must exist per DPDash)
	sites = [site for x in range(len(fnames))]
	subjects = [subject for x in range(len(fnames))]
	ref_times = [np.nan for x in range(len(fnames))]
	cur_date = datetime.date.today().strftime("%Y-%m-%d")
	trans_dates = [cur_date for x in range(len(fnames))]

	# construct current CSV
	values = [ref_times, site_days, times, week_days, sites, subjects, sub_nums, speaker_counts, s1_sens, 
			  total_sens, words, min_words, max_words, inauds, questions, brackets, redactions, fin_times, 
			  min_spaces, max_spaces, min_spaces_ratio, max_spaces_ratio, encodings, fnames, trans_dates]
	new_csv = pd.DataFrame()
	for i in range(len(headers)):
		h = headers[i]
		vals = values[i]
		new_csv[h] = vals

	# now can concat with prior records, where applicable
	if len(prev_trans_list) == 0:
		final_save = new_csv
	else:
		final_save = pd.concat([old_df,new_csv]).reset_index(drop=True)
	# specify main save path under PROTECTED here
	save_path = os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs", site + "_" + subject + "_" + "diaryTranscriptQC.csv")
	final_save.to_csv(save_path,index=False)

	# finally also deal with saving copy to be pushed to predict under GENERAL, matching expectations for DPDash
	final_save.sort_values(by="day",inplace=True) # should already be sorted but ensure before grabbing day number for name
	start_day = final_save["day"].dropna().tolist()[0]
	end_day = final_save["day"].dropna().tolist()[-1]
	# also drop any duplicates in case transcript got processed a second time - shouldn't happen via pipeline though!
	final_save.drop_duplicates(subset=["day", "timeofday"],inplace=True)
	final_save.drop(columns=["date_transcript_processed"],inplace=True)
	dpdash_name = site[-2:] + "-" + subject + "-diaryTranscriptQC-day" + str(start_day) + "to" + str(end_day) + ".csv"
	dpdash_folder = os.path.join(data_root, "GENERAL", site, "processed", subject, "phone", "audio_journals")
	try:
		os.chdir(dpdash_folder)
	except:
		print("WARNING: correct folder structure does not exist for push of DPDash CSV to predict server, please fix for future monitoring updates")
		return
	for old_dpdash in glob.glob(site[-2:] + "-" + subject + "-diaryTranscriptQC-day*.csv"):
		os.remove(old_dpdash)
	final_save.to_csv(dpdash_name,index=False)

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	transcript_diary_qc(sys.argv[1], sys.argv[2], sys.argv[3])

