#!/usr/bin/env python

import os
import glob
import sys
import re
import pandas as pd 
import numpy as np

# for input subject, find any newly produced redacted transcript CSVs and generate a CSV version that includes some basic stats for each sentence
# csv production mainly done by below helper function so could be run in more modular fashion if desired
# sentence stats include sentence-level versions of some of the basic counting stats from transcript-wide QC, plus duration estimated from timestamps
# also then includes disfluency count stats, which top level function in this script will use to create a transcript summary CSV about disfluencies
# this main function called using script does assume basic folder structure and transcript file naming conventions will match expectations set out
def transcript_sentence_stats_loop(data_root, site, subject):
	# first confirm all necessary set up for the function, in case something weird happens or it gets used incorrectly outside scope of pipeline
	try:
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", "redacted_copies", "csv"))
	except:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no redacted transcript CSVs yet for input subject " + subject + ", or problem with input arguments") 
		return
	cur_files = glob.glob("*.csv")
	if len(cur_files) == 0:
		# should generally not reach this warning if calling from main pipeline bash script
		print("WARNING: no redacted transcript CSVs yet for input subject " + subject)
		return
	if not os.path.isdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", "redacted_csvs_with_stats")):
		# again shouldn't get here within larger pipeline
		print("WARNING: output folders not correctly set up yet for input subject " + subject + " - please address in order to save transcript sentence stat outputs")
		return
	else:
		output_root = os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", "redacted_csvs_with_stats")
	if not os.path.isdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs")):
		# again shouldn't get here within larger pipeline
		print("WARNING: output folders not correctly set up yet for input subject " + subject + " - please address in order to save transcript sentence stat outputs")
		return

	# now check for existing audio QC output
	if not os.path.isfile(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs", site + "_" + subject + "_" + "diaryAudioQC.csv")):
		print("WARNING: no audio QC found for subject " + subject + " - will proceed with process but sentence duration estimates will be incomplete, and this could be sign of a larger issue")
		aud_qc_ref = pd.DataFrame()
	else:
		aud_qc_ref = pd.read_csv(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs", site + "_" + subject + "_" + "diaryAudioQC.csv"))

	new_count = 0 # track what is actually new versus a transcript previously encountered
	# initialize list for disfluencies info
	new_fnames = []
	new_days = []
	new_sub_nums = []
	new_durations = []
	new_word_counts = [] # adding this and audio dur where available for normalizing purposes
	non_edits = []
	verb_edits = []
	repeats = []
	restarts = []
	for trans in cur_files:
		trans_name = trans.split(".csv")[0]
		processed_trans_name = trans_name + "_withSentenceStats.csv"
		if not os.path.isfile(os.path.join(output_root,processed_trans_name)):
			# means not yet processed, so call the helper function (defined below) and update count
			new_count = new_count + 1
			# but first will get duration info for this interview to input if possible
			cur_day = int(trans.split("_day")[-1].split("_")[0])
			cur_sub_num = int(trans.split("_submission")[-1].split("_")[0])
			if aud_qc_ref.empty:
				cur_audio = None
			else:
				cur_check = aud_qc_ref[(aud_qc_ref["day"]==cur_day)&(aud_qc_ref["daily_submission_number"]==cur_sub_num)]
				if cur_check.shape[0] != 1:
					print("WARNING: could not identify unique record in audio QC for transcript " + trans)
					cur_audio = None
				else:
					cur_audio = cur_check["overall_db"].tolist()[0]
			# now ready to actually call - function will save main sentence stats CSV, and then use cur_df to get disfluency summary stat info to add
			cur_df = transcript_sentence_stats(trans,os.path.join(output_root,processed_trans_name),audio_duration=cur_audio)
			if not cur_df.empty:
				# append to the core disfluency CSV lists
				new_fnames.append(processed_trans_name)
				new_days.append(cur_day)
				new_sub_nums.append(cur_sub_num)
				if cur_audio is None:
					new_durations.append(np.nan)
				else:
					new_durations.append(cur_audio)
				new_word_counts.append(np.nansum(cur_df["word_count"].tolist()))
				non_edits.append(np.nansum(cur_df["nonverbal_edits"].tolist()))
				verb_edits.append(np.nansum(cur_df["verbal_edits"].tolist()))
				repeats.append(np.nansum(cur_df["repeats"].tolist()))
				restarts.append(np.nansum(cur_df["restarts"].tolist()))
			else:
				# indicates a problem was encountered with the input transcript
				new_count = new_count - 1
			
	print(str(new_count) + " total transcripts newly processed for subject " + subject)

	if new_count > 0:
		print("Finally, updating disfluencies summary stat table for " + subject + " with new data")

		# construct data frame from newly collected values
		disf_df = pd.DataFrame()
		disf_df["transcript_csv_name"] = new_fnames
		disf_df["day"] = new_days
		disf_df["daily_submission_number"] = new_sub_nums
		disf_df["length_minutes"] = new_durations
		disf_df["total_word_count"] = new_word_counts
		disf_df["nonverbal_edits_count"] = non_edits
		disf_df["verbal_edits_count"] = verb_edits
		disf_df["repeats_count"] = repeats
		disf_df["restarts_count"] = restarts

		# add additional summary stats
		disf_df["total_disfluencies"] = disf_df["nonverbal_edits_count"] + disf_df["verbal_edits_count"] + disf_df["repeats_count"] + disf_df["restarts_count"]
		disf_df["nonverbal_edits_per_word"] = disf_df["nonverbal_edits_count"]/disf_df.total_word_count.astype(float)
		disf_df["verbal_edits_per_word"] = disf_df["verbal_edits_count"]/disf_df.total_word_count.astype(float)
		disf_df["repeats_per_word"] = disf_df["repeats_count"]/disf_df.total_word_count.astype(float)
		disf_df["restarts_per_word"] = disf_df["restarts_count"]/disf_df.total_word_count.astype(float)
		disf_df["disfluencies_per_minute"] = disf_df["total_disfluencies"]/disf_df["length_minutes"]

		# now prep to save
		os.chdir(os.path.join(data_root,"PROTECTED", site, "processed", subject, "phone", "audio_journals", "dpdash_source_csvs"))
		# if it exists already concat with prior results first
		if os.path.isfile(site + "_" + subject + "_" + "diaryDisfluencies.csv"):
			old_df = pd.read_csv(site + "_" + subject + "_" + "diaryDisfluencies.csv")
			updated_df = pd.concat([old_df, disf_df])
			updated_df.sort_values(by="transcript_csv_name",inplace=True)
			updated_df.to_csv(site + "_" + subject + "_" + "diaryDisfluencies.csv", index=False)
		else:
			disf_df.sort_values(by="transcript_csv_name",inplace=True)
			disf_df.to_csv(site + "_" + subject + "_" + "diaryDisfluencies.csv", index=False)

	return

# helper function for performing the stats calculation for a given transcript 
# returns df in addition to saving at provided savepath
# if audio duration is provided (generally should be within above pipeline loop) then that can give a length estimate for final sentence in transcript
def transcript_sentence_stats(filename, savepath, audio_duration=None):
	# do argument sanity check first
	# if calling via this file's main, shouldn't hit these messages
	if not os.path.isfile(filename):
		print("Input transcript path is not a file (" + filename + "), skipping")
		return
	if os.path.exists(savepath):
		print("Intended output path already exists (" + savepath + "), skipping")
		return

	try:
		cur_trans = pd.read_csv(filename)
		cur_trans = cur_trans[["speakerID", "timefromstart", "text"]]
		cur_trans.dropna(how='any',inplace=True)
	except:
		print("WARNING: " + filename + " appears to be an incorrectly formatted CSV, skipping")
		return pd.DataFrame()

	if cur_trans.empty:
		print("WARNING: " + filename + " is an empty transcript, skipping")
		return pd.DataFrame()

	# get actual text from full transcript DF to use in all the QC metrics calc
	all_sens = [x.lower() for x in cur_trans["text"].tolist()] # case shouldn't matter in what we are doing here
	# start with basic counting
	words_per = [len(x.split(" ")) for x in all_sens]
	cur_trans["word_count"] = words_per
	bad_per = [x.count("?]") + x.count("[inaudible]") for x in all_sens]
	bracket_per = [x.count("]") - x.count("?]") - x.count("[inaudible]") for x in all_sens]
	cur_trans["inaudibles_and_questionables"] = bad_per
	cur_trans["other_bracketed_words"] = bracket_per
	redact_per = [x.count("redacted") for x in all_sens]
	cur_trans["redactions"] = redact_per

	# add sentence duration info
	cur_times = cur_trans["timefromstart"].tolist()
	# convert all timestamps to a float value indicating number of minutes
	try:
		cur_minutes = [float(int(x.split(":")[0]))*60.0 + float(int(x.split(":")[1])) + float(x.split(":")[2])/60.0 for x in cur_times]
	except:
		cur_minutes = [float(int(x.split(":")[0])) + float(x.split(":")[1])/60.0 for x in cur_times] # format sometimes will not include an hours time, so need to catch that
	# convert the minutes to a number of seconds for spacing
	cur_seconds = [m * 60.0 for m in cur_minutes] 
	differences_list = [j - i for i, j in zip(cur_seconds[: -1], cur_seconds[1 :])]
	# now just add the final length in seconds using audio duration if available
	if audio_duration is not None:
		differences_list.append(audio_duration*60.0 - cur_seconds[-1])
	else:
		differences_list.append(np.nan)
	cur_trans["estimated_sentence_seconds"] = differences_list

	# now finally add the disfluencies info, starting with nonverbal edits
	# use regex for nonverbal edits now to improve accuracy (don't include e.g. lithium)
	reg_ex_pattern = "[^a-z]u+[hm]+[^a-z]"
	words_lists = [x.split(" ") for x in all_sens]
	# add extra spaces to the words in the sentences for the regex, to ensure it can still match even when looking for non-alphabet chars surrounding
	reg_ex_list_hack = [" " + "  ".join(x) + " " for x in words_lists]
	uhum_per = [len(re.findall(reg_ex_pattern, x)) for x in reg_ex_list_hack]
	cur_trans["nonverbal_edits"] = uhum_per

	# now verbal edits
	like_per = [x.count("like,") for x in all_sens]
	know_per = [x.count("you know,") for x in all_sens]
	mean_per = [x.count("i mean,") for x in all_sens]
	# might consider expanding this list at some point to include other verbal fillers? but these are main ones used
	# (and similarly for other nonverbal filler sounds above?)
	cur_trans["verbal_edits"] = [x + y + z for x,y,z in zip(like_per,know_per,mean_per)]

	# now repeats - split into two types and then also save sum
	# for repetitions, looking for either repetition of characters after a single dash (no spaces)
	# or repetition of actual words in a sentence (splitting on space but also counting repetition if comma appears as the punctuation on either of the two words)
	dash_repetition = [np.nansum([1 if len(y.split("-")) > 1 and len(y.split("-")[0]) <= len(y.split("-")[1]) and y.split("-")[0]==y.split("-")[1][0:len(y.split("-")[0])] else 0 for y in x.split(" ")]) for x in all_sens]
	word_repetition = [np.nansum([1 if x.split(" ")[y-1].replace(",","")==x.split(" ")[y].replace(",","") else 0 for y in range(1,len(x.split(" ")))]) for x in all_sens]
	cur_trans["stutter_repeats"] = dash_repetition
	cur_trans["word_repeats"] = word_repetition
	cur_trans["repeats"] = [x + y for x,y in zip(dash_repetition, word_repetition)]
	# largely combined repeats in analysis in past so keeping that way at summary level, but may be worth looking into the two categories in more detail via these sentence level outputs soon

	# finally restarts
	ddash_per = [x.count("--") for x in all_sens] # estimate of sentence restarts, could also be long mid-sentence pause per TranscribeMe notation
	cur_trans["restarts"] = ddash_per

	cur_trans.to_csv(savepath, index=False)
	return cur_trans

	# Note this (and overall transcript QC) do not take into account differences for non-English languages yet

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	transcript_sentence_stats_loop(sys.argv[1], sys.argv[2], sys.argv[3])
