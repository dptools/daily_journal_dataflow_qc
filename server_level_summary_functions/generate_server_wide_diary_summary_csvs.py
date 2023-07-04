#!/usr/bin/env python

import os
import glob
import pandas as pd
import numpy as np
import sys
import datetime
import math

# creates per subject ID and per site summary CSVs that can be used in downstream reporting/visualizations
# mainly includes summary info on participation/engagement rates
def diary_csv_summarize(data_root, output_folder):
	# check necessary root (PROTECTED side) folder exists in order to change directories into it
	try:
		os.chdir(os.path.join(data_root,"PROTECTED"))
	except:
		# if pipeline setup correctly should not hit this error
		print("ERROR: given data_root invalid, please check function arguments") 
		return
	# make sure output directory exists as well
	# (note should be an absolute path)
	if not os.path.isdir(output_folder):
		# also should not be issue in actual pipeline
		print("ERROR: given output_folder invalid, please check function arguments")
		return

	# relying on python function run by pipeline right before this -
	# if they were possible to make, the output_folder should also already contain these 3 CSVs:
	#   "allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv"
	#   "allSubjectsServerWide_audioQCRejectedJournals_dataLog.csv"
	#   "allSubjectsServerWide_audioJournalMajorIssuesLog.csv"
	# the latter 2 not really needed here but the successfulJournals record will be useful
	# also if it doesn't exist there isn't really much summarizing worth doing
	# so going to go ahead and try to load it, exit otherwise 
	# - treat this function as mainly only relevant in scope of pipeline 
	# (visualization step can be a somewhat more general module)

	if not os.path.isfile(os.path.join(output_folder,"allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv")):
		print("WARNING: no QC has successfully been compiled across this server yet - nothing much to summarize, exiting")
		return
	processed_combined = pd.read_csv(os.path.join(output_folder,"allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv"))

	# besides summaries based on this processed_combined, also a few things to check on server
	# (both folder structure info and loading in from the surface level JSON tracking)

	# will do that first, starting with JSON CSVs
	json_csvs = glob.glob("*/processed/*/phone/audio_journals/file_accounting_details/*_appActivitiesJSONAccounting.csv")
	if len(json_csvs) == 0:
		print("WARNING: no metadata info found under processed across the server, some issue with pipeline outputs - exiting")
		return		
	json_dfs = [pd.read_csv(x) for x in json_csvs]
	json_sites = [x.split("/")[-1].split("_")[0] for x in json_csvs]
	json_subject_ids = [x.split("/")[-1].split("_")[1] for x in json_csvs]
	for df,st,pt in zip(json_dfs,json_sites,json_subject_ids):
		df["site"] = [st for x in range(df.shape[0])]
		df["subject"] = [pt for x in range(df.shape[0])]
	json_combined = pd.concat(json_dfs).reset_index(drop=True)
	json_combined["ema_day_bool"] = json_combined["ema_records_count"].clip(upper=1)
	json_combined["diary_day_bool"] = json_combined["diary_records_count"].clip(upper=1)
	json_combined["active_day_bool"] = (json_combined["ema_day_bool"] + json_combined["diary_day_bool"]).clip(upper=1)
	grouping_rename = {"active_day_bool":"num_days_any_activity_submit","ema_day_bool":"num_days_ema_submit","diary_day_bool":"num_days_journal_submit"}
	subject_id_json = json_combined[["subject","active_day_bool","ema_day_bool","diary_day_bool"]].groupby("subject", as_index=False).sum().reset_index(drop=True)
	subject_id_json.rename(columns=grouping_rename,inplace=True)
	sites_json = subject_id_json.merge(json_combined[["subject","site"]].drop_duplicates(),on="subject",how="left")
	sites_json["any_activity_bool"] = sites_json["num_days_any_activity_submit"].clip(upper=1)
	sites_json["any_ema_bool"] = sites_json["num_days_ema_submit"].clip(upper=1)
	sites_json["any_diary_bool"] = sites_json["num_days_journal_submit"].clip(upper=1)
	sites_json_group = sites_json[["site","any_activity_bool","any_ema_bool","any_diary_bool"]].groupby("site", as_index=False).sum().reset_index(drop=True)
	site_grouping_rename = {"any_activity_bool":"num_subjects_any_active_app","any_ema_bool":"num_subjects_any_ema","any_diary_bool":"num_subjects_any_journal"}
	sites_json_group.rename(columns=site_grouping_rename,inplace=True)
	site_prefix = sites_json_group["site"].tolist()[0][:-2]

	# then for site-level summary, also get folder structure info to merge with site-level JSON summary
	sites_list = os.listdir(".")
	sites_list_filt = [x for x in sites_list if os.path.isdir(x) and x.startswith(site_prefix)]
	num_subjects_total = []
	num_subjects_phone_protected = []
	for site in sites_list_filt:
		try:
			cur_sub_list = os.listdir(os.path.join(site,"raw"))
		except:
			cur_sub_list = []
		try:
			for gen_side_sub in os.listdir(os.path.join(data_root,"GENERAL",site,"raw")):
				if gen_side_sub not in cur_sub_list:
					cur_sub_list.append(gen_side_sub)
		except:
			pass
		cur_sub_list_filter = [x for x in cur_sub_list if os.path.isdir(os.path.join(site,"raw",x)) and x[0:2] == site[-2:]]
		num_subjects_total.append(len(cur_sub_list_filter))
		cur_subs_raw_phone = glob.glob(site + "/raw/*/phone")
		num_subjects_phone_protected.append(len(cur_subs_raw_phone))
	basic_server_df = pd.DataFrame()
	basic_server_df["site"] = sites_list_filt
	basic_server_df["num_subjects_found"] = num_subjects_total
	basic_server_df["num_subjects_raw_protected_phone_folder"] = num_subjects_phone_protected
	combined_subject_counts = basic_server_df.merge(sites_json_group,on="site",how="outer").fillna(0)

	# on subjects and site level, add count and duration sum of audio uploaded to TranscribeMe
	site_durs = processed_combined[["site","length_minutes"]].groupby("site", as_index=False).sum().reset_index(drop=True).rename(columns={"length_minutes":"sum_minutes_audio_uploaded"})
	site_counts = processed_combined[["site","length_minutes"]].groupby("site", as_index=False).count().reset_index(drop=True).rename(columns={"length_minutes":"num_audio_files_uploaded"})
	site_stats_combo = site_durs.merge(site_counts,on="site",how="inner").reset_index(drop=True)
	site_stats_combo_meta = combined_subject_counts.merge(site_stats_combo,on="site",how="outer").reset_index(drop=True)
	subject_durs = processed_combined[["subject","length_minutes"]].groupby("subject", as_index=False).sum().reset_index(drop=True).rename(columns={"length_minutes":"sum_minutes_audio_uploaded"})
	subject_counts = processed_combined[["subject","length_minutes"]].groupby("subject", as_index=False).count().reset_index(drop=True).rename(columns={"length_minutes":"num_audio_files_uploaded"})
	subject_stats_combo = subject_durs.merge(subject_counts,on="subject",how="inner").reset_index(drop=True)
	subject_stats_combo_meta = subject_id_json.merge(subject_stats_combo,on="subject",how="outer").reset_index(drop=True)

	# for each subject want to know how many days they theoretically could submit so far
	cur_date_str = datetime.date.today().strftime("%Y-%m-%d")
	cur_date = datetime.datetime.strptime(cur_date_str,"%Y-%m-%d")
	# should only ever be one consent date per subject, but add drops to be safe here (error would be logged elsewhere regarding changed consent)
	processed_combined_filt = processed_combined[["subject","consent_date_at_accounting"]].dropna(how="any").drop_duplicates(subset=["subject"])
	processed_combined_filt["study_day_at_compute_time"] = [(cur_date - datetime.datetime.strptime(x,"%Y-%m-%d")).days + 1 for x in processed_combined_filt["consent_date_at_accounting"].tolist()]
	subject_stats_add_time_int = subject_stats_combo_meta.merge(processed_combined_filt[["subject","study_day_at_compute_time"]],on="subject",how="left").reset_index(drop=True)
	# also add day number of first and last successful diaries for reference
	first_days = processed_combined[["subject","day"]].sort_values(by="day").drop_duplicates(subset=["subject"]).rename(columns={"day":"first_submit_day"}).reset_index(drop=True)
	last_days = processed_combined[["subject","day"]].sort_values(by="day",ascending=False).drop_duplicates(subset=["subject"]).rename(columns={"day":"last_submit_day"}).reset_index(drop=True)
	both_days = first_days.merge(last_days,on="subject",how="inner")
	subject_stats_add_time = subject_stats_add_time_int.merge(both_days,on="subject",how="left").reset_index(drop=True)

	# on site level add count of diary subjects within first 2 weeks of consent at present
	# and then also fraction of diary subjects past first 2 weeks that submitted a journal past that time
	# then can give count of all diary subjects that submitted something in most recent 2 weeks
	# may need to adjust parts of this somewhat as we get a better sense for participation timescales 
	# (and lag between project consent and first journal submission)
	subject_stats_add_time["first_two_weeks_bool"] = [1 if x <= 14 else 0 for x in subject_stats_add_time["study_day_at_compute_time"].tolist()]
	subject_stats_add_time["past_two_weeks_bool"] = [1 if x > 14 else 0 for x in subject_stats_add_time["study_day_at_compute_time"].tolist()]
	subject_stats_add_time["past_two_weeks_submit_bool"] = [1 if x > 14 else 0 for x in subject_stats_add_time["last_submit_day"].tolist()]
	subject_stats_add_time["time_since_last_submit"] = subject_stats_add_time["study_day_at_compute_time"] - subject_stats_add_time["last_submit_day"]
	subject_stats_add_time["recent_two_weeks_submit_bool"] = [1 if x <= 14 else 0 for x in subject_stats_add_time["time_since_last_submit"].tolist()]
	subject_stats_add_time["site"] = [site_prefix + x[0:2] for x in subject_stats_add_time["subject"].tolist()]
	site_new_subjects_count = subject_stats_add_time[["site","first_two_weeks_bool","past_two_weeks_bool","past_two_weeks_submit_bool","recent_two_weeks_submit_bool"]].groupby("site", as_index=False).sum().reset_index(drop=True)
	time_group_rename = {"first_two_weeks_bool":"num_subjects_within_first_two_weeks_of_enrollment","recent_two_weeks_submit_bool":"num_subjects_submit_within_last_two_weeks"}
	site_new_subjects_count.rename(columns=time_group_rename,inplace=True)
	site_new_subjects_count["fraction_diary_subjects_submit_after_two_weeks"] = site_new_subjects_count["past_two_weeks_submit_bool"]/site_new_subjects_count.past_two_weeks_bool.astype(float)
	site_new_subjects_count.drop(columns=["past_two_weeks_bool","past_two_weeks_submit_bool"],inplace=True)
	site_stats_add_time_int = site_stats_combo_meta.merge(site_new_subjects_count,on="site",how="left").reset_index(drop=True)
	subject_stats_add_time.drop(columns=["first_two_weeks_bool","past_two_weeks_bool","past_two_weeks_submit_bool","recent_two_weeks_submit_bool"],inplace=True)

	post_2week = processed_combined[processed_combined["day"]>14].dropna(subset=["day","subject","site","length_minutes"],how="any")
	pre_2week = processed_combined[processed_combined["day"]<=14].dropna(subset=["day","subject","site","length_minutes"],how="any")
	post_2_week_mins = post_2week[["site","length_minutes"]].groupby("site", as_index=False).mean().reset_index(drop=True).rename(columns={"length_minutes":"mean_minutes_per_diary_after_two_weeks"})
	pre_2_week_mins = pre_2week[["site","length_minutes"]].groupby("site", as_index=False).mean().reset_index(drop=True).rename(columns={"length_minutes":"mean_minutes_per_diary_first_two_weeks"})
	get_week_assign = subject_stats_add_time[["subject","study_day_at_compute_time"]].dropna(how="any")
	get_week_assign["week_assign"] = [math.ceil(x/7.0) - 2 for x in get_week_assign["study_day_at_compute_time"].tolist()]
	post_2_week_counts = post_2week[["site","length_minutes"]].groupby("site", as_index=False).count().reset_index(drop=True).rename(columns={"length_minutes":"num_diary_after_two_weeks"})
	num_weeks_valid_subjects = post_2week.merge(get_week_assign[["subject","week_assign"]],on="subject",how="left").drop_duplicates(subset=["subject"])
	num_weeks_merger = num_weeks_valid_subjects[["site","week_assign"]].groupby("site", as_index=False).sum().reset_index(drop=True).rename(columns={"week_assign":"num_weeks_so_far"})
	post_2_weeks_rate = post_2_week_counts.merge(num_weeks_merger,on="site",how="inner").reset_index(drop=True)
	post_2_weeks_rate["mean_accepted_diaries_per_subject_week_after_two_weeks"] = post_2_weeks_rate["num_diary_after_two_weeks"]/post_2_weeks_rate.num_weeks_so_far.astype(float)
	pre_2_week_subjects_avail = pre_2week[["site","subject"]].groupby("site", as_index=False).nunique().reset_index(drop=True).rename(columns={"subject":"subject_count"})
	pre_2_week_diaries_avail = pre_2week[["site","subject"]].groupby("site", as_index=False).count().reset_index(drop=True).rename(columns={"subject":"diary_count"})
	pre_2_weeks_rate = pre_2_week_subjects_avail.merge(pre_2_week_diaries_avail, on="site", how="inner").reset_index(drop=True)
	pre_2_weeks_rate["mean_accepted_diaries_per_subject_within_first_two_weeks"] = pre_2_weeks_rate["diary_count"]/pre_2_weeks_rate.subject_count.astype(float)
	site_stats_add_time_post = site_stats_add_time_int.merge(post_2_week_mins,on="site",how="left").merge(post_2_weeks_rate[["site","mean_accepted_diaries_per_subject_week_after_two_weeks"]],on="site",how="left").reset_index(drop=True)
	site_stats_add_time = site_stats_add_time_post.merge(pre_2_week_mins,on="site",how="left").merge(pre_2_weeks_rate[["site","mean_accepted_diaries_per_subject_within_first_two_weeks"]],on="site",how="left").reset_index(drop=True)

	# now can just save! other summaries will really require visualizations to better understand, so that can be done in other function
	subject_stats_add_time.to_csv(os.path.join(output_folder,"serverWide_subjectsLevel_journalSubmissionSummary.csv"),index=False)
	site_stats_add_time.to_csv(os.path.join(output_folder,"serverWide_sitesLevel_journalSubmissionSummary.csv"),index=False)

	return

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	diary_csv_summarize(sys.argv[1], sys.argv[2])

