#!/usr/bin/env python

import os
import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
from datetime import date
import math

# wrapper function that will generate all visualizations for diary pipeline monitoring, called in main of current script
# see comments within this function for what each visual specifically entails for the diary code
# general helper functions called to actually make each type of figure can be found below this main function
# currently e.g. histogram bin settings and feature names are hard-coded, 
#  -- but the helper functions would make this very easy to use differently/change within this code if desired
def diary_monitoring_visuals(source_folder):
	# check given folder exists in order to change directories into it
	try:
		os.chdir(source_folder)
	except:
		# if pipeline setup correctly should not hit this error
		print("ERROR: given source_folder invalid, please check function arguments") 
		return

	# 2 main sources expected in this folder for generating visuals from are:
	# serverWide_subjectsLevel_journalSubmissionSummary.csv
	# allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv
	# (for those running outside of pipeline - 
	# - if only one exists, the visuals that can be made with just this will be)
	# generated outputs will be saved to the same source folder

	# get date info for labeling PDFs
	cur_date = date.today().strftime("%m/%d/%Y")

	# after making a few more isolated figures will only proceed with rest of function if all inputs desired are available
	proceed = True

	if os.path.isfile("allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv"):
		combined_qc = pd.read_csv("allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv")
		# setup more readable site IDs for later legends
		combined_qc["siteID"] = [x[-2:] for x in combined_qc["site"].tolist()]
		# get server name automatically from site IDs as well 
		# (note this is fairly specific to AMPSCZ convention)
		cur_server = combined_qc["site"].dropna().tolist()[0][0:-2] 

		# first look at features to distribute over diaries without any coloring - will be single page PDF
		disfluencies = ["nonverbal_edits_per_word", "verbal_edits_per_word", "repeats_per_word", "restarts_per_word"]
		disfluencies_df = combined_qc[disfluencies].dropna()
		if disfluencies_df.empty:
			# this may not be a huge issue early on in running the code 
			# (and is not inherently a huge issue, though as time goes on could signal other major issues to be aware of)
			print("Note no disfluency rate data found on this server yet, skipping that visual")
		else:
			disf_title = cur_server + " Audio Journal General Distributions of Disfluency Rates" + "\n" + "(Data as of " + cur_date + ")"
			stacked_histograms_with_labels(disfluencies_df,[disfluencies],"allDiariesServerWide_disfluenciesDistributions.pdf",
								   		   counted_object_name="Diaries",plot_suptitle=disf_title)

		# next will consider QC features to distribute over diaries with site-level coloring
		# first make a few rate-based features where applicable
		if combined_qc.dropna(subset=["word_count"]).shape == 0:
			print("Note no transcript QC data found on this server yet, QC visuals will be shortened")
			qc_feat_list = [["day", "submit_hour_int", "weekday", "length_minutes"],
							["overall_db", "mean_flatness"]]
			qc_bins = [[list(range(1,365,14)),list(range(4,28)),list(range(1,8)),[0.0,0.1,0.25,0.5,0.75,1.0,1.5,2.0,3.0,4.0]],
					   [[40,45,50,55,60,65,70,75,80,90,100],25]]
			qc_title = cur_server + " Uploaded Audio Journal QC Metric Distributions by Site" + "\n" + "(Data as of " + cur_date + " - note no corresponding transcripts available at this time)"
		
			proceed=False
		else:
			# add the rate based features needed here
			combined_qc["words_per_sentence"] = [x/float(y) if not np.isnan(x) else np.nan for x,y in zip(combined_qc["word_count"].tolist(),combined_qc["total_sentence_count"].tolist())]
			combined_qc["inaudible_rate_per_word"] = [x/float(y) if not np.isnan(y) else np.nan for x,y in zip(combined_qc["inaudible_count"].tolist(),combined_qc["word_count"].tolist())]
			combined_qc["questionable_rate_per_word"] = [x/float(y) if not np.isnan(y) else np.nan for x,y in zip(combined_qc["questionable_count"].tolist(),combined_qc["word_count"].tolist())]
			combined_qc["other_brackets_rate_per_word"] = [x/float(y) if not np.isnan(y) else np.nan for x,y in zip(combined_qc["other_bracketed_notation_count"].tolist(),combined_qc["word_count"].tolist())]
			combined_qc["redacted_rate_per_word"] = [x/float(y) if not np.isnan(y) else np.nan for x,y in zip(combined_qc["redacted_count"].tolist(),combined_qc["word_count"].tolist())]

			qc_feat_list = [["day", "submit_hour_int", "weekday", "length_minutes"],
							["overall_db", "mean_flatness", "speakerID_count", "total_sentence_count"],
							["word_count", "words_per_sentence", "min_timestamp_space_seconds", "max_timestamp_space_seconds"],
							["inaudible_count", "questionable_count", "other_bracketed_notation_count", "redacted_count"],
							["inaudible_rate_per_word", "questionable_rate_per_word", "other_brackets_rate_per_word", "redacted_rate_per_word"]]
			rate_bin_list = [0,0.001,0.005,0.01,0.02,0.03,0.04,0.05]
			qc_bins = [[list(range(1,365,14)),list(range(4,28)),list(range(1,8)),[0.0,0.1,0.25,0.5,0.75,1.0,1.5,2.0,3.0,4.0]],
					   [[40,45,50,55,60,65,70,75,80,90,100],20,[1,2,3],15],
					   [30,20,10,10],[10,10,10,10],[rate_bin_list,rate_bin_list,rate_bin_list,rate_bin_list]]
			qc_title = cur_server + " Uploaded Audio Journal QC Metric Distributions by Site" + "\n" + "(Data as of " + cur_date + ")"

		stacked_histograms_with_labels(combined_qc,qc_feat_list,"allDiariesServerWide_QCDistributions_coloredBySite.pdf",
									   dividing_feature_name="siteID",bin_specs=qc_bins,counted_object_name="Diaries",plot_suptitle=qc_title)
	
		# now select qc features to distribute over diaries separately for each site with subject-level coloring
		# (just exclude sites with no transcript data yet - won't make at all if none do, no need to print extra warning since already did)
		if len(qc_feat_list) > 2:
			# note this may fail eventually if more than 30 subjects for a site start having journal data, would have to tweak approach
			fine_feat_list = ["length_minutes", "total_sentence_count", "word_count", "inaudible_rate_per_word"]
			fine_bin_list = [[0.0,0.1,0.25,0.5,0.75,1.0,1.5,2.0,3.0,4.0], 15, 30, rate_bin_list]

			sites_list_trans = list(set(combined_qc.dropna(subset=["word_count"])["site"].tolist()))
			sites_list_trans.sort()
			fine_dfs_list = [combined_qc[combined_qc["site"]==x] for x in sites_list_trans]

			fine_pdf = "diariesBySiteServerWide_selectQCDistributions_coloredBySubject.pdf"
			fine_title = cur_server + " Site-Specific Select Journal QC Metric Distributions by Subject" + "\n" + "(Data as of " + cur_date + ")"

			stacked_histograms_different_dfs(fine_dfs_list,sites_list_trans,fine_feat_list,fine_pdf,
											 dividing_feature_name="subject",n_bins=fine_bin_list,
											 counted_object_name="Diaries",plot_suptitle=fine_title)

			# will save server-wide version without coloring here as well, to use jpg in sendmail summary
			fine_title2 = cur_server + " Audio Journal General Distributions of Select QC Metrics" + "\n" + "(Data as of " + cur_date + ")"
			stacked_histograms_with_labels(combined_qc,[fine_feat_list],"allDiariesServerWide_keyQCMetricDistributions.pdf",
										   bin_specs=[fine_bin_list],counted_object_name="Diaries",plot_suptitle=fine_title2,
										   jpeg_save_tuple=(0,"serverWide_keyQCHistogram.jpg"))
	else:
		print("WARNING: no combined QC CSV found - could signal major issue if this was called from pipeline")
		print("Most visualizations will be skipped in this case, but will next check for subjects level summary CSV")
		proceed=False

	# moving on to per subject stats CSV for histograms
	if os.path.isfile("serverWide_subjectsLevel_journalSubmissionSummary.csv"):
		subject_qc = pd.read_csv("serverWide_subjectsLevel_journalSubmissionSummary.csv")
		# remove subjects that haven't been involved with diaries here
		subject_qc = subject_qc[subject_qc["num_days_journal_submit"]>0]
		# setup more readable site IDs for later legends
		subject_qc["siteID"] = [x[-2:] for x in subject_qc["site"].tolist()]
		# make sure the features that focus only on journals successfully uploaded to TranscribeMe are clearly marked
		col_updates = {"first_submit_day":"first_accepted_submit_day","last_submit_day":"last_accepted_submit_day","time_since_last_submit":"time_since_last_accepted_submit"}
		subject_qc.rename(columns=col_updates,inplace=True)
		# also need to get months_since_consent column for use in next step
		# (just going to +1 for each 30 days passed, rough estimate)
		subject_qc["months_since_consent"] = [math.floor(x/30.0) if not np.isnan(x) else np.nan for x in subject_qc["study_day_at_compute_time"].tolist()]

		# now setup for histogram function input
		subject_feats = [["num_days_ema_submit","num_days_journal_submit","num_audio_files_uploaded","sum_minutes_audio_uploaded"],
						 ["first_accepted_submit_day","last_accepted_submit_day","time_since_last_accepted_submit","months_since_consent"]]
		count_bins = [0,8,15,22,29,43,58,86,114,174,235,300,365]
		min_bins = [x * 1.5 for x in count_bins]
		min_bins.append(2.0*365)
		min_bins.append(3.0*365)
		first_day_bins = list(range(1,15))
		first_day_bins.extend([21,28,35,70,105])
		subject_bins = [[count_bins,count_bins,count_bins,min_bins],
						[first_day_bins,count_bins,first_day_bins,list(range(13))]]
		subject_title = cur_server + " Distributions of Participation Metrics Over Diary-Submitting Subject IDs by Site" + "\n" + "(Data as of " + cur_date + ")"

		stacked_histograms_with_labels(subject_qc,subject_feats,"allSubjectsServerWide_participationStatDistributions_coloredBySite.pdf",
									   dividing_feature_name="siteID",bin_specs=subject_bins,
									   counted_object_name="Subjects (with journal participation)",plot_suptitle=subject_title)
	else:
		print("WARNING: no subject level stats CSV found - could signal issue if this was called from pipeline")
		print("Visualizations related to per subject stats will therefore be skipped here")
		proceed=False

	if not proceed:
		print("WARNING: either one (or both) of diary QC and subject stat CSVs were missing, or QC CSV had no transcript data at all - generated distribution PDFs that were possible, but now exiting")
		return

	# not doing anything special for the couple of scatter plot PDFs, so not going to bother with helper function like for histograms

	# basic PDF of scatter plots with just a handful of total pages and going server-wide:
	# 2 pages (4 total scatters) of plots with subject ID as dots
	# then 1 page (2 total scatters) of plots with diary as dots
	scatter_pdf = PdfPages("serverWide_journalEngagementScatterPlots.pdf")
	# start with first page total count vs total duration per subject with months since consent hue
	# (second figure below is just zoomed-in version to see better those with less excellent participation rates)
	fig, axs = plt.subplots(figsize=(15,20), nrows=1, ncols=2)
	sns.scatterplot(x="num_audio_files_uploaded",y="sum_minutes_audio_uploaded",hue="months_since_consent",palette="coolwarm",hue_norm=(0,12),data=subject_qc,ax=axs[0])
	sns.scatterplot(x="num_audio_files_uploaded",y="sum_minutes_audio_uploaded",hue="months_since_consent",palette="coolwarm",hue_norm=(0,12),data=subject_qc,ax=axs[1])
	axs[1].set_xlim(0,32)
	axs[1].set_ylim(0,64)
	for ax in axs:
		ax.legend()
		ax.set_xlabel("Number of Successful Audio Journal Submissions (as of " + cur_date + ")")
		ax.set_ylabel("Total Minutes of Successful Audio Journal Submissions (as of " + cur_date + ")")
	axs[0].set_title("Full Subject ID Participation Scatter (months since consent hue)")
	axs[1].set_title("Zoomed-In Subject ID Participation Scatter (months since consent hue)")
	fig.suptitle(cur_server + " Journal Count vs Total Duration by Subject, Colored with Enrollment Time")
	fig.tight_layout()
	scatter_pdf.savefig()
	plt.savefig("serverWide_subjectsCountDurationScatter.jpg") # for this one also saving as jpg for email attachment
	# now second page, will relate to day numbers for subject ID
	# first and then last diary accepted day each versus gap since last diary, month since consent hue 
	fig, axs = plt.subplots(figsize=(15,20), nrows=1, ncols=2)
	sns.scatterplot(x="first_accepted_submit_day",y="time_since_last_accepted_submit",hue="months_since_consent",palette="coolwarm",hue_norm=(0,12),data=subject_qc,ax=axs[0])
	sns.scatterplot(x="last_accepted_submit_day",y="time_since_last_accepted_submit",hue="months_since_consent",palette="coolwarm",hue_norm=(0,12),data=subject_qc,ax=axs[1])
	for ax in axs:
		ax.legend()
		ax.set_title("Subject ID Timeline Stats Scatter (months since consent hue)")
		ax.set_ylabel("Days Since Last Successful Audio Journal Submission (as of " + cur_date + ")")
	axs[0].set_xlabel("Study Day of First Successful Audio Journal Submission")
	axs[1].set_xlabel("Study Day of Last Successful Audio Journal Submission (as of " + cur_date + ")")
	fig.suptitle(cur_server + " Submission Timepoint Info vs Latest Participation Gap (size in days) by Subject, Colored with Enrollment Time" + "\n" + "(top x = first study day with diary, bottom x = most recent study day with diary)")
	fig.tight_layout()
	scatter_pdf.savefig()
	# finally will do third page with some diary level dots for engagement info
	# relative study day versus word count with weekday hue and then relative study day versus submit_hour_int with duration hue
	fig, axs = plt.subplots(figsize=(15,20), nrows=1, ncols=2)
	sns.scatterplot(x="day",y="word_count",hue="weekday",data=combined_qc,ax=axs[0])
	sns.scatterplot(x="day",y="submit_hour_int",hue="length_minutes",palette="coolwarm",hue_norm=(0,4),data=combined_qc,ax=axs[1])
	for ax in axs:
		ax.legend()
		ax.set_xlabel("Study Day of Submission")
	axs[0].set_title("Diary Transcript Word Counts Over Time (weekday hue)")
	axs[0].set_ylabel("Transcript Total Word Count")
	axs[1].set_title("Diary Submission Hour Over Time (recording duration hue)")
	axs[1].set_ylabel("Submission Hour (timezone and social adjusted, floored integer)")
	fig.suptitle(cur_server + " Submission Study Day vs Enagagement Metrics by Journal" + "\n" + "(data as of " + cur_date + ")")
	fig.tight_layout()
	scatter_pdf.savefig()
	# now done!
	scatter_pdf.close()

	# now a separate PDF with chosen minimal scatter and one page per relevant site
	cur_site_list = list(set(combined_qc["siteID"].tolist()))
	cur_site_list.sort()
	scatter_sites_pdf = PdfPages("perSiteBreakdown_journalEngagementScatterPlots.pdf")
	# loop through sites for pages -- main scatter is day versus length_minutes 
	# on each page top scatter will be the site-specific one and bottom scatter will contain all for reference
	# hue for site-specific will be by subject, overall by site
	for st in cur_site_list:
		cur_qc = combined_qc[combined_qc["siteID"]==st].dropna(subset=["day","length_minutes","subject"],how="any")
		if cur_qc.empty:
			print("WARNING: site " + st + " has entries in combined QC CSV but no actual valid QC rows")
			continue		
		fig, axs = plt.subplots(figsize=(15,20), nrows=1, ncols=2)
		sns.scatterplot(x="day",y="length_minutes",hue="subject",data=cur_qc,ax=axs[0])
		sns.scatterplot(x="day",y="length_minutes",hue="siteID",data=combined_qc,ax=axs[1])
		for ax in axs:
			ax.legend()
			ax.set_xlabel("Study Day of Submission")
			ax.set_ylabel("Recording Duration (minutes)")
		axs[0].set_title("[Site " + st + "] Submission Study Day vs Diary Durations, Colored by Subject ID")
		axs[1].set_title("Server-wide Submission Study Day vs Diary Durations, Colored by Site (for reference)")
		fig.suptitle(cur_server + st + "  Successful Diary Submission Durations Over Time" + "\n" + "(server-wide at bottom for reference, data as of " + cur_date + ")")
		fig.tight_layout()
		scatter_sites_pdf.savefig()
	# now done!
	scatter_sites_pdf.close()

	# finally will make PDF for time course plots
	# for timecourses do also use helper below to get individual figure objects
	# (as well as a script-specific helper to abstract away some site-specific DF manipulation)
	times_pdf = PdfPages("serverWide_journalParticipationTimecourses.pdf")
	# first page of PDF will be server-wide though (but consider only accepted diaries, and for subjects only those with at least 1)
	# top panel of timecourse dotplot will be subject count available per day and diaries available per day
	# bottom panel dotplot is rolling mean with rolling stdev error bars of the fraction per day diary/subj (week win)
	cur_dots_df,cur_lines_df,cur_errors_df = get_timecourse_dfs_helper(subject_qc,combined_qc)
	fig, axs = stacked_line_plots([cur_dots_df,cur_lines_df],error_bars=[None,cur_errors_df])
	axs[0].set_ylabel("Count")
	axs[1].set_ylabel("Fraction (rolling mean with stdev error)")
	axs[1].set_xlabel("Study Day")
	fig.suptitle(cur_server + "Server-wide Participation Timecourse as of " + cur_date + "\n" + "(for each study day, considering subjects at or past that point with >=1 diary submit, and then counts of accepted diaries on that day)")
	fig.tight_layout
	times_pdf.savefig()
	plt.savefig("serverWide_participationTimecourse.jpg") # for this one also saving as jpg for email attachment
	# now repeat for individual sites, can use same site list as above scatter
	for st in cur_site_list:
		cur_subj_qc = subject_qc[subject_qc["siteID"]==st].dropna(subset=["day","subject"],how="any")		
		cur_comb_qc = combined_qc[combined_qc["siteID"]==st].dropna(subset=["day","subject"],how="any")
		if cur_subj_qc.empty or cur_comb_qc.empty:
			print("WARNING: site " + st + " has entries in combined QC CSV but no actual valid QC rows and/or subject-level stats")
			continue
		cur_dots_df,cur_lines_df,cur_errors_df = get_timecourse_dfs_helper(cur_subj_qc,cur_comb_qc)
		fig, axs = stacked_line_plots([cur_dots_df,cur_lines_df],error_bars=[None,cur_errors_df])
		axs[0].set_ylabel("Count")
		axs[1].set_ylabel("Fraction (rolling mean with stdev error)")
		axs[1].set_xlabel("Study Day (" + st + " only)" )
		fig.suptitle(cur_server + st + "Site-specific Participation Timecourse as of " + cur_date + "\n" + "(for each study day, considering subjects at or past that point with >=1 diary submit, and then counts of accepted diaries on that day)")
		fig.tight_layout
		times_pdf.savefig()
	# now done!
	times_pdf.close()

	return


##### VISUALIZATION HELPERS:

# makes PDF with 4 horizontal distribution histograms per page from features in input_df
# features for a given page corresond to features in one of the lists contained in features_nested_list
# PDF saved to pdf_savepath
# if dividing_feature_name given (generally will be), this feature is used to divide the histogram up
# in that case distributions will be stacked in alphabetical order of the options for dividing_feature_name in input_df
# each category in dividing_feature_name will have a unique color (or if > 10 unique in combo with hatching, up to 30 options)
# bin_specs is an option argument that can be a list of lists in order to specify the number of bins for each histogram
#  -- or it can be a list of lists of lists to specify the exact bin delimiters for each histogram
# note that the page-level lists can contain a mix of int and list to use both these options on different histograms
# (but if bin_specs provided at all should have top level and second level shape matching that of features_nested_list)
# counted_object_name and plot_suptitle are then just optional arguments to allow further text labeling on plot as desired
# (counted_object_name is prepended to x-axis labels and plot_suptitle put at top of each page)
# note each figure panel itself will be named according to the features_nested_list and legend according to dividing_features_name
# those names should match input_df columns, but of course columns could be renamed before input into this function for prettier plots
# finally there is an optional (hacky) argument where a tuple of (page index, filename) can be passed --
#  -- to allow one of the page figures to also be saved as jpeg (in service of summary emails)
def stacked_histograms_with_labels(input_df,features_nested_list,pdf_savepath,dividing_feature_name=None,
								   bin_specs=None,counted_object_name=None,plot_suptitle=None,jpeg_save_tuple=None):
	# check that feature names are all fine and can find things in input_df okay
	try:
		for feat_list in features_nested_list:
			for feat in feat_list: 
				if feat not in input_df.columns:
					print("ERROR: some specified features not actually found in input_df - exiting")
					return
	except:
		print("ERROR: formatting issue with either input_df or features_nested_list - exiting")
		return

	# get categories for dividing up where applicable
	if dividing_feature_name is not None:
		try:
			available_categories = list(set(input_df[dividing_feature_name].tolist()))
		except:
			print("ERROR: problem with given dividing_feature_name, please revisit input arguments - exiting")
			return
		if len(available_categories) == 0:
			print("ERROR: problem with given dividing_feature_name, please revisit input arguments - exiting")
			return
		if len(available_categories) > 30:
			print("WARNING: too many possible categories for specified divider " + dividing_feature_name + " - plot would be messy, exiting")
			return
		available_categories.sort()

		# add hatches to avoid color repeat confusion
		# for ampscz specifically:
		# max number of sites for a given server will be below 30, colors repeat after 10
		# so need at most 2 hatches, presently only need 1
		# site assignments will change from week to week as new sites get added, because the sort is alphabetical
		if len(available_categories) > 20:
			hatch_list = ['' for x in range(10)]
			hatch_list.extend(['...' for x in range(10)])
			hatch_list.extend(['xxx' for x in range(len(available_categories)-20)])
		elif len(available_categories) > 10:
			hatch_list = ['' for x in range(10)]
			hatch_list.extend(['...' for x in range(len(available_categories)-10)])
		else:
			hatch_list = ['' for x in range(len(available_categories))]

	# setup pdf
	try:
		combined_pdf = PdfPages(pdf_savepath)
	except:
		# will have a bunch of error catching througout that shouldn't apply when this function is used in context of pipeline
		print("ERROR: problem with input savepath for PDF, exiting")
		return

	# now loop through pages in PDF and make histogram figures for each according to given settings
	for j in range(len(features_nested_list)):
		key_features = features_nested_list[j]
		if len(key_features) != 4:
			print("WARNING: specified histograms for a PDF page were not exactly 4, will still generate but may be missing expected histograms or have a weirdly-formatted page (with less than 4)")
		if bin_specs is not None:
			n_bins = bin_specs[j]
			if len(n_bins) != 4 and len(n_bins) != len(key_features):
				print("WARNING: bin_specs were given but don't match expected number of histograms, will use auto-generated bins for this page")
				n_bins = None
		else:
			n_bins = None
		
		fig, axs = plt.subplots(figsize=(15,15), nrows=2, ncols=2)
		# loop through hist panels on page
		for i in range(min(len(key_features),4)):
			if dividing_feature_name is not None:
				comb_list = [input_df[input_df[dividing_feature_name]==x][key_features[i]].tolist() for x in available_categories]
			else:
				sing_list = input_df[key_features[i]].tolist()
			if n_bins is not None:
				cur_bins = n_bins[i]

			if i < 2:
				cur_ax = axs[0][i]
			else:
				cur_ax = axs[1][i-2]

			# plot current histogram given settings
			if dividing_feature_name is not None:
				if n_bins is not None:
					cur_n_output, cur_bins_output, cur_patches = cur_ax.hist(comb_list, cur_bins, histtype="bar", stacked=True, orientation="horizontal", label=available_categories, edgecolor = "black")
				else:
					cur_n_output, cur_bins_output, cur_patches = cur_ax.hist(comb_list, histtype="bar", stacked=True, orientation="horizontal", label=available_categories, edgecolor = "black")
				for p in range(len(cur_patches)):
					patch = cur_patches[p]
					hatch = hatch_list[p]
					for cur_bar in patch:
						cur_bar.set(hatch = hatch)
				cur_ax.legend()
			else:
				if n_bins is not None:
					cur_ax.hist(sing_list, cur_bins, histtype="bar", orientation="horizontal")
				else:
					cur_ax.hist(sing_list, histtype="bar", orientation="horizontal")
			
			# add more labeling to panel
			cur_ax.set_title(key_features[i])
			if counted_object_name is not None:
				x_prefix = counted_object_name + " Count"
			else:
				x_prefix = "Count"
			if n_bins is not None:
				try:
					low_lim = cur_bins[0]
					high_lim = cur_bins[-1]
					outer_count = str(input_df[(input_df[key_features[i]] < low_lim) | (input_df[key_features[i]] > high_lim)].shape[0])
					xname = x_prefix + " (" + outer_count + " outside of range)"
					yname = "Value (using predefined bins - upper edge exclusive)"
					cur_ax.set_yticks(cur_bins)
				except:
					xname = x_prefix
					yname = "Value (using " + str(cur_bins) + " autogenerated bins)"
			else:
				xname = x_prefix
				yname = "Value (using entirely autogenerated bins)"
			cur_ax.set_xlabel(xname)
			cur_ax.set_ylabel(yname)
			max_x = cur_ax.get_xlim()[-1]
			if max_x > 100:
				x_ticks_list = list(range(0,100,5))
				x_ticks_list.extend(list(range(100,int(max_x)+1,15)))
				cur_ax.set_xticks(x_ticks_list)
				for tick in cur_ax.get_xticklabels():
					tick.set_rotation(45)
			else:
				cur_ax.set_xticks(range(0,int(max_x)+1,5))
			cur_ax.grid(color="#d3d3d3")
			cur_ax.set_axisbelow(True)

		# now a few page-level labeling/formatting things
		if plot_suptitle is not None:
			fig.suptitle(plot_suptitle)
		fig.tight_layout()
		# save to ongoing PDF
		combined_pdf.savefig()

		if jpeg_save_tuple is not None:
			try:
				if jpeg_save_tuple[0] == j:
					plt.savefig(jpeg_save_tuple[1])
			except:
				print("WARNING: attempted to provide jpeg_save_tuple but something wrong with argument formatting, ignoring that part")

	# when done with all pages finalize the PDF
	combined_pdf.close()
	return

# similar to above function, but now each page uses a different input dataframe from input_df_list
# and instead makes the same histograms expecting a single list of 4
# because same features each time bins should also now be a less-nested list
# also add df_name_list argument so have a name for each input DF that can go in labeling (required)
# note same dividing feature name needs to be viable for all included input_dfs 
# -- and they all need to include all specified features too
def stacked_histograms_different_dfs(input_df_list,df_name_list,key_features,pdf_savepath,
									 dividing_feature_name=None,n_bins=None,
									 counted_object_name=None,plot_suptitle=None):
	# check that feature names are all fine and can find things in each input_df of input_df_list okay
	try:
		for feat,input_df in zip(key_features,input_df_list):
			if feat not in input_df.columns:
				print("ERROR: some specified features not actually found in some of the input_dfs - exiting")
				return
	except:
		print("ERROR: formatting issue with either input_df_list or key_features - exiting")
		return

	if len(input_df_list) != len(df_name_list):
		# obviously also the df_names need to be string, function will crash otherwise
		print("ERROR: issue with input_df_list naming map df_name_list - either missing names or missing dfs, so exiting")
		return

	# get categories for dividing up where applicable
	if dividing_feature_name is not None:
		nested_categories_list = []
		nested_hatch_list = []
		for input_df in input_df_list:
			try:
				available_categories = list(set(input_df[dividing_feature_name].tolist()))
			except:
				print("ERROR: problem with given dividing_feature_name, please revisit input arguments - exiting")
				return
			if len(available_categories) == 0:
				print("ERROR: problem with given dividing_feature_name, please revisit input arguments - exiting")
				return
			if len(available_categories) > 30:
				print("WARNING: too many possible categories for specified divider " + dividing_feature_name + " - plot would be messy, exiting")
				return
			available_categories.sort()

			# add hatches to avoid color repeat confusion
			# for ampscz specifically:
			# max number of sites for a given server will be below 30, colors repeat after 10
			# so need at most 2 hatches, presently only need 1
			# site assignments will change from week to week as new sites get added, because the sort is alphabetical
			if len(available_categories) > 20:
				hatch_list = ['' for x in range(10)]
				hatch_list.extend(['...' for x in range(10)])
				hatch_list.extend(['xxx' for x in range(len(available_categories)-20)])
			elif len(available_categories) > 10:
				hatch_list = ['' for x in range(10)]
				hatch_list.extend(['...' for x in range(len(available_categories)-10)])
			else:
				hatch_list = ['' for x in range(len(available_categories))]

			nested_categories_list.append(available_categories)
			nested_hatch_list.append(hatch_list)

	# setup pdf
	try:
		combined_pdf = PdfPages(pdf_savepath)
	except:
		# will have a bunch of error catching througout that shouldn't apply when this function is used in context of pipeline
		print("ERROR: problem with input savepath for PDF, exiting")
		return

	# settings warnings that won't cause exit now
	if len(key_features) != 4:
		print("WARNING: specified histograms for PDF pages is not exactly 4, will still generate but may be missing expected histograms or have a weirdly-formatted page (with less than 4)")
	if n_bins is not None:
		if len(n_bins) != 4 and len(n_bins) != len(key_features):
			print("WARNING: n_bins were given but don't match expected number of histograms, will use auto-generated bins for this pdf")
			n_bins = None

	# now loop through pages in PDF and make histogram figures for each according to given settings
	for j in range(len(input_df_list)):
		input_df = input_df_list[j]
		cur_df_name = df_name_list[j]
		if dividing_feature_name is not None:
			available_categories = nested_categories_list[j]
			hatch_list = nested_hatch_list[j]
		
		fig, axs = plt.subplots(figsize=(15,15), nrows=2, ncols=2)
		
		# loop through hist panels on page
		for i in range(min(len(key_features),4)):
			if dividing_feature_name is not None:
				comb_list = [input_df[input_df[dividing_feature_name]==x][key_features[i]].tolist() for x in available_categories]
			else:
				sing_list = input_df[key_features[i]].tolist()
			if n_bins is not None:
				cur_bins = n_bins[i]

			if i < 2:
				cur_ax = axs[0][i]
			else:
				cur_ax = axs[1][i-2]

			# plot current histogram given settings
			if dividing_feature_name is not None:
				if n_bins is not None:
					cur_n_output, cur_bins_output, cur_patches = cur_ax.hist(comb_list, cur_bins, histtype="bar", stacked=True, orientation="horizontal", label=available_categories, edgecolor = "black")
				else:
					cur_n_output, cur_bins_output, cur_patches = cur_ax.hist(comb_list, histtype="bar", stacked=True, orientation="horizontal", label=available_categories, edgecolor = "black")
				for p in range(len(cur_patches)):
					patch = cur_patches[p]
					hatch = hatch_list[p]
					for cur_bar in patch:
						cur_bar.set(hatch = hatch)
				cur_ax.legend()
			else:
				if n_bins is not None:
					cur_ax.hist(sing_list, cur_bins, histtype="bar", orientation="horizontal")
				else:
					cur_ax.hist(sing_list, histtype="bar", orientation="horizontal")
			
			# add more labeling to panel
			cur_ax.set_title(key_features[i] + " (" + cur_df_name + ")")
			if counted_object_name is not None:
				x_prefix = counted_object_name + " Count"
			else:
				x_prefix = "Count"
			if n_bins is not None:
				try:
					low_lim = cur_bins[0]
					high_lim = cur_bins[-1]
					outer_count = str(input_df[(input_df[key_features[i]] < low_lim) | (input_df[key_features[i]] > high_lim)].shape[0])
					xname = x_prefix + " (" + outer_count + " outside of range)"
					yname = "Value (using predefined bins - upper edge exclusive)"
					cur_ax.set_yticks(cur_bins)
				except:
					xname = x_prefix
					yname = "Value (using " + str(cur_bins) + " autogenerated bins)"
			else:
				xname = x_prefix
				yname = "Value (using entirely autogenerated bins)"
			cur_ax.set_xlabel(xname)
			cur_ax.set_ylabel(yname)
			max_x = cur_ax.get_xlim()[-1]
			if max_x > 100:
				x_ticks_list = list(range(0,100,5))
				x_ticks_list.extend(list(range(100,int(max_x)+1,15)))
				cur_ax.set_xticks(x_ticks_list)
				for tick in cur_ax.get_xticklabels():
					tick.set_rotation(45)
			else:
				cur_ax.set_xticks(range(0,int(max_x)+1,5))
			cur_ax.grid(color="#d3d3d3")
			cur_ax.set_axisbelow(True)

		# now a few page-level labeling/formatting things
		if plot_suptitle is not None:
			fig.suptitle(plot_suptitle + "\n" + "Plots on this page specific to " + cur_df_name)
		else:
			fig.suptitle("Plots on this page specific to " + cur_df_name)
		fig.tight_layout()
		# save to ongoing PDF
		combined_pdf.savefig()

	# when done with all pages finalize the PDF
	combined_pdf.close()
	return

# helper function for making stacked time course plots on one figure
# this will return the fig,axs object so that PDF construction is not part of this helper
# (instead above main func loops through and calls helper for each page)
# here the input_df_list are assumed to all share same x axis
# each input_df in input_df_list will be plotted in separate panel from top to bottom
# each column in input_df will be one of the things plotted in the current panel, with legend matching col names
# if "x_nums" is a column in the input_df that will be used as x vals for that panel, otherwise will just use range on shape of df
# if error_bars is provided it should have same length as input_df_list - each entry will be matched to input_df_list index
# for given error_bars index, can either provide None to make a dot plot or provide a dataframe with columns matching input_df
# if error_bar df is provided for given panel, then it will be a line plot and a column should match each plotted name - used to shade
def stacked_line_plots(input_df_list,error_bars=None):
	# first do various argument checks
	if error_bars is not None:
		try:
			if len(input_df_list) != len(error_bars):
				print("WARNING: error_bars argument provided but does not match length of input data frames list, exiting")
				return
			for eb,df in zip(error_bars,input_df_list):
				if eb is not None:
					for col in df:
						if col != "x_nums" and col not in eb.columns:
							print("WARNING: error bar df corresponding to one of the input dfs does not have appropriate columns for shading, exiting")
							return
		except:
			print("WARNING: issue with formatting of one or more input arguments, exiting")
			return
	try:
		if len(input_df_list) == 0 or len(input_df_list) > 10:
			print("WARNING: too many (or no) panels attempted in plot, exiting")
			return
		for df in input_df_list:
			if len(df.columns) == 0 or len(df.columns) > 6:
				print("WARNING: too many (or no) time courses attempted in single panel, exiting")
				return
	except:
		print("WARNING: issue with formatting of one or more input arguments, exiting")
		return

	# now can proceed
	if len(input_df_list) <= 3:
		# if fewer total panels, make them slightly longer vertically
		y_mult = 7
	else:
		y_mult = 5
	# all panels will share x axis object
	fig, axs = plt.subplots(len(input_df_list),sharex=True,figsize=(15,y_mult*len(input_df_list)))
	# loop through panels
	for j in range(len(input_df_list)):
		input_df = input_df_list[j]
		if error_bars is not None:
			error_bar = error_bars[j]
		else:
			error_bar = None
		cur_ax = axs[j]

		# same x indices for all in panel, if not in DF just define as range	
		if "x_nums" in input_df.columns:
			x_nums = input_df["x_nums"].tolist()
			input_df.drop(columns=["x_nums"],inplace=True)
		else:
			x_nums = range(input_df.shape[0]) 

		legends = input_df.columns 
		cur_l = legends[0]
		# loop through time courses to overlay
		for i in range(len(legends)):
			if error_bar is not None:
				cur_ax.plot(x_nums, input_df[legends[i]].tolist(), marker="", linestyle='solid', label=legends[i])
				cur_ax.fill_between(x_nums, np.array(input_df[legends[i]].tolist())-np.array(error_bar[legends[i]].tolist()), np.array(input_df[legends[i]].tolist())+np.array(error_bar[legends[i]].tolist()),alpha=0.3)
			else:
				cur_ax.plot(x_nums, input_df[legends[i]].tolist(), marker=".", linestyle='', label=legends[i])

		# make sure to display legend for panel
		cur_ax.legend()

	# final figure cleanup
	for ax in axs: # Hide x labels and tick labels for all but bottom plot.
		ax.label_outer()
	fig.tight_layout()

	return fig,axs


# this final helper is really just specific to current pipeline's main function
# but will allow easier set up of timecourses via df manipulation on site by site level
# by taking input dfs that I can then call each time filtered by site above
# specifically, want available subject count for each possibly study day to date, as well as actual count of diaries by day
def get_timecourse_dfs_helper(subject_qc_inp,combined_qc_inp):
	days_avail = subject_qc_inp["study_day_at_compute_time"].tolist()
	days_avail.sort(reverse=True)
	counting_list = [0 for x in range(int(days_avail[0]))]
	for d in days_avail:
		counting_list[:int(d)] = [x + 1 for x in counting_list[:int(d)]]
	if counting_list[0] != subject_qc_inp.shape[0]:
		print("WARNING: count of subjects available per day does not line up with total diary-submitting subject count, please review manually for accuracy")
	possible_subjects_per_day = pd.DataFrame()
	possible_subjects_per_day["day"] = list(range(1,int(days_avail[0])+1))
	possible_subjects_per_day["subject_count"] = counting_list
	total_diaries_per_study_day = combined_qc_inp[["day","subject"]].dropna(how="any").groupby("day",as_index=False).count().reset_index(drop=True).rename(columns={"subject":"diaries_count"})
	diary_accounting = possible_subjects_per_day.merge(total_diaries_per_study_day,on="day",how="outer").fillna(0).reset_index(drop=True)
	diary_accounting_check = diary_accounting[diary_accounting["subject_count"]>0]
	if not diary_accounting_check.equals(diary_accounting):
		print("WARNING: count of subjects available per day does not line up with days available in the merged QC CSV, please review manually for accuracy")
	diary_accounting_check["current_response_rate"] = diary_accounting_check["diaries_count"]/diary_accounting_check.subject_count.astype(float)
	# also get rolling mean and stdev part
	diary_accounting_check["rolling_mean_week_window_daily_response_fraction_eligible_subjects"] = diary_accounting_check["current_response_rate"].rolling(7).mean()
	diary_accounting_check["weekly_response_stdev_rolling"] = diary_accounting_check["current_response_rate"].rolling(7).std()
	# now prep for return, separating stuff out according to the timecourses helper above
	diary_accounting_check.rename(columns={"day":"x_nums","subject_count":"total_subjects_reached_day","diaries_count":"total_accepted_diaries_submitted_on_day"},inplace=True)
	diary_accounting_dots = diary_accounting_check[["x_nums","total_subjects_reached_day","total_accepted_diaries_submitted_on_day"]]
	diary_accounting_line = diary_accounting_check[["x_nums","rolling_mean_week_window_daily_response_fraction_eligible_subjects"]]
	diary_accounting_error = diary_accounting_check[["x_nums","weekly_response_stdev_rolling"]].rename(columns={"weekly_response_stdev_rolling":"rolling_mean_week_window_daily_response_fraction_eligible_subjects"})
	return diary_accounting_dots,diary_accounting_line,diary_accounting_error


if __name__ == '__main__':
	# Map command line arguments to function arguments.
	diary_monitoring_visuals(sys.argv[1])

