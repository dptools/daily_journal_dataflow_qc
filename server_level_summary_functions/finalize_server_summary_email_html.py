#!/usr/bin/env python

import os
import pandas as pd
from datetime import date
import sys

# function set up to put together final HTML file with embedded data frame for site-level stats
# (and context for images to be embedded by wrapping pipeline sendmail command)
def diary_monitoring_html(source_folder,html_path):
	# check given folder exists in order to change directories into it
	try:
		os.chdir(source_folder)
	except:
		# if pipeline setup correctly should not hit this error
		print("ERROR: given source_folder invalid, please check function arguments") 
		return
	# make sure savepath for html exists too
	# (note should be an absolute path)
	if len(html_path.split("/"))>1 and not os.path.isdir('/'.join(html_path.split("/")[:-1])):
		# also should not be issue in actual pipeline
		print("ERROR: given html_path invalid, please check function arguments")
		return

	# load main summary DF
	try:
		df = pd.read_csv("serverWide_sitesLevel_journalSubmissionSummary.csv")
	except:
		print("WARNING: couldn't load main per sites summary CSV, exiting")
		return

	# clean up dataframe
	df["Site"] = [x[-2:] for x in df["site"].tolist()]
	rename_dict = {"num_subjects_found":"Subject IDs Found",
				   "num_subjects_raw_protected_phone_folder":"Subjects w/ Phone Data",
				   "num_subjects_any_ema":"Subjects Submitting EMAs (>=1)",
				   "num_subjects_any_journal":"Subjects Submitting Journals (>=1)",
				   "num_subjects_submit_within_last_two_week":"Subjects Recording a Journal in Last 2 Weeks",
				   "num_audio_files_uploaded":"Total Successful Audio Journal Submissions",
				   "sum_minutes_audio_uploaded":"Sum Minutes Uploaded to TranscribeMe"}
	df.rename(columns=rename_dict,inplace=True)
	# remove sites that presumably haven't even had pipeline launched for them yet
	df = df[df["num_subjects_any_active_app"]>0]
	df_col_list1 = ["Site","Total Successful Audio Journal Submissions","Sum Minutes Uploaded to TranscribeMe"]
	df_col_list2 = ["Site","Subject IDs Found","Subjects w/ Phone Data","Subjects Submitting EMAs (>=1)","Subjects Submitting Journals (>=1)","Subjects Recording a Journal in Last 2 Weeks"]

	# finalize dfs for use in html email
	cur_date = date.today().strftime("%m/%d/%Y")
	cur_server = df["site"].tolist()[0][:-2]
	df_header1 = cur_server + " Per-Site Audio Journal Cumulative Processed Submission Stats (as of " + cur_date + ")"
	df_html1 = df[df_col_list1].reset_index(drop=True).to_html()
	df_header2 = cur_server + " Per-Site Audio Journal Subject Count Participation Stats (as of " + cur_date + ")"
	df_html2 = df[df_col_list2].reset_index(drop=True).to_html()

	# start setting up HTML and embed tables with desired style
	start = "<html><head><meta http-equiv=\"content-type\" content=\"text/html; charset=ISO-8859-15\"><style>h3 {text-align: center;}</style></head><body class=\"rendered_html\"> <link rel=\"stylesheet\" href=\"https://cdn.jupyter.org/notebook/5.1.0/style/style.min.css\">" 
	df_render1 = "<h3>" + df_header1 + "</h3><br>" + df_html1 + "<br>" 
	df_render2 = "<h3>" + df_header2 + "</h3><br>" + df_html2 + "<br>" 

	# then setup to embed images - headers are hard-coded within this function, but really all that is specified here is a code that will be used in pipeline wrapper
	# this enables 1 image per image_render object here to be embedded, paths to images will be specified in sendmail in that other script in order
	image_header1 = "Relationship Between Subject Submission Counts and Durations (as well as time in study so far)"
	image_header2 = "Participation Rate Trends Over Time Enrolled in Study"
	image_header3 = "Server-wide Distribution of Key QC Features (for diaries with returned transcripts)"
	image_render1 = "<h3>" + image_header1 + "</h3> <br> <img src=\"cid:part1.06090408.01060107\" alt=\"" + image_header1 + "\">"
	image_render2 = "<h3>" + image_header2 + "</h3> <br> <img src=\"cid:part2.06090408.01060107\" alt=\"" + image_header2 + "\">"
	image_render3 = "<h3>" + image_header3 + "</h3> <br> <img src=\"cid:part3.06090408.01060107\" alt=\"" + image_header3 + "\">"

	# now actually write out full well-formatted HTML file using above components
	end="</body></html>"
	with open(html_path, 'w') as f:
		f.write(start)
		f.write(df_render1)
		f.write(df_render2)
		f.write("<br><br>") # add spacing between image sections
		f.write(image_render1)
		f.write("<br><br>")
		f.write(image_render2)
		f.write("<br><br>")
		f.write(image_render3)
		f.write(end)

	return

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	diary_monitoring_html(sys.argv[1], sys.argv[2])

