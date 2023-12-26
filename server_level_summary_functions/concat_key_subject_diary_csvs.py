#!/usr/bin/env python

import os
import glob
import pandas as pd
import sys

# concatenate 3 main per journal tracking CSVs across all subjects on the server with relevant data:
# detailed info about successful audios, list of QC-rejected audios, and list of various specific errors
def diary_csv_concat(data_root, output_folder):
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

	# now find all paths across sites for relevant CSVs of each type
	success_csvs = glob.glob("*/processed/*/phone/audio_journals/*successfulJournals_allQC_withMetadata.csv")
	rejected_csvs = glob.glob("*/processed/*/phone/audio_journals/*audioQCRejectedJournals_dataLog.csv")
	error_csvs = glob.glob("*/processed/*/phone/audio_journals/*audioJournalMajorIssuesLog.csv")

	# all CSVs have site and subject columns already as well as consistent columns
	# - so it is safe to just straight concat for each

	# now do the concat, just checking to make sure there were some CSVs found first
	# success initially then will proceed in order with the other two
	if len(success_csvs) == 0:
		print("WARNING: no successful audio records found on server at this time! Nothing to concatenate")
	else:
		print("Now saving combined CSV with detailed successfully processed audio journals info across subjects and sites")
		success_dfs = [pd.read_csv(x) for x in success_csvs]
		success_concat = pd.concat(success_dfs).reset_index(drop=True)
		success_final = success_concat.sort_values(by="subject",kind="stable")
		# within subject already sorted by relative study day number
		save_name = "allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv"
		success_final.to_csv(os.path.join(output_folder,save_name),index=False)

	if len(rejected_csvs) == 0:
		print("As no rejected audio records found on server at this time, no rejected CSV to concatenate")
	else:
		print("Now saving combined CSV with info on QC-rejected diary audios across subjects and sites")
		rejected_dfs = [pd.read_csv(x) for x in rejected_csvs]
		rejected_concat = pd.concat(rejected_dfs).reset_index(drop=True)
		rejected_final = rejected_concat.sort_values(by="subject",kind="stable")
		# within subject already sorted by relative study day number
		save_name = "allSubjectsServerWide_audioQCRejectedJournals_dataLog.csv"
		rejected_final.to_csv(os.path.join(output_folder,save_name),index=False)

	if len(error_csvs) == 0:
		print("As no major issue warnings found on server at this time, no error CSV to concatenate")
	else:
		print("Now saving combined CSV with info on possible major issues detected in uploaded audio journals across subjects and sites")
		error_dfs = [pd.read_csv(x) for x in error_csvs]
		error_concat = pd.concat(error_dfs).reset_index(drop=True)
		error_concat.sort_values(by="site",inplace=True) # glob goes in arbitrary order so get sites grouped first
		error_final = error_concat.sort_values(by="date_detected",ascending=False,kind="stable")
		# for errors really want latest stuff at top, so did different sort here
		save_name = "allSubjectsServerWide_audioJournalMajorIssuesLog.csv"
		error_final.to_csv(os.path.join(output_folder,save_name),index=False)

	return

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	diary_csv_concat(sys.argv[1], sys.argv[2])

