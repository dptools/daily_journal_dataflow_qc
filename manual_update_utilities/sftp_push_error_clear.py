# As major issues log never deletes issues, this helper script can clear out failed SFTP upload warnings
# Only does so of course if the file is no longer being held in the pending TranscribeMe upload area
# Just a way to keep error logs clean of outdated issues, can periodically be run manually if upload errors appear
# Also serves as a way for monitoring staff to quickly check if batch of failed uploads has been resolved or not

# Note this is currently written for ProNET, as only ever observed failed uploads there 
# (and only on one day to date, not expecting this to be a regular issue)
# But to run for Prescient would just need to update hard coded root path here 
# (and hard coded software path in the bash wrapper that sets up dependencies to call this as root account)

import glob
import os
import pandas as pd

files = glob.glob("/mnt/ProNET/Lochness/PHOENIX/PROTECTED/*/processed/*/phone/audio_journals/*_audioJournalMajorIssuesLog.csv")

for errors in files:
	edf = pd.read_csv(errors)
	e_subset = edf[edf["error_message"]=="TranscribeMe SFTP upload failed"]

	if e_subset.empty:
		continue

	names_to_check = e_subset["filename"].tolist()
	# each error file is maintained per subject
	site_id = names_to_check[0].split("_")[0]
	subject_id = names_to_check[0].split("_")[1]
	folder_path_to_check = "/mnt/ProNET/Lochness/PHOENIX/PROTECTED/" + site_id + "/processed/" + subject_id + "/phone/audio_journals/audio_to_send"
	# find audios that are not pending anymore 
	names_no_longer_pending = [x for x in names_to_check if not os.path.exists(os.path.join(folder_path_to_check,x))]

	if len(names_no_longer_pending) == 0:
		continue

	# filter out resolved SFTP upload issues
	e_keep = edf[~((edf["error_message"]=="TranscribeMe SFTP upload failed") & (edf["filename"].isin(names_no_longer_pending)))]

	if e_keep.empty:
		# remove the CSV instead if no other errors remain
		os.remove(errors)
	else:
		e_keep.to_csv(errors,index=False)
