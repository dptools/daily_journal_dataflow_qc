# As major issues log never deletes issues, this helper script can clear out waiting >2 week errors
# Only does so of course if the file is no longer being held in the pending TranscribeMe return area
# Just a way to keep error logs clean of outdated issues, can periodically be run manually if upload errors appear
# However there are cases where TranscribeMe actually never returns a file, perhaps because of bad quality -
#   - in which case we want to keep those!

# Note this is currently written for ProNET
# To run for Prescient would just need to update hard coded root path here 
# (and hard coded software path in the bash wrapper that sets up dependencies to call this as root account)

import glob
import os
import pandas as pd

files = glob.glob("/mnt/ProNET/Lochness/PHOENIX/PROTECTED/*/processed/*/phone/audio_journals/*_audioJournalMajorIssuesLog.csv")

for errors in files:
	edf = pd.read_csv(errors)
	e_subset = edf[edf["error_message"]=="File has been pending return from TranscribeMe for over 2 weeks now"]

	if e_subset.empty:
		continue

	names_to_check = e_subset["filename"].tolist()
	# each error file is maintained per subject
	site_id = names_to_check[0].split("_")[0]
	subject_id = names_to_check[0].split("_")[1]
	folder_path_to_check = "/mnt/ProNET/Lochness/PHOENIX/PROTECTED/" + site_id + "/processed/" + subject_id + "/phone/audio_journals/pending_audio"
	# find audios that are not pending anymore 
	names_no_longer_pending_int = [x for x in names_to_check if not os.path.exists(os.path.join(folder_path_to_check,x))]
	# also confirm the transcript exists
	folder_path_to_check2 = "/mnt/ProNET/Lochness/PHOENIX/PROTECTED/" + site_id + "/processed/" + subject_id + "/phone/audio_journals/transcripts"
	names_no_longer_pending = [x for x in names_no_longer_pending_int if os.path.exists(os.path.join(folder_path_to_check2,x.split(".wav")[0] + ".txt"))]

	if len(names_no_longer_pending) == 0:
		continue

	# filter out resolved SFTP upload issues
	e_keep = edf[~((edf["error_message"]=="File has been pending return from TranscribeMe for over 2 weeks now") & (edf["filename"].isin(names_no_longer_pending)))]

	if e_keep.empty:
		# remove the CSV instead if no other errors remain
		os.remove(errors)
	else:
		e_keep.to_csv(errors,index=False)
