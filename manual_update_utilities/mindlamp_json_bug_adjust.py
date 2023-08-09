# Lochness does not modify already pulled MindLAMP data, so it is generally safe to assume that JSON will never be edited
# As such, the pipeline looks at each JSON only once and will not modify its accounting
# In the special case of the MindLAMP bug that caused missing diary data from 4/13 to 7/31, this assumption was manually overrided
# Now that data from this period are recovered, we need to delete the prior pipeline records of the corresponding JSONs
# This utility script adjusts the existing pipeline outputs to allow for reprocessing of the changed JSONs
# It does so by looking for MP3s that didn't have a match to a valid JSON, and deleting the record of the corresponding invalid JSON
# This will therefore need to be rerun periodically as backlogged MP3s now come in from Lochness
# Once all files are recovered, it should not be needed again
# The script also deletes all prior major issue events related directly to the issue to keep the log from being overwhelmed
# Note this is relevant only to ProNET, not Prescient

import glob
import pandas as pd
import os

files = glob.glob("/mnt/ProNET/Lochness/PHOENIX/PROTECTED/*/processed/*/phone/audio_journals/file_accounting_details/*_availablePhoneMP3sAccounting.csv")

for csv in files:
	df = pd.read_csv(csv)
	to_keep = df[df["json_record_existence_check"]==True]
	to_note = df[df["json_record_existence_check"]==False]["found_mp3_name"].tolist()
	json_names = [x.split("_sound")[0] + ".json" for x in to_note]
	to_keep.to_csv(csv,index=False)

	site = csv.split("/")[6]
	subject = csv.split("/")[8]
	json_csv = csv.split("file_accounting_details")[0] + "file_accounting_details/"  + site + "_" + subject + "_appActivitiesJSONAccounting.csv"

	df2 = pd.read_csv(json_csv)
	to_keep2 = df2[~df2["json_filename"].isin(json_names)]
	to_keep2.to_csv(json_csv,index=False)

files2 = glob.glob("/mnt/ProNET/Lochness/PHOENIX/PROTECTED/*/processed/*/phone/audio_journals/*_audioJournalMajorIssuesLog.csv")

for errors in files2:
	edf = pd.read_csv(errors)
	e_keep = edf[edf["error_message"]!="MP3 found without a matching JSON record"]
	if e_keep.empty:
		os.remove(errors)
	else:
		e_keep.to_csv(errors,index=False)
