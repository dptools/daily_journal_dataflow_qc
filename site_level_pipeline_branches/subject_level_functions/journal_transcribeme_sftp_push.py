#!/usr/bin/env python

import pysftp
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import json

# make sure if paramiko throws an error it will get mentioned in log files
import logging
logging.basicConfig()

def map_transcription_language(language_code: int) -> str:
	"""
	Maps the language code to the language name.

	Can be found in Data_Dictionary.

	Parameters:
		language_code: The language code.

	Returns:
		The language name.
	"""

	# 1, Cantonese | 2, Danish | 3, Dutch | 4, English | 5, French
	# 6, German | 7, Italian | 8, Korean | 9, Mandarin (TBC) | 10, Spanish
	language_code_map: Dict[int, str] = {
		1: "Cantonese",
		2: "Danish",
		3: "Dutch",
		4: "English",
		5: "French",
		6: "German",
		7: "Italian",
		8: "Korean",
		9: "Mandarin",
		10: "Spanish"
	}

	return language_code_map[language_code]


def get_transcription_language_csv(sociodemographics_file_csv_file: Path) -> Optional[int]:
	"""
	Reads the sociodemographics survey and attempts to get the language variable.
	Prescient uses RPMS CSVs for surveys.

	Returns None if the language is not found.

	Parameters:
		sociodemographics_file_csv_file: The path to the sociodemographics survey CSV file.

	Returns:
		The language code if found, otherwise None.
	"""
	language_variable = "chrdemo_assess_lang"
	sociodemographics_df = pd.read_csv(sociodemographics_file_csv_file)

	try:
		language_variable_value = sociodemographics_df[language_variable].iloc[0]
	except KeyError:
		return None
	
	# Check if language is missing
	if pd.isna(language_variable_value):
		return None
	
	return int(language_variable_value)


def get_transcription_language_json(json_file: Path) -> Optional[int]:
	"""
	Reads the REDCap JSON file and attempts to get the language variable.
	ProNET uses REDCap for surveys.

	Returns None if the language is not found.

	Parameters:
		json_file: The path to the sociodemographics survey JSON file.

	Returns:
		The language code if found, otherwise None.
	"""
	language_variable = "chrdemo_assess_lang"
	with open(json_file, "r") as f:
		json_data = json.load(f)
	
	for event_data in json_data:
		if language_variable in event_data:
			value = event_data[language_variable]
			if value != "":
				return int(value)
	
	return None


def get_transcription_language(subject_id: str, study: str, data_root: Path) -> Optional[str]:
	"""
	Attempts to get the transcription language for the participant from the sociodemographics survey.

	Returns None if the language is not found.

	Parameters:
		subject_id: The participant's ID.
		study: The study name.
		data_root: The root directory of the data.

	Returns:
		The transcription language if found, otherwise None.
	"""

	data_root = Path(data_root)
	surveys_root = data_root / "PROTECTED" / study / "raw" / subject_id / "surveys"

	# Get sociodemographics survey
	sociodemographics_files = surveys_root.glob("*sociodemographics*.csv")

	# Check if sociodemographics survey exists
	if sociodemographics_files:
		# Read sociodemographics survey
		sociodemographics_file = list(sociodemographics_files)[0]
		language_code = get_transcription_language_csv(sociodemographics_file)
	else:
		json_files = surveys_root.glob("*.Pronet.json")
		if json_files:
			json_file = list(json_files)[0]
			language_code = get_transcription_language_json(json_file)
		else:
			return None

		return map_transcription_language(int(language_code))

def transcript_push(data_root, site, subject, username, password, transcription_language):
	# initialize list to keep track of which audio files will need to be moved at end of patient run
	push_list = []

	# hardcode the basic properties for the transcription service, as these shouldn't change
	destination_directory = "audio"
	host = "sftp.transcribeme.com"

	try:
		directory = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "audio_to_send")
		os.chdir(directory) 
	except:
		# if this is called by pipeline the folder will exist, but add for potential outside callers
		print("WARNING: audio_to_send folder not found for diaries matching current input arguments, please revisit")
		return

	try:
		sociodemographics_language = get_transcription_language(subject_id=subject, study=site, data_root=data_root)
		if sociodemographics_language is not None:
			transcription_language = sociodemographics_language
	except Exception as e:
		print(f"Error getting transcription language from sociodemographics survey: {e}")
		print(f"Using default transcription language: {transcription_language}")
		pass

	# now actually attempt to establish SFTP connection for all push for this subject
	# give it a few tries before falling back
	count = 0
	connect_pending = True
	while connect_pending and count < 10:
		try:
			cnopts = pysftp.CnOpts()
			cnopts.hostkeys = None # ignore hostkey
			with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
				# loop through the WAV files in to_send, push them to TranscribeMe
				for filename in os.listdir("."):
					if not filename.endswith(".wav"): # should always only be WAV if called from pipeline, but double check
						continue 
					# source filepath is just filename, setup desired destination path
					filename_with_lang = filename.split("submission")[0] + transcription_language + "_submission" + filename.split("submission")[1]
					dest_path = os.path.join(destination_directory, filename_with_lang)
					if not sftp.exists(dest_path): # double check in case gets part way through loop on one connection for some reason
						sftp.put(filename, dest_path)
					push_list.append(filename) # if get to this point push was successful, add to list
				# if get to this point connection was fully successful, can stop loop after doing pushes
				connect_pending = False	
		except:
			# any files that had problem with push will still be in to_send after this script runs, so can just pass here
			# in future may try to catch specific types of errors to identify when the problem is incorrect login info versus something else 
			# (in the past have run out of storage space in the input folder, not sure if that specific issue could be caught by the python errors though)
			
			# here will increment counter and possibly try again
			count = count + 1 
			# but do short delay first
			time.sleep(5) # waiting 5 seconds before reattempting connection
		
	# now move all the successfully uploaded files from to_send to pending_audio
	for filename in push_list:
		new_path = "../pending_audio/" + filename
		# move the file lcoally
		shutil.move(filename, new_path)

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	transcript_push(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
