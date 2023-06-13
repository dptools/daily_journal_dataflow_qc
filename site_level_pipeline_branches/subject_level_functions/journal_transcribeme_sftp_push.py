#!/usr/bin/env python

import pysftp
import os
import shutil
import sys
import time

# make sure if paramiko throws an error it will get mentioned in log files
import logging
logging.basicConfig()

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
