#!/usr/bin/env python

import pysftp
import os
import sys

# make sure if paramiko throws an error it will get mentioned in log files
import logging
logging.basicConfig()

def transcript_pull(data_root, site, subject, username, password, transcription_language):
	# track transcripts that got properly pulled this time, for use in cleaning up server later
	successful_transcripts = []

	# hardcode the basic properties for the transcription service, password is only sftp-related input for now
	source_directory = "output" 
	# also specify input directory for cleanup of finished audio
	input_directory = "audio"
	# need to ensure transcribeme is actually putting .txt files into the top level output directory as they are done consistently
	# (hasn't really been a problem for interviews for this project though thankfully)
	host = "sftp.transcribeme.com"

	try:
		directory = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "pending_audio")
		os.chdir(directory) 
	except:
		# no files for this patient then - though should never encounter this if called from overall pipeline
		print("WARNING: no actual pending audio to check for subject " + subject)
		return

	# loop through files in pending, trying to pull corresponding txt files that are in the transcribeme output folder
	cur_pending = os.listdir(".")
	if len(cur_pending) == 0:
		# no files for this patient - though again should never happen when called through pipeline wrapper
		print("WARNING: no actual pending audio to check for subject " + subject)
		return

	# now actually attempt to establish SFTP connection for all (available) pull for this subject
	# give it a few tries before falling back
	count = 0
	connect_pending = True
	while connect_pending and count < 10:
		try:
			cnopts = pysftp.CnOpts()
			cnopts.hostkeys = None # ignore hostkey
			with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
				# loop through WAV files marked as currently pending 
				for filename in cur_pending:
					# setup expected source filepath and desired destination filepath
					rootname = filename.split(".wav")[0]
					transname = rootname + ".txt"
					# need to add language marker in for lookup on transcribeme side
					transname_lookup = rootname.split("submission")[0] + transcription_language + "_submission" + rootname.split("submission")[1] + ".txt"
					src_path = os.path.join(source_directory, transname_lookup)
					local_path = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", transname)
					if sftp.exists(src_path): # check if the transcript has been transcribed yet and if so try to get it
						sftp.get(src_path, local_path) # pull it if available
						# append pending WAV name to list for later local audio renaming/other tracking
						successful_transcripts.append(filename) # if we reach this line it means transcript has been successfully pulled onto PHOENIX
						print("New transcript " + transname + " successfully pulled")

						# additionally, do cleanup on transcribemes server for this file
						# will remove audio, as they do not need it anymore
						match_name = transname_lookup.split(".txt")[0] 
						match_audio = match_name + ".wav"
						remove_path = os.path.join(input_directory, match_audio)
						# will also move the pulled transcript into an archive subfolder of output on their server (organized by site)
						cur_path = os.path.join(source_directory, transname_lookup)
						archive_name = site + "_journals_archive"
						archive_path = os.path.join(source_directory, archive_name, transname_lookup)
						archive_folder = os.path.join(source_directory, archive_name)
						if not sftp.exists(archive_folder):
							sftp.mkdir(archive_folder)
						sftp.rename(cur_path, archive_path)
						sftp.remove(remove_path) 
				# if get to this point connection was fully successful, can stop loop after doing pushes
				connect_pending = False	
		except:
			# here will increment counter and possibly try again
			count = count + 1 
			if count == 10:
				print("WARNING: connections to TranscribeMe's SFTP server failed when attempting to pull transcripts for subject " + subject)
			else:
				# but do short delay first
				time.sleep(5) # waiting 5 seconds before reattempting connection

	# log some very basic info about success of script
	print("Final for subject " + subject + ": " + str(len(successful_transcripts)) + " total new transcripts pulled (" + str(len(cur_pending)-len(successful_transcripts)) + " remain pending)")

	# now do cleanup of audios locally
	# successful audios no longer pending, file should be moved to completed audio
	for transcript in successful_transcripts:
		pending_rename = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "completed_audio", transcript)
		os.rename(transcript,pending_rename)

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	transcript_pull(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    