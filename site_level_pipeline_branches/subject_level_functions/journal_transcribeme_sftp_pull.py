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
	
	for filename in cur_pending:
		# setup expected source filepath and desired destination filepath
		rootname = filename.split(".wav")[0]
		transname = rootname + ".txt"
		# need to add language marker in for lookup on transcribeme side
		transname_lookup = rootname.split("submission")[0] + transcription_language + "_submission" + rootname.split("submission")[1] + ".txt"
		src_path = os.path.join(source_directory, transname_lookup)
		local_path = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "transcripts", transname)
		# now actually attempt the pull. will hit the except in all cases where the transcript isn't available yet, but don't want that to crash rest of code obviously
		# (note it hasn't been a problem previously to establish new connection each time - 
		#  but in future may want to refactor so only need to establish connection once per patient instead of per file)
		try:
			cnopts = pysftp.CnOpts()
			cnopts.hostkeys = None # ignore hostkey
			with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
				sftp.get(src_path, local_path)
			successful_transcripts.append(transname_lookup) # if we reach this line it means transcript has been successfully pulled onto PHOENIX
			print("New transcript " + transname + " successfully pulled")
			# this audio is no longer pending then, file should be moved from pending to completed audio
			pending_rename = os.path.join(data_root, "PROTECTED", site, "processed", subject, "phone", "audio_journals", "completed_audio", filename)
			os.rename(filename,pending_rename)
		except:
			# nothing to do here, most likely need to just keep waiting on this transcript
			print(filename + " not returned yet, will remain pending")
			try:
				# seems this sometimes creates an empty file at the data aggregation server transcript path, so just remove that for clarity
				os.remove(local_path)
			except:
				pass 

	# log some very basic info about success of script
	print("Final for subject " + subject + ": " + str(len(successful_transcripts)) + " total new transcripts pulled")

	# now do cleanup on trancribeme server for those transcripts successfully pulled
	input_directory = "audio"
	for transcript in successful_transcripts:
		# will remove audio from TranscribeMe's server, as they do not need it anymore
		match_name = transcript.split(".txt")[0] 
		match_audio = match_name + ".wav"
		remove_path = os.path.join(input_directory, match_audio)
		# will also move the pulled transcript into an archive subfolder of output on their server (organized by site)
		cur_path = os.path.join(source_directory, transcript)
		archive_name = site + "_journals_archive"
		archive_path = os.path.join(source_directory, archive_name, transcript)
		archive_folder = os.path.join(source_directory, archive_name)
		# actually connect to server to do the updates for current transcript
		try:
			cnopts = pysftp.CnOpts()
			cnopts.hostkeys = None # ignore hostkey
			with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
				if not sftp.exists(archive_folder):
					sftp.mkdir(archive_folder)
				sftp.rename(cur_path, archive_path)
				sftp.remove(remove_path) 
		except:
			# expect failures here to be rare (if generic connection problems successful_transcripts would likely be empty)
			print("WARNING: possible error cleaning up TranscribeMe server, please check on file " + match_name)

if __name__ == '__main__':
	# Map command line arguments to function arguments.
	transcript_pull(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    