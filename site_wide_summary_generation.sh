#!/bin/bash

# this script will actually manage the server-wide summary and emails generation

# takes required arguments needed for the settings of given server
# (specified via main script that actually runs cron, here that can be found under ampscz_diaries_launch)
server_name="$1"
data_root="$2"
summary_email_list="$3" # note this one can be embedded HTML via sendmail
summary_from="$4" # for sendmail able to easily appear as from a specific address too, so that is also setting
detailed_email_list="$5"
# for mail that goes out with attachments using mailx, need to know a or A so give that argument 
# (if doesn't match either default to a)
mail_attach="$6" 

# start by getting the absolute path to the directory this script is in, which will be top level of the repo
# this way script will work even if the repo is downloaded to a new location, rather than relying on hard coded paths to where I put it 
full_path=$(realpath $0)
repo_root=$(dirname $full_path)
# all of the helpers to be called here can be found in the server-level helper functions folder
func_root="$repo_root"/server_level_summary_functions

# make directory for logs if needed
if [[ ! -d ${repo_root}/logs ]]; then
	mkdir "$repo_root"/logs
fi
# keep logs here in a high level folder
if [[ ! -d ${repo_root}/logs/TOTAL ]]; then
	mkdir "$repo_root"/logs/TOTAL
fi
# put attachments for emailing and log files from this run into subfolder, organized by date
cur_date=$(date +%Y%m%d)
if [[ ! -d ${repo_root}/logs/TOTAL/${cur_date} ]]; then
	mkdir "$repo_root"/logs/TOTAL/"$cur_date"
fi
# if running manually, print to console and log files simultaneously
exec >  >(tee -ia "$repo_root"/logs/TOTAL/"$cur_date"/overall_summary_logging.txt)
exec 2> >(tee -ia "$repo_root"/logs/TOTAL/"$cur_date"/overall_summary_logging.txt >&2)

# let user know script is starting and give basic settings info for reference
echo ""
echo "Beginning server-level daily journal summary processing pipeline run for:"
echo "$server_name"
echo "with data root:"
echo "$data_root"
echo ""
echo "Higher level summary email (sent as embedded HTML via sendmail) will go to:"
echo "$summary_email_list"
echo "with sender address appearing as ${summary_from}"
echo ""
echo "More detailed combined logging email (sent with attachments via mailx) will go to:"
echo "$detailed_email_list"
if [[ $mail_attach == "A" ]]; then
	echo "using attachment arg -A here"
else
	echo "using attachment arg -a here (defaults to a if A not explicitly given)"
fi
echo ""

# add current time for runtime tracking purposes
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# now go through and generate status summaries of interest
# will use outputs from the 2 main branches of pipeline + per subject summary branch
# (all assumed to have run on site level for each site already before calling this)

echo "Concatenating main logging CSVs (major issues, rejected audios list, and accepted audios complete QC + metadata stats)"
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# run server-wide concatenation as described, saving into the specified logs folder
# (all will be attached to detailed logging CSV at end)
python "$func_root"/concat_key_subject_diary_csvs.py "$data_root" "$repo_root"/logs/TOTAL/"$cur_date"

echo ""
echo "Moving on to create CSVs with higher level summaries on subject and site levels"
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# run server-wide summary operations, again saving into today's total logs folder
# (all will be attached to detailed logging email at end and/or used in the summary work of the next step)
python "$func_root"/generate_server_wide_diary_summary_csvs.py "$data_root" "$repo_root"/logs/TOTAL/"$cur_date"

echo ""
echo "Creating final visualizations from generated CSVs"
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# everything generated by this step will be part of one or the other email
# uses outputs from prior two steps and again saves in same logging folder specified for today
# - which is also where all input CSVs will now be found!
# given setup of expected summary CSVs (see function), this script would be easy to run outside of pipeline
python "$func_root"/create_shareable_visualizations.py "$repo_root"/logs/TOTAL/"$cur_date"

echo ""
echo "Putting together final email body for HTML summary email"
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# this final python function uses info to create the HTML that will be sent by sendmail next
# first argument input path is output folder from prior scripts, as that is where all info is sourced by this point
# second argument output path is now the HTML file that will be created with all that info
# script embeds site-level summary CSV generated by prior step as an HTML table, and also add a few most key visualizations
# HTML body adds context for included table and viz interpretation as well (hard-coded in this python function)
python "$func_root"/finalize_server_summary_email_html.py "$repo_root"/logs/TOTAL/"$cur_date" "$repo_root"/logs/TOTAL/"$cur_date"/summary_body.html "$repo_root"/setup/cdn.jupyter.org_notebook_5.1.0_style_style.min_edited.css
# also giving path to css style sheet copied into repo to try to embed table style if possible

echo ""
echo "Ready to send emails - first sendmail, and then email with all relevant attachments for detailed monitoring"
now=$(date +"%T")
echo "Current time: ${now}"
echo ""

# go to folder where relevant contents kept first
cd "$repo_root"/logs/TOTAL/"$cur_date" 

# handle sendmail command
# going to just rely on embedded images to exist, otherwise part might be missing within email
# will at least check for HTML first though
if [[ ! -e summary_body.html ]]; then
	echo "WARNING: no summary email body HTML generated, some issue with pipeline? Exiting"
	exit
fi
# note order of images to be embedded and their corresponding codes is directly related to what is specified in the html generation python script
# so any changes to this email need to be made in both places!

sendmail_subject="${server_name} Weekly Journals Data Summary - ${cur_date}" 
/usr/sbin/sendmail -t <<EOT
To: ${summary_email_list}
From: ${summary_from}
Subject: ${sendmail_subject}
MIME-Version: 1.0
Content-Type: multipart/related;boundary="XYZ"

--XYZ
Content-Type: text/html; charset=ISO-8859-15
Content-Transfer-Encoding: 7bit

$(cat summary_body.html)

--XYZ
Content-Type: image/jpeg;name="serverWide_participationTimecourse.jpg"
Content-Transfer-Encoding: base64
Content-ID: <part1.06090408.01060107>
Content-Disposition: inline; filename="serverWide_participationTimecourse.jpg"

$(base64 serverWide_participationTimecourse.jpg)

--XYZ
Content-Type: image/jpeg;name="serverWide_keyQCHistogram.jpg"
Content-Transfer-Encoding: base64
Content-ID: <part2.06090408.01060107>
Content-Disposition: inline; filename="serverWide_keyQCHistogram.jpg"

$(base64 serverWide_keyQCHistogram.jpg)

--XYZ
Content-Type: image/jpeg;name="serverWide_subjectsCountDurationScatter.jpg"
Content-Transfer-Encoding: base64
Content-ID: <part3.06090408.01060107>
Content-Disposition: inline; filename="serverWide_subjectsCountDurationScatter.jpg"

$(base64 serverWide_subjectsCountDurationScatter.jpg)

--XYZ--
EOT

# now ready to prep attachments email
if [[ ! -e allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv ]]; then
	echo "ERROR: main merged QC CSV does not exist, something went wrong with this run of code"
	echo "Exiting before even attempting email with detailed attachments, please manually troubleshoot"
	exit
fi

# make sure using valid mail attachment flag for server (given settings in cron)
if [[ $mail_attach != "A" ]]; then
	mail_attach="a"
fi

# (should) know this exists, otherwise major issues across whole server - so start attachment chain with it
attachments_flag_list=("-${mail_attach}" "allSubjectsServerWide_successfulJournals_allQC_withMetadata.csv")
# then check for existence of other possible expected outputs to add them where possible (should generally exist)
if [[ -e allSubjectsServerWide_audioQCRejectedJournals_dataLog.csv ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="allSubjectsServerWide_audioQCRejectedJournals_dataLog.csv"
else
	echo "Note there is no QC-rejected audio list on this server at present"
fi
if [[ -e allSubjectsServerWide_audioJournalMajorIssuesLog.csv ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="allSubjectsServerWide_audioJournalMajorIssuesLog.csv"
else
	echo "Note there is no major issues log for daily journals on this server at present"
fi
if [[ -e serverWide_subjectsLevel_journalSubmissionSummary.csv ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="serverWide_subjectsLevel_journalSubmissionSummary.csv"
else
	echo "WARNING: no subject-level summary CSV has been generated, may signal issue with pipeline and will be missing from this email's attachments"
fi
if [[ -e serverWide_sitesLevel_journalSubmissionSummary.csv ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="serverWide_sitesLevel_journalSubmissionSummary.csv"
else
	echo "WARNING: no site-level summary CSV has been generated, may signal issue with pipeline and will be missing from this email's attachments"
fi

# similarly have visualization PDFs to check for
if [[ -e allSubjectsServerWide_participationStatDistributions_coloredBySite.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="allSubjectsServerWide_participationStatDistributions_coloredBySite.pdf"
else
	echo "WARNING: no diary participation histograms over subject IDs have been generated, may signal issue with pipeline and will be missing from this email's attachments"
fi
if [[ -e allDiariesServerWide_QCDistributions_coloredBySite.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="allDiariesServerWide_QCDistributions_coloredBySite.pdf"
else
	echo "WARNING: no diary QC histograms have been generated, may signal issue with pipeline and will be missing from this email's attachments"
fi
if [[ -e diariesBySiteServerWide_selectQCDistributions_coloredBySubject.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="diariesBySiteServerWide_selectQCDistributions_coloredBySubject.pdf"
else
	# this won't be generated unless transcript data available, whereas above should really always exist even at very early stages
	echo "Note there is no per site diaries QC histogram PDF on the server at present"
fi
if [[ -e allDiariesServerWide_disfluenciesDistributions.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="allDiariesServerWide_disfluenciesDistributions.pdf"
else
	echo "Note there is no disfluencies distribution PDF for diaries on this server at present"
fi
if [[ -e serverWide_journalEngagementScatterPlots.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="serverWide_journalEngagementScatterPlots.pdf"
else
	echo "Note there is no server-wide scatterplots PDF for diaries on this server at present"
fi
if [[ -e perSiteBreakdown_journalEngagementScatterPlots.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="perSiteBreakdown_journalEngagementScatterPlots.pdf"
else
	echo "Note there is no site-by-site scatterplots PDF for diaries on this server at present"
fi
if [[ -e serverWide_journalParticipationTimecourses.pdf ]]; then
	attachments_flag_list[${#attachments_flag_list[@]}]="-${mail_attach}" 
	attachments_flag_list[${#attachments_flag_list[@]}]="serverWide_journalParticipationTimecourses.pdf"
else
	echo "Note there is no participation timecourses PDF for diaries on this server at present"
fi

mailx_subject="${server_name} Weekly Journals Monitoring Status Details - ${cur_date}"
echo "" > dummy.txt # can't have attachments and email body actually print at once with the -A version of mailx, so just forget email body
mailx -s "$mailx_subject" "${attachments_flag_list[@]}" "$detailed_email_list" < dummy.txt || echo "WARNING: problem sending ${mailx_subject} to addresses ${detailed_email_list}" 
rm dummy.txt

echo ""
echo "Final server-wide summary script completed!"
now=$(date +"%T")
echo "Current time: ${now}"
