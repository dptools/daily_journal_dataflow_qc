#!/bin/bash

# setup is specific to pronet here as I've only encountered this on pronet (rare issue anyway)
. /opt/software/miniconda3/etc/profile.d/conda.sh
conda activate audio_process
sudo env "PATH=$PATH" python sftp_push_error_clear.py