#!/bin/bash

. /opt/software/miniconda3/etc/profile.d/conda.sh
conda activate audio_process
sudo env "PATH=$PATH" python mindlamp_json_bug_adjust.py