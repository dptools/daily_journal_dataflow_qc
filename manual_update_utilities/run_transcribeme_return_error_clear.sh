#!/bin/bash

# setup is specific to pronet here, can easily adapt to Prescient if needed
. /opt/software/miniconda3/etc/profile.d/conda.sh
conda activate audio_process
sudo env "PATH=$PATH" python transcribeme_return_error_clear.py