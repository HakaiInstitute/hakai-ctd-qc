#!/bin/bash

source $HOME/miniconda3/etc/profile.d/conda.sh

conda activate hakai-profile-qaqc
python hakai_profile_qc
conda deactivate