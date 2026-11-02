#!/bin/bash
# Runs during every Cloudera AI build (model and application). The model image does not see
# packages a job pip-installed into ~/.local. Pins first, then the rest of requirements.txt
# (streamlit, impyla) which the application needs. Wheels only, so a missing wheel fails
# the build instead of compiling.
set -euo pipefail
pip3 install --disable-pip-version-check --only-binary=:all: -r requirements-model.txt
pip3 install --disable-pip-version-check --only-binary=:all: -r requirements.txt
