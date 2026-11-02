#!/bin/bash
# Runs during every Cloudera AI model build. The replica does not see packages the job
# pip-installed into ~/.local. Pin the training versions; wheels only, so a missing wheel
# fails the build instead of compiling.
set -euo pipefail
pip3 install --disable-pip-version-check --only-binary=:all: -r requirements-model.txt
