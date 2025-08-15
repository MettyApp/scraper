#!/bin/sh

set -u
LAYER_DIR=".aws-sam/build/SharedLayer/"

rm -rf ${LAYER_DIR}/python/boto*
find ${LAYER_DIR}/python/ -path "**/__pycache__/*" -delete
rm -f .aws-sam/build/*/items.json
rm -f .aws-sam/build/*/*.ipynb
rm -rf .aws-sam/build/*/data/