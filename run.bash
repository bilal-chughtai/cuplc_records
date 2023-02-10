#!/bin/bash
cd /home/bc464/cuplc/records
git pull
poetry run python script.py >> log.txt