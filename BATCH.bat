@echo off

set baseYm=%1
set gnr=%2

python featureinput.py %baseYm% %gnr%>> ./log/feature_input.log 2>&1

python scoring.py %baseYm% %gnr%>> ./log/model_scoring.log 2>&1

pause