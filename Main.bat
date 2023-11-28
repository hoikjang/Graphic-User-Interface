@echo off

set baseYM=%1
set gnr=%2
set gnr=%3

call sampling.bat %yyyymm% %baseYM%
call modeling.bat %yyyymm% 
call batch_3_prediction.bat %yyyymm%


python featureinput.py %baseYM% %gnr%>> ./log/feature_input.log 2>&1

python scoring.py %baseYM% %gnr%>> ./log/model_scoring.log 2>&1

pause