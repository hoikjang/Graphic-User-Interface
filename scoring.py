

########################################################################################################################
### 데이터 적재 및 IMPORT 패키지
########################################################################################################################

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine

########################################################################################################################
### 모델링용 패키지 IMPORT
########################################################################################################################
import numpy as np
import sklearn 
from sklearn.model_selection import train_test_split 
import sklearn.ensemble  as rf
import lightgbm as lgbm
import xgboost  as xgb
import re
import copy
import pytz
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO

KST = pytz.timezone('Asia/Seoul')


########################################################################################################################
### DB 연결
########################################################################################################################
# 일반연결
conn = sqlite3.connect("pine.db", isolation_level=None)
cur  = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2  = engine.connect()

########################################################################################################################
### 필요 객체 생성
########################################################################################################################

baseYM  = os.environ['baseYM']
gnr     = os.environ['gnr']
phase   = os.environ['phase']

if phase == '운영':

    # baseYM        = '201206'
    # gnr           = '범죄'

    dst_tm        = datetime.now(KST)
    baseYm_bfr_1y = datetime.strftime(datetime.strptime(baseYM, '%Y%m') - relativedelta(months = 11), '%Y%m')


    print(f'''[LOG] 스코어링 코드 실행, 기준년월 = {baseYM}, 장르:{gnr}''')


    ##############################################################################################################
    ### 모델 로드
    ##############################################################################################################
    dst_tm        = datetime.now(KST)
    print(f'''[LOG] 모델로드 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
    filename1     = f'model_{gnr}.sav'
    filename2     = f'{gnr}_colname'
    with open(filename1, 'rb') as file:
        loaded_model = pickle.load(file)

    with open(filename2, 'rb') as file:
        col_name     = pickle.load(file)

    dend_tm = datetime.now(KST)
    del_tm  = dend_tm-dst_tm
    print(f'''[LOG] 모델로드 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}''')

    ##############################################################################################################
    ### 예측 데이터 형성
    ##############################################################################################################
    dst_tm           = datetime.now(KST)
    print(f'''[LOG] 모델 예측용 Input_X 형성 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
    list_col_bsc     = ['기준년월','회원번호']
    pr_set           = []
    pr_set           = input_mart.drop(columns = list_col_bsc).reset_index().drop(columns='index')
    pr_set.columns   = col_name

    dend_tm = datetime.now(KST)
    del_tm  = dend_tm-dst_tm
    print(f'''[LOG] 모델 예측용 Input_X 형성 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, 행수:{len(pr_set)}, 컬럼 수 = {len(pr_set.columns)}''')

    ##############################################################################################################
    ### 모델 예측
    ##############################################################################################################
    dst_tm              = datetime.now(KST)
    print(f'''[LOG] 모델예측 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')

    df_Y_result         = pd.concat([input_mart[['기준년월','회원번호']], pd.Series([x[1] for x in loaded_model.predict_proba(pr_set)])], axis = 1)
    df_Y_result.columns = ['기준년월', '회원번호','스코어']

    dend_tm = datetime.now(KST)
    del_tm = dend_tm-dst_tm
    print(f'''[LOG] 모델예측 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, 결과 데이터 길이 = {len(df_Y_result)}, 결과 데이터 행수= {len(df_Y_result.columns)}''')

    ##############################################################################################################
    ### 결과 적재
    ##############################################################################################################

    dst_tm             = datetime.now(KST)
    print(f'''[LOG] 모델결과 적재시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')


    input_str          = 'SCRMART'

    cur.execute(f"""delete from {input_str} WHERE 기준년월 = {baseYM}""")
    df_Y_result.to_sql(
          name         = f'{input_str}'
        , con          = engine
        , if_exists    = 'append'
        , index        = False
        , method       = "multi"
        , chunksize    = 10000
    )

    dend_tm = datetime.now(KST)
    del_tm = dend_tm-dst_tm
    print(f'''[LOG] 모델결과 적재 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}''')
    print(f'''[LOG] 스코어링 코드 완료, 기준년월 = {baseYM}, 장르:{gnr}''')