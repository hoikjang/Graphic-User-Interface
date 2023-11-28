########################################################################################################################
### 패키지 IMPORT
########################################################################################################################

import warnings 
warnings.filterwarnings('ignore')

##############################################################################################################
### 데이터 적재 및 IMPORT 패키지
##############################################################################################################

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO

KST = pytz.timezone('Asia/Seoul')

########################################################################################################################
### DB 연결
########################################################################################################################
# 일반연결
conn   = sqlite3.connect("pine.db", isolation_level=None)
cur    = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2  = engine.connect()

######################################################################################################################## 
###### 리스트 정보
########################################################################################################################
gnr                  = os.environ['gnr']
phase                = os.environ['phase']
baseYM               = os.environ['baseYM']



# list_YYYYMM          = ['201105','201106','201107','201108','201109','201110','201111','201112',
#                         '201201','201202','201203','201204','201205','201206','201207','201208','201209']

list_YYYYMM          = ['201001','201002','201003','201004','201005','201006','201007','201008','201009',
                        '201010','201011','201012','201101','201102','201103','201104','201105','201106',
                        '201107','201108','201109','201110','201111','201112','201201','201202','201203',
                        '201204','201205','201206','201207','201208','201209','201210']

list_YYYYMM          = list_YYYYMM[:(list_YYYYMM.index(f'{baseYM}')+1)]
list_avoid           = ['기준년월','회원번호','나이','나이범주','성별','결혼여부']

# list_gnr           = ['애니','드라마','역사','코미디','액션','범죄','스릴러','다큐','모험','판타지'
#                       '가족','로맨스','음악','호러','전쟁','서부','미스테리','단편','뮤지컬','스포츠','공상과학','전기','뉴스']

list_gnr             = ['범죄']

list_YYYYMMTST       = list_YYYYMM[(list_YYYYMM.index(f'{baseYM}')-2):]

dst_tm        = datetime.now(KST)


print(f'''[LOG] 샘플링 코드 실행, 기준년월 = {baseYM}, 장르:{gnr}''')

######################################################################################################################## 
###### 데이터 샘플링
########################################################################################################################
dst_tm        = datetime.now(KST)
print(f'''[LOG] 샘플링 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')


input_str='TRGTMART_SUB'
smpl_col_name                  = ['기준년월','회원번호','장르명' ,'샘플구분', 'Y']
if phase =='학습'
    df_trgt = pd.DataFrame(columns = smpl_col_name)
    for idx2, YYYYMMTRGT in enumerate(list_YYYYMM):
        ####################################################################################################################
        #### 쿼리 1 타겟마트 장르-기준년월별 이벤트 데이터 추출
        ####################################################################################################################
        sql_trgt_mart= f'''
            SELECT
                DISTINCT
                 A.기준년월
                ,A.회원번호    
                ,'{gnr}'                                                                                                 AS 장르명
                ,B.성별
                ,B.나이범주
                ,A.Y_{gnr}                                                                                               AS Y
            FROM TRGTMART                                                                                                AS A
            INNER JOIN FTRMART                                                                                           AS B
                ON A.기준년월 = B.기준년월 and A.회원번호 = B.회원번호
            WHERE  A.기준년월= {YYYYMMTRGT}
        '''
        df_trgt_mart                  = pd.read_sql(sql_trgt_mart,conn)
        df_trgt_sub                   = df_trgt_mart.loc[df_trgt_mart['Y'] ==1]
        trgt_len                      = len(df_trgt_sub)

        print('타겟행수: ', trgt_len)

        df_ntrgt                      = df_trgt_mart.loc[df_trgt_mart['Y'] !=1]
        ntrgt_len                     = len(df_ntrgt)
        print('NE타겟행수: ', ntrgt_len)

        if   YYYYMMTRGT     in (list_YYYYMMTST):
            ################################################################################################################
            #### 타겟마트 테스트데이터셋 장르-기준년월별 타겟추출 및 1:n 샘플링
            ################################################################################################################
            df_ntrgt['샘플구분']      = 0
            df_trgt_sub['샘플구분']   = 0
            df_trgt                   = pd.concat([df_trgt,pd.concat([df_trgt_sub[smpl_col_name],df_ntrgt[smpl_col_name]])])

            print('테스트셋 타겟마트: ', df_trgt)
        elif YYYYMMTRGT not in (list_YYYYMMTST):    
            ################################################################################################################
            #### 타겟마트 학습데이터셋 장르-기준년월별 타겟추출 및 1:n 샘플링
            ################################################################################################################
            for idx3, multiplier in enumerate([1,2,3]):
                df_trgt_sub['샘플구분']  = multiplier
                df_ntrgt_sub             = df_ntrgt.groupby(['성별','나이범주'],group_keys = False)\
                                          .apply(lambda x: x.sample(frac=min(1,trgt_len*multiplier/ntrgt_len), replace=False))
                df_ntrgt_sub['샘플구분'] = multiplier
                df_sub                   = pd.concat([df_trgt_sub[smpl_col_name],df_ntrgt_sub[smpl_col_name]])

                print(f'타겟마트_{YYYYMMTRGT}_샘플구분_{multiplier} 크기: ', len(df_sub), 
                      f'예상 크기: ', trgt_len*(multiplier+1))

                df_trgt                  = pd.concat([df_trgt,df_sub])
        else:
            continue

    cur.execute(f"""delete from {input_str}""")
    df_trgt.to_sql(
          name         = f'{input_str}'
        , con          = engine
        , if_exists    = 'append'
        , index        = False
        , method       = "multi"
        , chunksize    = 10000
    )
    타겟행수       = pd.read_sql(f'''select count(*) from {input_str}''',conn)
    print('타겟행수: ', 타겟행수, '예상타겟행수: ', trgt_len*9*33)
    list_col_name  = pd.read_sql('''PRAGMA table_info(FTRMART)''',conn).name

dend_tm = datetime.now(KST)
del_tm  = dend_tm-dst_tm
print(f'''[LOG] 샘플링 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, 행수 = {len(df_trgt)}, 컬럼 수 = {len(df_trgt.columns)}''')