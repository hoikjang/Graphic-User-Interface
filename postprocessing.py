
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
conn = sqlite3.connect("pine.db", isolation_level=None)
cur  = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2  = engine.connect()



####################################################################################################################
###### 컬럼 속성 및 IV 계산
####################################################################################################################

gnr                  = os.environ['gnr']
phase                = os.environ['phase']
baseYM               = os.environ['baseYM']

dst_tm        = datetime.now(KST)
baseYm_bfr_1y = datetime.strftime(datetime.strptime(baseYM, '%Y%m') - relativedelta(months = 11), '%Y%m')


print(f'''[LOG] 분석마트 후처리 코드 실행, 기준년월 = {baseYM}, 장르:{gnr}''')
#################################################################################################################### 
###### 분석마트 분석
####################################################################################################################
list_col_cat               = []
list_anlysIV               = []
####################################################################################################################
###### 이벤트/Non이벤트별 데이터 구분
####################################################################################################################

filename1     = f'{gnr}_input_mart'
with open(filename1, 'rb') as file:
input_mart    = pickle.load(file)

df_anlys_mart                                     = input_mart
df_anlys_mart[f'전체기간{gnr}시청여부']           = df_anlys_mart.groupby('회원번호')[f'월별{gnr}시청여부'].transform('max')

df_anlys_mart_ev                                   = df_anlys_mart.loc[df_anlys_mart[f'전체기간{gnr}시청여부']==1]
df_anlys_mart_nev                                  = df_anlys_mart.loc[df_anlys_mart[f'전체기간{gnr}시청여부']!=1]
list_col_num                                       = df_anlys_mart.columns[df_anlys_mart2.dtypes!='object'].to_list()

for idx1, table in enumerate(list_table):
    df_col_list_1 = df_anlys_mart2.columns.to_list()
    for idx1, col_name in enumerate(df_col_list_1):
    ################################################################################################################
    ###### 연속형 컬럼 이벤트/Non이벤트별 최소, 25% Quant, 50% Quant, 75% Quant, 최대, 평균 및 각 구간별 개수 추출
    ################################################################################################################
        if col_name not in (list_col_num):
            col_type          = '연속형'
            col_min           = df_anlys_mart2[col_name].min()
            col_qnt1          = df_anlys_mart2[col_name].quantile(0.25)
            col_qnt2          = df_anlys_mart2[col_name].quantile(0.50)
            col_qnt3          = df_anlys_mart2[col_name].quantile(0.75)
            col_max           = df_anlys_mart2[col_name].max()
            col_avg           = df_anlys_mart2[col_name].mean()

            eevent            = '시청'
            ecol_val          = 'None'
            ecol_cnt          = 'None'
            ena_count         = df_anlys_mart_ev[col_name].isna().sum()
            etot_count        = df_anlys_mart_ev[col_name].count()+ena_count

            if df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name] <col_qnt1)].count() != 0:
                ecol_qnt1_cnt = df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name] <col_qnt1)].count()
            else:
                ecol_qnt1_cnt = 0.5

            if df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name] >col_qnt1)&(df_anlys_mart_ev[col_name]<col_qnt2)].count() != 0:
                ecol_qnt2_cnt = df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name] >col_qnt1)&(df_anlys_mart_ev[col_name]<col_qnt2)].count()
            else:
                ecol_qnt2_cnt = 0.5

            if df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name]>=col_qnt2)&(df_anlys_mart_ev[col_name]<col_qnt3)].count() != 0:
                ecol_qnt3_cnt = df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name]>=col_qnt2)&(df_anlys_mart_ev[col_name]<col_qnt3)].count()
            else:
                ecol_qnt3_cnt = 0.5

            if df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name]>=col_qnt3)&(df_anlys_mart_ev[col_name]<col_max)].count() != 0:
                ecol_qnt4_cnt = df_anlys_mart_ev[col_name].loc[(df_anlys_mart_ev[col_name]>=col_qnt3)&(df_anlys_mart_ev[col_name]<col_max)].count()
            else:
                ecol_qnt4_cnt = 0.5





            nevent            = '미시청'
            ncol_val          = 'None'
            ncol_cnt          = 'None'
            nna_count         = df_anlys_mart_nev[col_name].isna().sum()
            ntot_count        = df_anlys_mart_nev[col_name].count() + nna_count

            if df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name] <col_qnt1)].count() != 0:
                ncol_qnt1_cnt = df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name] <col_qnt1)].count()
            else:
                ncol_qnt1_cnt = 0.5

            if df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name] >col_qnt1)&(df_anlys_mart_nev[col_name]<col_qnt2)].count() != 0:
                ncol_qnt2_cnt = df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name] >col_qnt1)&(df_anlys_mart_nev[col_name]<col_qnt2)].count()
            else:
                ncol_qnt2_cnt = 0.5

            if df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name]>=col_qnt2)&(df_anlys_mart_nev[col_name]<col_qnt3)].count() != 0:
                ncol_qnt3_cnt = df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name]>=col_qnt2)&(df_anlys_mart_nev[col_name]<col_qnt3)].count()
            else:
                ncol_qnt3_cnt = 0.5

            if df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name]>=col_qnt3)&(df_anlys_mart_nev[col_name]<col_max)].count() != 0:
                ncol_qnt4_cnt = df_anlys_mart_nev[col_name].loc[(df_anlys_mart_nev[col_name]>=col_qnt3)&(df_anlys_mart_nev[col_name]<col_max)].count()
            else:
                ncol_qnt4_cnt = 0.5




            prtcnt_ev1     = ecol_qnt1_cnt/etot_count
            prtcnt_ev2     = ecol_qnt2_cnt/etot_count
            prtcnt_ev3     = ecol_qnt3_cnt/etot_count
            prtcnt_ev4     = ecol_qnt4_cnt/etot_count

            prtcnt_nv1     = ncol_qnt1_cnt/ntot_count
            prtcnt_nv2     = ncol_qnt2_cnt/ntot_count
            prtcnt_nv3     = ncol_qnt3_cnt/ntot_count
            prtcnt_nv4     = ncol_qnt4_cnt/ntot_count

            woe1           = np.log(prtcnt_nv1/prtcnt_ev1)
            woe2           = np.log(prtcnt_nv2/prtcnt_ev2)
            woe3           = np.log(prtcnt_nv3/prtcnt_ev3)
            woe4           = np.log(prtcnt_nv4/prtcnt_ev4)

            iv1            = (prtcnt_nv1-prtcnt_ev1)*woe1
            iv2            = (prtcnt_nv2-prtcnt_ev2)*woe2
            iv3            = (prtcnt_nv3-prtcnt_ev3)*woe3
            iv4            = (prtcnt_nv4-prtcnt_ev4)*woe4

            prtcnt_ev      = 0
            prtcnt_nv      = 0
            woe            = 0 
            iv             = iv1+iv2+iv3+iv4

            list_anlysIV.append([col_name,idx1+1,col_type,col_min,col_avg,col_max,col_qnt1,col_qnt2,col_qnt3
                                    ,eevent,etot_count,ena_count,ecol_val,ecol_cnt,ecol_qnt1_cnt,ecol_qnt2_cnt,ecol_qnt3_cnt,ecol_qnt4_cnt
                                    ,nevent,ntot_count,nna_count,ncol_val,ncol_cnt,ncol_qnt1_cnt,ncol_qnt2_cnt,ncol_qnt3_cnt,ncol_qnt4_cnt
                                    ,prtcnt_ev1,prtcnt_ev2,prtcnt_ev3,prtcnt_ev4,prtcnt_nv1,prtcnt_nv2,prtcnt_nv3,prtcnt_nv4
                                    ,woe1,woe2,woe3,woe4, iv1,iv2,iv3,iv4
                                    ,prtcnt_ev,prtcnt_nv,woe,iv])       



        elif col_name in (['기준년월','회원번호','장르명','샘플구분','Y']):
            continue
    ################################################################################################################
    ###### 범주형 컬럼 이벤트/Non이벤트별 범주 속성값 및 속성값 별 개수 추출
    ################################################################################################################
        else:
            col_type       = '범주형'
            col_min        = 0 
            col_qnt1       = 0
            col_qnt2       = 0
            col_qnt3       = 0
            col_max        = 0
            col_avg        = 0

            eevent         = '시청'
            ena_count      = df_anlys_mart_ev[col_name].isna().sum()
            etot_count     = df_anlys_mart_ev[col_name].count() + ena_count
            ecol_qnt1_cnt  = 0
            ecol_qnt2_cnt  = 0
            ecol_qnt3_cnt  = 0
            ecol_qnt4_cnt  = 0

            nevent         = '미시청'
            nna_count      = df_anlys_mart_nev[col_name].isna().sum()
            ntot_count     = df_anlys_mart_nev[col_name].count() + nna_count
            ncol_qnt1_cnt  = 0
            ncol_qnt2_cnt  = 0
            ncol_qnt3_cnt  = 0
            ncol_qnt4_cnt  = 0

            prtcnt_ev1     = 0
            prtcnt_ev2     = 0
            prtcnt_ev3     = 0
            prtcnt_ev4     = 0

            prtcnt_nv1     = 0
            prtcnt_nv2     = 0
            prtcnt_nv3     = 0
            prtcnt_nv4     = 0

            woe1           = 0
            woe2           = 0
            woe3           = 0
            woe4           = 0

            iv1            = 0
            iv2            = 0
            iv3            = 0
            iv4            = 0



            df_tmp_2           = df_anlys_mart_ev[col_name].value_counts().rename_axis('unique_values').reset_index(name='counts')
            df_tmp_3           = df_anlys_mart_nev[col_name].value_counts().rename_axis('unique_values').reset_index(name='counts')
            for idx2, col_val in enumerate(df_tmp_2.unique_values.tolist()):
                ecol_cnt       = df_tmp_2.counts.loc[df_tmp_2.unique_values==col_val].values[0]
                try:
                    ncol_cnt   = df_tmp_3.counts.loc[df_tmp_3.unique_values==col_val].values[0]
                except:
                    ncol_cnt   = 0

                prtcnt_ev      = ecol_cnt/(etot_count)
                prtcnt_nv      = ncol_cnt/(ntot_count)
                woe            = np.log(prtcnt_nv/prtcnt_ev)
                iv             = (prtcnt_nv-prtcnt_ev)*woe

                list_anlysIV.append([col_name,idx1+1,col_type,col_min,col_avg,col_max,col_qnt1,col_qnt2,col_qnt3
                                    ,eevent,etot_count,ena_count,col_val,ecol_cnt,ecol_qnt1_cnt,ecol_qnt2_cnt,ecol_qnt3_cnt,ecol_qnt4_cnt
                                    ,nevent,ntot_count,nna_count,col_val,ncol_cnt,ncol_qnt1_cnt,ncol_qnt2_cnt,ncol_qnt3_cnt,ncol_qnt4_cnt
                                    ,prtcnt_ev1,prtcnt_ev2,prtcnt_ev3,prtcnt_ev4,prtcnt_nv1,prtcnt_nv2,prtcnt_nv3,prtcnt_nv4
                                    ,woe1,woe2,woe3,woe4, iv1,iv2,iv3,iv4
                                    ,prtcnt_ev,prtcnt_nv,woe,iv])    


##############################################################################################################
###### 범주형 컬럼 이벤트/Non이벤트별 범주 속성값 및 속성값 별 개수 추출
##############################################################################################################

ftr_importances       = pd.Series(rfc.feature_importances_, index=tr_input_X_org.columns)
df_ftr_imptnc         = pd.DataFrame(ftr_importances)
df_ftr_imptnc         = df_ftr_imptnc.reset_index() 
df_ftr_imptnc.columns = ['컬럼명','중요도']

df_tmp                = pd.DataFrame(list_anlysIV)
df_tmp =df_tmp.fillna(0)
df_tmp=df_tmp.replace([np.inf, -np.inf], 999)
df_tmp.columns        =   ['컬럼명','컬럼번호','컬럼타입', '컬럼최소', '컬럼평균','컬럼최대','컬럼1사분위','컬럼2사분위','컬럼3사분위'
                          ,'이벤트','전체건수','전체결측건수','컬럼값','컬럼값개수','1분위개수','2분위개수','3분위개수','4분위개수'
                          ,'이벤트','전체건수','전체결측건수','컬럼값','컬럼값개수','1분위개수','2분위개수','3분위개수','4분위개수'
                          ,'%이벤트1구간','%이벤트2구간','%이벤트3구간','%이벤트4구간','%N이벤트1구간','%N이벤트2구간','%N이벤트3구간','%N이벤트4구간'
                          ,'WOE1구간','WOE2구간','WOE3구간','WOE4구간','IV1구간','IV2구간','IV3구간','IV4구간'
                          ,'%이벤트','%N이벤트','WOE','IV_sub']
df_tmp['IV합계']      = df_tmp.groupby('컬럼명')['IV_sub'].transform('sum')

df_통합               = pd.merge(df_tmp, df_ftr_imptnc, left_on='컬럼명', right_on='컬럼명', how='left')
df_통합               = df_통합.fillna(0)
df_통합.to_excel(f'{gnr}_IV_FTImportance.xlsx')

dend_tm = datetime.now(KST)
del_tm  = dend_tm-dst_tm
print(f'''[LOG] 분석마트 후처리 코드 실행 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}''')

##############################################################################################################
###### 회원 스코어링 구간 형성
##############################################################################################################

# if   phase == '학습':

# elif phase == '학습':

# pd.read_sql(f'''SELECT ''',conn)

# list_bin = np.arange(0,1.1,0.1)
# list_tmp = []
# for idx, val in enumerate(np.arange(0,1,0.1)):
#     cnt_bin_num    = df_Y_result['Y_Real'].loc[(df_Y_result['Y_Prob'] >= val)
#                                            &(df_Y_result['Y_Prob']<list_bin[idx+1])].count()
#     cnt_bin_rl_num = df_Y_result['Y_Real'].loc[(df_Y_result['Y_Prob'] >= val)
#                                            &(df_Y_result['Y_Prob']<list_bin[idx+1])
#                                            &(df_Y_result['Y_Real']    == 1)].count()
#     bin_pcnt       = cnt_bin_rl_num/cnt_bin_num
#     print(f'구간: {round(val,2)} 구간 수: {cnt_bin_num}, 구간실제: {cnt_bin_rl_num}, 구간실제비율:{round(bin_pcnt,3)}')
#     list_tmp.append([round(val,2),cnt_bin_num,cnt_bin_rl_num,round(bin_pcnt,3)])

# df_tmp=pd.concat([df_Y_result,pr_set[['나이','통근거리','모기지액수','계좌잔액','연봉']]],axis=1)
# df_tmp_2=pd.concat([df_tmp.loc[df_tmp['Y_Prob']>=0.5].mean(),df_tmp.loc[df_tmp['Y_Prob']>=0.5].std(),df_tmp.loc[df_tmp['Y_Prob']<0.5].mean(),df_tmp.loc[df_tmp['Y_Prob']<0.5].std()],axis=1)
# df_tmp_2.to_csv(f'{gnr}_result_cust.csv')
