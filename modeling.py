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

##############################################################################################################
### 모델링용 패키지 IMPORT
##############################################################################################################
import numpy as np
import sklearn 
from sklearn.model_selection import train_test_split 
import sklearn.ensemble  as rf
import lightgbm as lgbm
import xgboost  as xgb
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report
from sklearn.preprocessing import Binarizer
from sklearn.metrics import confusion_matrix
import re
import copy
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
###### 리스트 정보
########################################################################################################################
gnr                  = os.environ['gnr']
phase1               = os.environ['phase']
baseYM               = os.environ['baseYM']



list_YYYYMM          = ['201001','201002','201003','201004','201005','201006','201007','201008','201009',
                        '201010','201011','201012','201101','201102','201103','201104','201105','201106',
                        '201107','201108','201109','201110','201111','201112','201201','201202','201203',
                        '201204','201205','201206','201207','201208','201209','201210']

list_YYYYMM          = list_YYYYMM[:(list_YYYYMM.index(f'{baseYM}')+1)]

list_YYYYMMTST       = list_YYYYMM[(list_YYYYMM.index(f'{baseYM}')-2):(list_YYYYMM.index(f'{baseYM}')+1)]

list_col_cat         = ['성별','나이범주','학력','직업군','고객군','결혼여부','애완동물','가족구성원수'
                       ,'가족구성원수범주','대륙코드','국가명','주명','모기지액수범주','소유모기지수','소유차량수'
                       ,'소유차량수범주','계좌잔액범주','전임근무','연봉범주','연봉구간','자가소유형태','잔액부족횟수'
                       ,'모기지납입지연횟수','모기지납입지연횟수범주']

list_avoid		     = ['기준년월','회원번호','나이','나이범주','성별','결혼여부']

# list_gnr		     = ['애니','드라마','역사','코미디','액션','범죄','스릴러','다큐','모험','판타지'
#						'가족','로맨스','음악','호러','전쟁','서부','미스테리','단편','뮤지컬','스포츠','공상과학','전기','뉴스']

list_gnr			 = ['범죄']


list_YYYYMMTST       = list_YYYYMM[(list_YYYYMM.index(f'{baseYM}')-2):]




 
hyprprmvar           = False 
modelsave            = True


if phase1 == '학습':

    print(f'''[LOG] 모델링 코드 실행, 기준년월 = {baseYM}, 장르:{gnr}''')
    ######################################################################################################################## 
    ###### 인풋마트 적재 
    ########################################################################################################################
    dst_tm        = datetime.now(KST)
    print(f'''[LOG] 인풋마트 적재 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''') 
    filename1     = f'{gnr}_input_mart'
    with open(filename1, 'rb') as file:
    input_mart    = pickle.load(file)

    dend_tm = datetime.now(KST)
    del_tm  = dend_tm-dst_tm
    print(f'''[LOG] 인풋마트 적재 완료, 기준년월 = {baseYM}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, 행수 = {len(input_mart)}, 컬럼 수 = {len(input_mart.columns)}''')
    ########################################################################################################################
    ###### 모델링
    ########################################################################################################################
    YYYYMMTRNCLNG      	   = baseYM
    # list_YYYYMMFLR		   = ['201001', '201105','201112','201203']
    list_YYYYMMFLR		   = ['201001']
    # list_PRD			   = ['전체', '1년', '6개월', '3개월']
    list_PRD			   = ['전체']
    # list_colnames		   = ['장르', '시작년도','끝년도','학습기간', '단계','샘플비율'
    #                          ,'Cutoff','알고리즘','정확도(Accuracy)','AUC','반응률(Precision)'
    #                          ,'적중률(Recall)','F-1스코어','단계(Test1)','샘플비율'
    #                          ,'Cutoff','알고리즘','정확도(Accuracy)','AUC','반응률(Precision)'
    #                          ,'적중률(Recall)','F-1스코어','단계(Test2)','샘플비율'
    #                          ,'Cutoff','알고리즘','정확도(Accuracy)','AUC','반응률(Precision)'
    #                          ,'적중률(Recall)','F-1스코어','단계(Test3)','샘플비율'
    #                          ,'Cutoff','알고리즘','정확도(Accuracy)','AUC','반응률(Precision)'
    #                          ,'적중률(Recall)','F-1스코어'] 
    list_colnames          = [   '장르','시작년도','끝년도','학습기간'
                                ,'단계','샘플비율','Cutoff','알고리즘','정확도(Accuracy)','반응률(Precision)','적중률(Recall)','F-1스코어'
                         ,'단계(Test1)','샘플비율','Cutoff','알고리즘','정확도(Accuracy)','반응률(Precision)','적중률(Recall)','F-1스코어'
                         ,'단계(Test2)','샘플비율','Cutoff','알고리즘','정확도(Accuracy)','반응률(Precision)','적중률(Recall)','F-1스코어'
                         ,'단계(Test3)','샘플비율','Cutoff','알고리즘','정확도(Accuracy)','반응률(Precision)','적중률(Recall)','F-1스코어'] 


    random_state		   = 529
    list_output 		   = []
    # list_smpl_num          = [1,2,3]
    list_smpl_num          = [3]
    # list_algrthm           = ['RF','LGBM','XGB']
    list_algrthm           = ['RF']

    dst_tm        = datetime.now(KST)
    print(f'''[LOG] 모델링 시작, 기준년월 = {baseYM}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')

    df_tmp_test_vrtcl      = pd.DataFrame()
    for idx1,  YYYYMMTRNFLR in enumerate(list_YYYYMMFLR):
        ############################################################################################
        ###### Training & Validation
        ############################################################################################
        for idx2,  smpl_num in enumerate(list_smpl_num):
            ########################################################################################
            ###### Input 마트 생성
            ########################################################################################
            dst_tm        = datetime.now(KST)
            print(f'''[LOG] 모델링 인풋마트 샘플{smpl_num} 적재, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
            input_mart_sub 	 	  = input_mart.loc[(input_mart['샘플구분']             == smpl_num) 
                                                 & (input_mart['장르명']               == gnr)
                                                 & (input_mart['기준년월'].astype(str) >=YYYYMMTRNFLR) 
                                                 & (input_mart['기준년월'].astype(str) <=YYYYMMTRNCLNG)]
            dend_tm = datetime.now(KST)
            del_tm  = dend_tm-dst_tm
            print(f'''[LOG] 모델링 인풋마트 샘플{smpl_num} 적재 완료, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, 행수 = {len(input_mart_sub)}, 컬럼 수 = {len(input_mart_sub.columns)}''')
            # print(input_mart_sub)
            ########################################################################################
            ###### 모델링 - X Input Y Input 구분
            ########################################################################################
            dst_tm        = datetime.now(KST)
            print(f'''[LOG] 모델링 인풋마트 샘플{smpl_num} X,Y구분 , 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
            col_trgt_Y         = 'Y'
            list_col_bsc       = ['기준년월','회원번호','장르명','샘플구분'] + [col_trgt_Y]
            tr_input_X_org     = input_mart_sub.drop(columns = list_col_bsc)
            tr_input_X         = copy.deepcopy(tr_input_X_org)
            tr_input_X.columns = ['col_' + str(num).zfill(3) for num in list(range(0,tr_input_X.shape[1]))]
            col_info           = dict(zip(tr_input_X.columns,tr_input_X_org.columns))
            tr_input_Y         = input_mart_sub[col_trgt_Y]
            pickle.dump(tr_input_X.columns, open(f'{gnr}_colname', 'wb'))
            dend_tm = datetime.now(KST)
            del_tm  = dend_tm-dst_tm
            print(f'''[LOG] 모델링 인풋마트 샘플{smpl_num} X,Y구분 완료, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, Y행수 = {len(tr_input_Y)}, Y컬럼 수 = {len(tr_input_Y.columns)}''')
            ########################################################################################
            ###### 모델링 - TR Set VAL Set 구분 8:2
            ########################################################################################
            tr_x, val_x, tr_y, val_y = train_test_split(tr_input_X, tr_input_Y, test_size = 0.2, 
                                                        random_state = random_state)
            # tr_set 				     = [tr_x,tr_y]
            evl_set				     = [val_x,val_y]
            if hyprprmvar:
                rfc_mx_dpth_add 	 =   1
                xgbc_mx_dpth_add 	 = 0.1
            else:
                rfc_mx_dpth_add 	 =   0
                xgbc_mx_dpth_add 	 =   0

            for idx3, algrthm in enumerate(list_algrthm):
                df_tmp_test_hrz		     = pd.DataFrame()
                for idx4, phase in enumerate(['Validation','Test1','Test2','Test3']):
                
                    if  phase 		    == 'Validation':
                        pr_set           = []
                        true_set         = []
                        pr_set		  	 = val_x.reset_index().drop(columns='index')
                        true_set      	 = val_y.reset_index().drop(columns='index')
                        dst_tm        = datetime.now(KST)
                        print(f'''[LOG] 모델링 {algrthm} 학습 시작, 샘플{smpl_num} X,Y구분 , 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
                        if algrthm      == 'RF':
                        ################################################################################
                        ###### 모델링 - 랜덤포레스트
                        ################################################################################
                            
                            rfc                 = rf.RandomForestClassifier(max_depth = None, n_estimators = 300,random_state=random_state)
                            rfc.fit(tr_x,tr_y.values.ravel())
                            df_Y_result         = pd.concat([true_set, pd.Series([x[1] for x in rfc.predict_proba(pr_set)])], axis = 1)
                            df_Y_result.columns = ['Y_Real', 'Y_Prob'] 
                            
                        elif algrthm    == 'XGB':
                        ################################################################################
                        ###### 모델링 - XGBOOST
                        ################################################################################
                            xgbc                = xgb.XGBClassifier(learning_rate = 0.3, max_depth = (6+rfc_mx_dpth_add) ,random_state=random_state)
                            xgbc.fit(tr_x,tr_y.values.ravel())
                            df_Y_result         = pd.concat([true_set, pd.Series([x[1] for x in xgbc.predict_proba(pr_set)])], axis = 1)
                            df_Y_result.columns = ['Y_Real', 'Y_Prob'] 

                        elif algrthm    == 'LGBM':
                        ################################################################################
                        ###### 모델링 - LGBM
                        ################################################################################
                            lgbmc               = lgbm.LGBMClassifier(learning_rate = 0.1+(xgbc_mx_dpth_add), max_depth = -1 ,n_estimators = 1000,seed=random_state)
                            lgbmc.fit(tr_x,tr_y.values.ravel())
                            df_Y_result         = pd.concat([true_set, pd.Series([x[1] for x in lgbmc.predict_proba(pr_set)])], axis = 1)
                            df_Y_result.columns = ['Y_Real', 'Y_Prob'] 
                        dend_tm = datetime.now(KST)
                        del_tm  = dend_tm-dst_tm
                        print(f'''[LOG] 모델링 {algrthm} 학습 완료, 샘플{smpl_num} X,Y구분 완료, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, Y행수 = {len(df_Y_result)}, Y컬럼 수 = {len(df_Y_result.columns)}''')
                    elif phase in (['Test1','Test2', 'Test3']):
                    ################################################################################
                    ###### 모델링 테스트- X Input Y Input 구분
                    ################################################################################
                        pr_set           = []
                        true_set         = []
                        input_mart_tst	 = input_mart.loc[input_mart['기준년월'].astype(str)==list_YYYYMMTST[idx4-1]]
                        pr_set		  	 = input_mart_tst.drop(columns = list_col_bsc).reset_index().drop(columns='index')
                        pr_set.columns   = tr_input_X.columns
                        true_set      	 = input_mart_tst[col_trgt_Y].reset_index().drop(columns='index')
                        
                                        
                    else:
                        continue
                    
                    

                    ################################################################################
                    ###### 모델링 데이터 후처리
                    ################################################################################
                    list_test_data       = []
                    dst_tm        = datetime.now(KST)
                    print(f'''[LOG] 모델링 {algrthm} {phase} 후처리 시작, 샘플{smpl_num} X,Y구분 , 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
                    for thrshld in range(1, 100):
                    # for thrshld in [57]:
                        tmp_Y = df_Y_result.copy()
                        
                        tmp_Y["cutOff"] = thrshld
                        
                        # 예측여부 판단
                        tmp_Y["Y_Pred"] = tmp_Y["Y_Prob"].apply(lambda x: 1 if x >= thrshld / 100 else 0)
                        
                        # 성능 판단
                        tmp_Y["N11"]    = np.where((tmp_Y["Y_Real"] == 1) & (tmp_Y["Y_Pred"] == 1), 1, 0)
                        tmp_Y["N10"]    = np.where((tmp_Y["Y_Real"] == 1) & (tmp_Y["Y_Pred"] == 0), 1, 0)
                        tmp_Y["N01"]    = np.where((tmp_Y["Y_Real"] == 0) & (tmp_Y["Y_Pred"] == 1), 1, 0)
                        tmp_Y["N00"]    = np.where((tmp_Y["Y_Real"] == 0) & (tmp_Y["Y_Pred"] == 0), 1, 0)
                        
                        # 집계
                        tmp_Y_2 = (
                            tmp_Y
                            .groupby("cutOff", as_index = False)
                            .agg(
                                  R1    = ("Y_Real", 'sum')
                                , P1    = ("Y_Pred", 'sum')
                                , N11   = ("N11"   , 'sum')
                                , N10   = ("N10"   , 'sum')
                                , N01   = ("N01"   , 'sum')
                                , N00   = ("N00"   , 'sum')
                            )   
                        )
                        
                        # 성능 계산
                        tmp_Y_2["Accuracy"]           = (tmp_Y_2["N11"] + tmp_Y_2["N00"]) / len(tmp_Y)
                        tmp_Y_2["반응률(Precision)"]  = tmp_Y_2["N11"]  / (tmp_Y_2["N11"] + tmp_Y_2["N01"])
                        tmp_Y_2["적중률(Recall)"]     = tmp_Y_2["N11"]  / (tmp_Y_2["N11"] + tmp_Y_2["N10"])
                        tmp_Y_2["F1Score"]            = 2 * tmp_Y_2["반응률(Precision)"] * tmp_Y_2["적중률(Recall)"] / (tmp_Y_2["반응률(Precision)"] + tmp_Y_2["적중률(Recall)"])
                        
                        if  phase           == 'Validation':
                            list_output.append([gnr,YYYYMMTRNFLR,'201205',list_PRD[idx1],phase,smpl_num,thrshld
                                            ,algrthm, tmp_Y_2['Accuracy'].values[0]
                                                     ,tmp_Y_2['반응률(Precision)'].values[0]
                                                     ,tmp_Y_2['적중률(Recall)'].values[0]
                                                     ,tmp_Y_2['F1Score'].values[0]])
                        elif phase        in (['Test1','Test2', 'Test3']):
                            list_test_data.append([phase,smpl_num,thrshld
                                            ,algrthm, tmp_Y_2['Accuracy'].values[0]
                                                     ,tmp_Y_2['반응률(Precision)'].values[0]
                                                     ,tmp_Y_2['적중률(Recall)'].values[0]
                                                     ,tmp_Y_2['F1Score'].values[0]])
                    dend_tm = datetime.now(KST)
                    del_tm  = dend_tm-dst_tm
                    print(f'''[LOG] 모델링 {algrthm} {phase} 후처리 완료, 샘플{smpl_num} X,Y구분 완료, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}, Y행수 = {len(tmp_Y_2)}, Y컬럼 수 = {len(tmp_Y_2.columns)}''')

                    df_tmp_test_hrz  = pd.concat([df_tmp_test_hrz,pd.DataFrame(list_test_data)],axis=1)

                df_tmp_test_vrtcl    = pd.concat([df_tmp_test_vrtcl,df_tmp_test_hrz],axis=0)
                ####################################################################################
                ###### 모델 및 결과 Pickle 저장
                ####################################################################################
                dst_tm        = datetime.now(KST)
                print(f'''[LOG] 모델링 {algrthm} {phase} 모델 저장 시작, 샘플{smpl_num} X,Y구분 , 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 시작시간 = {datetime.strftime(dst_tm, '%Y%m%d %H:%M:%S')}''')
                if modelsave:
                    filename      = f'model_{gnr}.sav'
                    if algrthm   == 'RF':
                        pickle.dump(rfc, open(filename, 'wb'))
                    elif algrthm == 'XGB':
                        pickle.dump(xgbc, open(filename, 'wb'))
                    elif algrthm == 'LGBM':
                        pickle.dump(lgbmc, open(filename, 'wb'))
                dend_tm = datetime.now(KST)
                del_tm  = dend_tm-dst_tm
                print(f'''[LOG] 모델링 {algrthm} {phase} 모델 저장 완료, 샘플{smpl_num} X,Y구분 완료, 시작년월 = {YYYYMMTRNFLR}, 끝년월 = {YYYYMMTRNCLNG}, 완료시간 = {datetime.strftime(dend_tm, '%Y%m%d %H:%M:%S')}, 소요시간 = {del_tm}''')
    ################################################################################################
    ###### 모델링 데이터 엑셀 저장
    ################################################################################################

    df_tmp_test_vrtcl.columns = list_colnames[12:]
    df_tmp_val                = pd.DataFrame(list_output, index=df_tmp_test_vrtcl.index)
    df_tmp_val.columns        = list_colnames[:12]
    df_output 		          = pd.concat([df_tmp_val,df_tmp_test_vrtcl],axis=1)
    df_output.to_excel(f'''{gnr}_result.xlsx''')


