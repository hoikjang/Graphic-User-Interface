########################################################################################################################
### 패키지 IMPORT
########################################################################################################################

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO

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
### 리스트 정보
########################################################################################################################
baseYM                    = os.environ['baseYM']

list_col_specified_1      = ['장르코드', '추천여부' ,'활동코드' ,'평점' ,'판매금액' ,'대륙코드' ,'학력' ,'전임근무' ,
                             '연봉구간','직업군' ,'결혼여부' ,'애완동물' ,'고객군', '제작년도', '예산액', '흥행액', '가족구성원수',
                             '잔액부족횟수', '모기지액수' ,'소유차량수' ,'소유모기지수']
                             
list_col_specified_2      = ['모기지액수']

list_YYYYMM               = ['201001','201002','201003','201004','201005','201006','201007','201008','201009',
                             '201010','201011','201012','201101','201102','201103','201104','201105','201106',
                             '201107','201108','201109','201110','201111','201112','201201','201202','201203',
                             '201204','201205','201206','201207','201208','201209','201210']

list_YYYYMM               = list_YYYYMM[:(list_YYYYMM.index(f'{baseYM}')+1)]

######################################################################################################################## 
#### 피처 마트 생성
########################################################################################################################

input_str='FTRMART'
cur.execute(f"""drop table if exists {input_str}""")
cur.execute(f"""
    create table if not exists {input_str} (
         기준년월                                                     TEXT
        ,회원번호                                                     TEXT
        ,성별                                                         TEXT
        ,나이                                                         INT
        ,나이범주                                                     TEXT
        ,학력                                                         TEXT
        ,직업군                                                       TEXT
        ,고객군                                                       TEXT
        ,통근거리                                                     DOUBLE
        ,결혼여부                                                     TEXT
        ,애완동물                                                     TEXT
        ,가족구성원수                                                 TEXT
        ,가족구성원수범주                                             TEXT
        ,대륙코드                                                     TEXT
        ,국가명                                                       TEXT
        ,주명                                                         TEXT
        ,모기지액수                                                   INT
        ,모기지액수범주                                               TEXT
        ,소유모기지수                                                 TEXT
        ,소유차량수                                                   TEXT
        ,소유차량수범주                                               TEXT
        ,계좌잔액                                                     INT
        ,계좌잔액범주                                                 TEXT
        ,전임근무                                                     TEXT
        ,연봉                                                         INT
        ,연봉범주                                                     TEXT
        ,연봉구간                                                     TEXT
        ,자가소유형태                                                 TEXT
        ,잔액부족횟수                                                 TEXT
        ,모기지납입지연횟수                                           INT
        ,모기지납입지연횟수범주                                       TEXT
        ,월별애니시청여부                                             TEXT
        ,월별드라마시청여부                                           TEXT
        ,월별역사시청여부                                             TEXT
        ,월별코미디시청여부                                           TEXT
        ,월별액션시청여부                                             TEXT
        ,월별범죄시청여부                                             TEXT
        ,월별스릴러시청여부                                           TEXT
        ,월별다큐시청여부                                             TEXT
        ,월별모험시청여부                                             TEXT
        ,월별판타지시청여부                                           TEXT
        ,월별가족시청여부                                             TEXT
        ,월별로맨스시청여부                                           TEXT
        ,월별음악시청여부                                             TEXT
        ,월별호러시청여부                                             TEXT
        ,월별전쟁시청여부                                             TEXT
        ,월별서부시청여부                                             TEXT
        ,월별미스테리시청여부                                         TEXT
        ,월별단편시청여부                                             TEXT
        ,월별뮤지컬시청여부                                           TEXT
        ,월별스포츠시청여부                                           TEXT
        ,월별공상과학시청여부                                         TEXT
        ,월별전기시청여부                                             TEXT
        ,월별뉴스시청여부                                             TEXT
        ,월별평점평균                                                 DOUBLE
        ,월별판매금액평균                                             DOUBLE
        ,월별평점총합                                                 INT
        ,월별판매금액총합                                             DOUBLE
        ,월별시청횟수                                                 INT
        ,월별탐색횟수                                                 INT
        ,월별만족횟수                                                 INT
        ,월별불만족횟수                                               INT
        ,월별시청평균                                                 DOUBLE
        ,월별탐색평균                                                 DOUBLE
        ,월별만족평균                                                 DOUBLE
        ,월별불만족평균                                               DOUBLE
        ,월별시청흥행액평균                                           DOUBLE
        ,월별시청예산액평균                                           DOUBLE
        ,월별시청흥행액총액                                           INT
        ,월별시청예산액총액                                           INT
        ,월별애니판매금액총액                                         DOUBLE
        ,월별드라마판매금액총액                                       DOUBLE
        ,월별역사판매금액총액                                         DOUBLE
        ,월별코미디판매금액총액                                       DOUBLE
        ,월별액션판매금액총액                                         DOUBLE
        ,월별범죄판매금액총액                                         DOUBLE
        ,월별스릴러판매금액총액                                       DOUBLE
        ,월별다큐판매금액총액                                         DOUBLE
        ,월별모험판매금액총액                                         DOUBLE
        ,월별판타지판매금액총액                                       DOUBLE
        ,월별가족판매금액총액                                         DOUBLE
        ,월별로맨스판매금액총액                                       DOUBLE
        ,월별음악판매금액총액                                         DOUBLE
        ,월별호러판매금액총액                                         DOUBLE
        ,월별전쟁판매금액총액                                         DOUBLE
        ,월별서부판매금액총액                                         DOUBLE
        ,월별미스테리판매금액총액                                     DOUBLE
        ,월별단편판매금액총액                                         DOUBLE
        ,월별뮤지컬판매금액총액                                       DOUBLE
        ,월별스포츠판매금액총액                                       DOUBLE
        ,월별공상과학판매금액총액                                     DOUBLE
        ,월별전기판매금액총액                                         DOUBLE
        ,월별뉴스판매금액총액                                         DOUBLE
        ,월별애니판매금액평균                                         DOUBLE
        ,월별드라마판매금액평균                                       DOUBLE
        ,월별역사판매금액평균                                         DOUBLE
        ,월별코미디판매금액평균                                       DOUBLE
        ,월별액션판매금액평균                                         DOUBLE
        ,월별범죄판매금액평균                                         DOUBLE
        ,월별스릴러판매금액평균                                       DOUBLE
        ,월별다큐판매금액평균                                         DOUBLE
        ,월별모험판매금액평균                                         DOUBLE
        ,월별판타지판매금액평균                                       DOUBLE
        ,월별가족판매금액평균                                         DOUBLE
        ,월별로맨스판매금액평균                                       DOUBLE
        ,월별음악판매금액평균                                         DOUBLE
        ,월별호러판매금액평균                                         DOUBLE
        ,월별전쟁판매금액평균                                         DOUBLE
        ,월별서부판매금액평균                                         DOUBLE
        ,월별미스테리판매금액평균                                     DOUBLE
        ,월별단편판매금액평균                                         DOUBLE
        ,월별뮤지컬판매금액평균                                       DOUBLE
        ,월별스포츠판매금액평균                                       DOUBLE
        ,월별공상과학판매금액평균                                     DOUBLE
        ,월별전기판매금액평균                                         DOUBLE
        ,월별뉴스판매금액평균                                         DOUBLE
        ,월별애니시청횟수                                             INT
        ,월별애니탐색횟수                                             INT
        ,월별드라마시청횟수                                           INT
        ,월별드라마탐색횟수                                           INT
        ,월별역사시청횟수                                             INT
        ,월별역사탐색횟수                                             INT
        ,월별코미디시청횟수                                           INT
        ,월별코미디탐색횟수                                           INT
        ,월별액션시청횟수                                             INT
        ,월별액션탐색횟수                                             INT
        ,월별범죄시청횟수                                             INT
        ,월별범죄탐색횟수                                             INT
        ,월별스릴러시청횟수                                           INT
        ,월별스릴러탐색횟수                                           INT
        ,월별다큐시청횟수                                             INT
        ,월별다큐탐색횟수                                             INT
        ,월별모험시청횟수                                             INT
        ,월별모험탐색횟수                                             INT
        ,월별판타지시청횟수                                           INT
        ,월별판타지탐색횟수                                           INT
        ,월별가족시청횟수                                             INT
        ,월별가족탐색횟수                                             INT
        ,월별로맨스시청횟수                                           INT
        ,월별로맨스탐색횟수                                           INT
        ,월별음악시청횟수                                             INT
        ,월별음악탐색횟수                                             INT
        ,월별호러시청횟수                                             INT
        ,월별호러탐색횟수                                             INT
        ,월별전쟁시청횟수                                             INT
        ,월별전쟁탐색횟수                                             INT
        ,월별서부시청횟수                                             INT
        ,월별서부탐색횟수                                             INT
        ,월별미스테리시청횟수                                         INT
        ,월별미스테리탐색횟수                                         INT
        ,월별단편시청횟수                                             INT
        ,월별단편탐색횟수                                             INT
        ,월별뮤지컬시청횟수                                           INT
        ,월별뮤지컬탐색횟수                                           INT
        ,월별스포츠시청횟수                                           INT
        ,월별스포츠탐색횟수                                           INT
        ,월별공상과학시청횟수                                         INT
        ,월별공상과학탐색횟수                                         INT
        ,월별전기시청횟수                                             INT
        ,월별전기탐색횟수                                             INT
        ,월별뉴스시청횟수                                             INT
        ,월별뉴스탐색횟수                                             INT
        ,월별애니추천횟수                                             INT
        ,월별애니만족횟수                                             INT
        ,월별드라마추천횟수                                           INT
        ,월별드라마만족횟수                                           INT
        ,월별역사추천횟수                                             INT
        ,월별역사만족횟수                                             INT
        ,월별코미디추천횟수                                           INT
        ,월별코미디만족횟수                                           INT
        ,월별액션추천횟수                                             INT
        ,월별액션만족횟수                                             INT
        ,월별범죄추천횟수                                             INT
        ,월별범죄만족횟수                                             INT
        ,월별스릴러추천횟수                                           INT
        ,월별스릴러만족횟수                                           INT
        ,월별다큐추천횟수                                             INT
        ,월별다큐만족횟수                                             INT
        ,월별모험추천횟수                                             INT
        ,월별모험만족횟수                                             INT
        ,월별판타지추천횟수                                           INT
        ,월별판타지만족횟수                                           INT
        ,월별가족추천횟수                                             INT
        ,월별가족만족횟수                                             INT
        ,월별로맨스추천횟수                                           INT
        ,월별로맨스만족횟수                                           INT
        ,월별음악추천횟수                                             INT
        ,월별음악만족횟수                                             INT
        ,월별호러추천횟수                                             INT
        ,월별호러만족횟수                                             INT
        ,월별전쟁추천횟수                                             INT
        ,월별전쟁만족횟수                                             INT
        ,월별서부추천횟수                                             INT
        ,월별서부만족횟수                                             INT
        ,월별미스테리추천횟수                                         INT
        ,월별미스테리만족횟수                                         INT
        ,월별단편추천횟수                                             INT
        ,월별단편만족횟수                                             INT
        ,월별뮤지컬추천횟수                                           INT
        ,월별뮤지컬만족횟수                                           INT
        ,월별스포츠추천횟수                                           INT
        ,월별스포츠만족횟수                                           INT
        ,월별공상과학추천횟수                                         INT
        ,월별공상과학만족횟수                                         INT
        ,월별전기추천횟수                                             INT
        ,월별전기만족횟수                                             INT
        ,월별뉴스추천횟수                                             INT
        ,월별뉴스만족횟수                                             INT
        ,월별애니불만족횟수                                           INT
        ,월별드라마불만족횟수                                         INT
        ,월별역사불만족횟수                                           INT
        ,월별코미디불만족횟수                                         INT
        ,월별액션불만족횟수                                           INT
        ,월별범죄불만족횟수                                           INT
        ,월별스릴러불만족횟수                                         INT
        ,월별다큐불만족횟수                                           INT
        ,월별모험불만족횟수                                           INT
        ,월별판타지불만족횟수                                         INT
        ,월별가족불만족횟수                                           INT
        ,월별로맨스불만족횟수                                         INT
        ,월별음악불만족횟수                                           INT
        ,월별호러불만족횟수                                           INT
        ,월별전쟁불만족횟수                                           INT
        ,월별서부불만족횟수                                           INT
        ,월별미스테리불만족횟수                                       INT
        ,월별단편불만족횟수                                           INT
        ,월별뮤지컬불만족횟수                                         INT
        ,월별스포츠불만족횟수                                         INT
        ,월별공상과학불만족횟수                                       INT
        ,월별전기불만족횟수                                           INT
        ,월별뉴스불만족횟수                                           INT
        ,월별흥행0등급시청횟수                                        INT
        ,월별흥행1등급시청횟수                                        INT
        ,월별흥행2등급시청횟수                                        INT
        ,월별흥행3등급시청횟수                                        INT
        ,월별흥행4등급시청횟수                                        INT
        ,월별예산0등급시청횟수                                        INT
        ,월별예산1등급시청횟수                                        INT
        ,월별예산2등급시청횟수                                        INT
        ,월별예산3등급시청횟수                                        INT
        ,월별예산4등급시청횟수                                        INT
        ,월별1950이전시청횟수                                         INT
        ,월별1965이전시청횟수                                         INT
        ,월별1980이전시청횟수                                         INT
        ,월별1999이전시청횟수                                         INT
        ,월별2010이전시청횟수                                         INT
        ,월별2010이후시청횟수                                         INT
        ,PRIMARY KEY(기준년월, 회원번호) 
    )
"""
)
for idx, YYYYMM in enumerate(list_YYYYMM):
    sql_ftr_mart= f'''
        SELECT
            {YYYYMM}                                                                                                                         AS 기준년월
           ,A.회원번호
           ,A.성별
           ,A.나이
           ,A.나이범주
           ,A.학력
           ,A.직업군
           ,A.고객군
           ,A.통근거리
           ,A.결혼여부
           ,A.애완동물
           ,A.가족구성원수
           ,A.가족구성원수범주
           ,A.대륙코드
           ,A.국가명
           ,A.주명
           ,A.모기지액수
           ,A.모기지액수범주
           ,A.소유모기지수
           ,A.소유차량수
           ,A.소유차량수범주
           ,A.계좌잔액
           ,A.계좌잔액범주
           ,A.전임근무
           ,A.연봉
           ,A.연봉범주
           ,A.연봉구간
           ,A.자가소유형태
           ,A.잔액부족횟수
           ,A.모기지납입지연횟수
           ,A.모기지납입지연횟수범주
           ,B.월별드라마시청여부
           ,B.월별애니시청여부
           ,B.월별역사시청여부
           ,B.월별코미디시청여부
           ,B.월별액션시청여부
           ,B.월별범죄시청여부
           ,B.월별스릴러시청여부
           ,B.월별다큐시청여부
           ,B.월별모험시청여부
           ,B.월별판타지시청여부
           ,B.월별가족시청여부
           ,B.월별로맨스시청여부
           ,B.월별음악시청여부
           ,B.월별호러시청여부
           ,B.월별전쟁시청여부
           ,B.월별서부시청여부
           ,B.월별미스테리시청여부
           ,B.월별단편시청여부
           ,B.월별뮤지컬시청여부
           ,B.월별스포츠시청여부
           ,B.월별공상과학시청여부
           ,B.월별전기시청여부
           ,B.월별뉴스시청여부
           ,B.월별평점평균
           ,B.월별판매금액평균
           ,B.월별평점총합
           ,B.월별판매금액총합
           ,B.월별시청횟수
           ,B.월별탐색횟수
           ,B.월별만족횟수
           ,B.월별불만족횟수
           ,B.월별시청평균
           ,B.월별탐색평균
           ,B.월별만족평균
           ,B.월별불만족평균
           ,B.월별시청흥행액평균
           ,B.월별시청예산액평균
           ,B.월별시청흥행액총액
           ,B.월별시청예산액총액
           ,B.월별애니판매금액총액
           ,B.월별드라마판매금액총액
           ,B.월별역사판매금액총액
           ,B.월별코미디판매금액총액
           ,B.월별액션판매금액총액
           ,B.월별범죄판매금액총액
           ,B.월별스릴러판매금액총액
           ,B.월별다큐판매금액총액
           ,B.월별모험판매금액총액
           ,B.월별판타지판매금액총액
           ,B.월별가족판매금액총액
           ,B.월별로맨스판매금액총액
           ,B.월별음악판매금액총액
           ,B.월별호러판매금액총액
           ,B.월별전쟁판매금액총액
           ,B.월별서부판매금액총액
           ,B.월별미스테리판매금액총액
           ,B.월별단편판매금액총액
           ,B.월별뮤지컬판매금액총액
           ,B.월별스포츠판매금액총액
           ,B.월별공상과학판매금액총액
           ,B.월별전기판매금액총액
           ,B.월별뉴스판매금액총액
           ,B.월별애니판매금액평균
           ,B.월별드라마판매금액평균
           ,B.월별역사판매금액평균
           ,B.월별코미디판매금액평균
           ,B.월별액션판매금액평균
           ,B.월별범죄판매금액평균
           ,B.월별스릴러판매금액평균
           ,B.월별다큐판매금액평균
           ,B.월별모험판매금액평균
           ,B.월별판타지판매금액평균
           ,B.월별가족판매금액평균
           ,B.월별로맨스판매금액평균
           ,B.월별음악판매금액평균
           ,B.월별호러판매금액평균
           ,B.월별전쟁판매금액평균
           ,B.월별서부판매금액평균
           ,B.월별미스테리판매금액평균
           ,B.월별단편판매금액평균
           ,B.월별뮤지컬판매금액평균
           ,B.월별스포츠판매금액평균
           ,B.월별공상과학판매금액평균
           ,B.월별전기판매금액평균
           ,B.월별뉴스판매금액평균
           ,B.월별애니시청횟수
           ,B.월별애니탐색횟수
           ,B.월별드라마시청횟수
           ,B.월별드라마탐색횟수
           ,B.월별역사시청횟수
           ,B.월별역사탐색횟수
           ,B.월별코미디시청횟수
           ,B.월별코미디탐색횟수
           ,B.월별액션시청횟수
           ,B.월별액션탐색횟수
           ,B.월별범죄시청횟수
           ,B.월별범죄탐색횟수
           ,B.월별스릴러시청횟수
           ,B.월별스릴러탐색횟수
           ,B.월별다큐시청횟수
           ,B.월별다큐탐색횟수
           ,B.월별모험시청횟수
           ,B.월별모험탐색횟수
           ,B.월별판타지시청횟수
           ,B.월별판타지탐색횟수
           ,B.월별가족시청횟수
           ,B.월별가족탐색횟수
           ,B.월별로맨스시청횟수
           ,B.월별로맨스탐색횟수
           ,B.월별음악시청횟수
           ,B.월별음악탐색횟수
           ,B.월별호러시청횟수
           ,B.월별호러탐색횟수
           ,B.월별전쟁시청횟수
           ,B.월별전쟁탐색횟수
           ,B.월별서부시청횟수
           ,B.월별서부탐색횟수
           ,B.월별미스테리시청횟수
           ,B.월별미스테리탐색횟수
           ,B.월별단편시청횟수
           ,B.월별단편탐색횟수
           ,B.월별뮤지컬시청횟수
           ,B.월별뮤지컬탐색횟수
           ,B.월별스포츠시청횟수
           ,B.월별스포츠탐색횟수
           ,B.월별공상과학시청횟수
           ,B.월별공상과학탐색횟수
           ,B.월별전기시청횟수
           ,B.월별전기탐색횟수
           ,B.월별뉴스시청횟수
           ,B.월별뉴스탐색횟수
           ,B.월별애니추천횟수
           ,B.월별애니만족횟수
           ,B.월별드라마추천횟수
           ,B.월별드라마만족횟수
           ,B.월별역사추천횟수
           ,B.월별역사만족횟수
           ,B.월별코미디추천횟수
           ,B.월별코미디만족횟수
           ,B.월별액션추천횟수
           ,B.월별액션만족횟수
           ,B.월별범죄추천횟수
           ,B.월별범죄만족횟수
           ,B.월별스릴러추천횟수
           ,B.월별스릴러만족횟수
           ,B.월별다큐추천횟수
           ,B.월별다큐만족횟수
           ,B.월별모험추천횟수
           ,B.월별모험만족횟수
           ,B.월별판타지추천횟수
           ,B.월별판타지만족횟수
           ,B.월별가족추천횟수
           ,B.월별가족만족횟수
           ,B.월별로맨스추천횟수
           ,B.월별로맨스만족횟수
           ,B.월별음악추천횟수
           ,B.월별음악만족횟수
           ,B.월별호러추천횟수
           ,B.월별호러만족횟수
           ,B.월별전쟁추천횟수
           ,B.월별전쟁만족횟수
           ,B.월별서부추천횟수
           ,B.월별서부만족횟수
           ,B.월별미스테리추천횟수
           ,B.월별미스테리만족횟수
           ,B.월별단편추천횟수
           ,B.월별단편만족횟수
           ,B.월별뮤지컬추천횟수
           ,B.월별뮤지컬만족횟수
           ,B.월별스포츠추천횟수
           ,B.월별스포츠만족횟수
           ,B.월별공상과학추천횟수
           ,B.월별공상과학만족횟수
           ,B.월별전기추천횟수
           ,B.월별전기만족횟수
           ,B.월별뉴스추천횟수
           ,B.월별뉴스만족횟수
           ,B.월별애니불만족횟수
           ,B.월별드라마불만족횟수
           ,B.월별역사불만족횟수
           ,B.월별코미디불만족횟수
           ,B.월별액션불만족횟수
           ,B.월별범죄불만족횟수
           ,B.월별스릴러불만족횟수
           ,B.월별다큐불만족횟수
           ,B.월별모험불만족횟수
           ,B.월별판타지불만족횟수
           ,B.월별가족불만족횟수
           ,B.월별로맨스불만족횟수
           ,B.월별음악불만족횟수
           ,B.월별호러불만족횟수
           ,B.월별전쟁불만족횟수
           ,B.월별서부불만족횟수
           ,B.월별미스테리불만족횟수
           ,B.월별단편불만족횟수
           ,B.월별뮤지컬불만족횟수
           ,B.월별스포츠불만족횟수
           ,B.월별공상과학불만족횟수
           ,B.월별전기불만족횟수
           ,B.월별뉴스불만족횟수
           ,B.월별흥행0등급시청횟수
           ,B.월별흥행1등급시청횟수
           ,B.월별흥행2등급시청횟수
           ,B.월별흥행3등급시청횟수
           ,B.월별흥행4등급시청횟수
           ,B.월별예산0등급시청횟수
           ,B.월별예산1등급시청횟수
           ,B.월별예산2등급시청횟수
           ,B.월별예산3등급시청횟수
           ,B.월별예산4등급시청횟수
           ,B.월별1950이전시청횟수
           ,B.월별1965이전시청횟수
           ,B.월별1980이전시청횟수
           ,B.월별1999이전시청횟수
           ,B.월별2010이전시청횟수
           ,B.월별2010이후시청횟수
        FROM(
            SELECT
                DISTINCT 
                회원번호
               ,대륙코드
               ,국가명
               ,주명
               ,모기지액수
               ,CASE
                    WHEN 
                        CAST(모기지액수 AS INT) >=       0 and CAST(모기지액수 AS INT) < 200000
                    THEN '모기지1구간'
                    WHEN 
                        CAST(모기지액수 AS INT) >=  200000 and CAST(모기지액수 AS INT) < 500000
                    THEN '모기지2구간'
                END                                                                                                                                      AS 모기지액수범주
               ,소유차량수
               ,CASE
                    WHEN 소유차량수  in (0,1) THEN '적다'
                    WHEN 소유차량수     >= 2  THEN '많다'
                END                                                                                                                                      AS 소유차량수범주
               ,계좌잔액
               ,CASE
                    WHEN 
                        CAST(계좌잔액 AS INT) >=    0 and CAST(계좌잔액 AS INT) <  500 
                    THEN '잔액1'
                    WHEN 
                        CAST(계좌잔액 AS INT) >=  500 and CAST(계좌잔액 AS INT) < 1000 
                    THEN '잔액2'
                    WHEN 
                        CAST(계좌잔액 AS INT) >= 1000 and CAST(계좌잔액 AS INT) < 2000 
                    THEN '잔액3'
                END                                                                                                                                      AS 계좌잔액범주
               ,소유모기지수
               ,전임근무
               ,연봉
               ,CASE
                    WHEN 
                        CAST(연봉 AS INT) >=     29 and CAST(연봉 AS INT)<  27629 
                    THEN '연봉1'
                    WHEN 
                        CAST(연봉 AS INT) >=  27629 and CAST(연봉 AS INT)<  48329 
                    THEN '연봉2'
                    WHEN 
                        CAST(연봉 AS INT) >=  48329 and CAST(연봉 AS INT)<  55229 
                    THEN '연봉3'
                    WHEN 
                        CAST(연봉 AS INT) >=  55229 and CAST(연봉 AS INT)<  75929 
                    THEN '연봉4'
                    WHEN 
                        CAST(연봉 AS INT) >=  75929 and CAST(연봉 AS INT)<  96629 
                    THEN '연봉5'
                    WHEN 
                        CAST(연봉 AS INT) >=  96629 and CAST(연봉 AS INT)< 120000 
                    THEN '연봉6'
                END                                                                                                                          AS 연봉범주
               ,연봉구간
               ,자가소유형태
               ,모기지납입지연횟수
               ,CASE
                    WHEN 모기지납입지연횟수  = 0 THEN '0회'
                    WHEN 모기지납입지연횟수  = 1 THEN '1회'
                    WHEN 모기지납입지연횟수  = 2 THEN '2회'
                    WHEN 모기지납입지연횟수 >= 3 THEN '3회이상'
                END                                                                                                                          AS 모기지납입지연횟수범주
               ,잔액부족횟수
               ,결혼여부
               ,애완동물
               ,가족구성원수
               ,CASE
                    WHEN 가족구성원수  = 1                        THEN '싱글'
                    WHEN 가족구성원수  = 2                        THEN '결혼'
                    WHEN 가족구성원수 >= 3                        THEN '(대)가족'
                END                                                                                                                          AS 가족구성원수범주
               ,성별
               ,나이
               ,CASE
                    WHEN 
                        CAST(나이 AS INT) >= 16 and CAST(나이 AS INT)< 32
                    THEN '1세대'
                    WHEN 
                        CAST(나이 AS INT) >= 32 and CAST(나이 AS INT)< 48
                    THEN '2세대'
                    WHEN 
                        CAST(나이 AS INT) >= 48 and CAST(나이 AS INT)< 64
                    THEN '3세대'
                    WHEN 
                        CAST(나이 AS INT) >= 64 and CAST(나이 AS INT)< 81
                    THEN '4세대'
                END                                                                                                                          AS 나이범주
               ,학력
               ,직업군
               ,고객군
               ,통근거리
            FROM CUSTOMER
        )                                                                                                                                    AS A
        LEFT JOIN(
            SELECT
                 B.기준년월
                ,B.회원번호
                ,MAX( CASE WHEN B.드라마    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별드라마시청여부'
                ,MAX( CASE WHEN B.애니      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별애니시청여부'
                ,MAX( CASE WHEN B.역사      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별역사시청여부'
                ,MAX( CASE WHEN B.코미디    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별코미디시청여부'
                ,MAX( CASE WHEN B.액션      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별액션시청여부'
                ,MAX( CASE WHEN B.범죄      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별범죄시청여부'
                ,MAX( CASE WHEN B.스릴러    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별스릴러시청여부'
                ,MAX( CASE WHEN B.다큐      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별다큐시청여부'
                ,MAX( CASE WHEN B.모험      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별모험시청여부'
                ,MAX( CASE WHEN B.판타지    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별판타지시청여부'
                ,MAX( CASE WHEN B.가족      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별가족시청여부'
                ,MAX( CASE WHEN B.로맨스    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별로맨스시청여부'
                ,MAX( CASE WHEN B.음악      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별음악시청여부'
                ,MAX( CASE WHEN B.호러      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별호러시청여부'
                ,MAX( CASE WHEN B.전쟁      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별전쟁시청여부'
                ,MAX( CASE WHEN B.서부      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별서부시청여부'
                ,MAX( CASE WHEN B.미스테리  = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별미스테리시청여부'
                ,MAX( CASE WHEN B.단편      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별단편시청여부'
                ,MAX( CASE WHEN B.뮤지컬    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬시청여부'
                ,MAX( CASE WHEN B.스포츠    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별스포츠시청여부'
                ,MAX( CASE WHEN B.공상과학  = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별공상과학시청여부'
                ,MAX( CASE WHEN B.전기      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별전기시청여부'
                ,MAX( CASE WHEN B.뉴스      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별뉴스시청여부' 
                ,AVG( B.평점)                                                                                                                AS '월별평점평균'
                ,AVG( B.판매금액)                                                                                                            AS '월별판매금액평균'
                ,SUM( B.평점)                                                                                                                AS '월별평점총합'
                ,SUM( B.판매금액)                                                                                                            AS '월별판매금액총합'
                ,SUM( CASE WHEN                               B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별시청횟수'
                ,SUM( CASE WHEN                               B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별탐색횟수'
                ,SUM( CASE WHEN                               B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별만족횟수'
                ,SUM( CASE WHEN                               B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별불만족횟수'
                ,AVG( CASE WHEN                               B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별시청평균'
                ,AVG( CASE WHEN                               B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별탐색평균'
                ,AVG( CASE WHEN                               B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별만족평균'
                ,AVG( CASE WHEN                               B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별불만족평균'
                ,AVG( CASE WHEN                               B.시청     = 1               THEN B.흥행액ORGNL ELSE 0 END)                    AS '월별시청흥행액평균'
                ,AVG( CASE WHEN                               B.시청     = 1               THEN B.예산액ORGNL ELSE 0 END)                    AS '월별시청예산액평균'
                ,SUM( CASE WHEN                               B.시청     = 1               THEN B.흥행액ORGNL ELSE 0 END)                    AS '월별시청흥행액총액'
                ,SUM( CASE WHEN                               B.시청     = 1               THEN B.예산액ORGNL ELSE 0 END)                    AS '월별시청예산액총액'
                ,SUM( CASE WHEN                               B.애니     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별애니판매금액총액'
                ,SUM( CASE WHEN                               B.드라마   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별드라마판매금액총액'
                ,SUM( CASE WHEN                               B.역사     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별역사판매금액총액'
                ,SUM( CASE WHEN                               B.코미디   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별코미디판매금액총액'
                ,SUM( CASE WHEN                               B.액션     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별액션판매금액총액'
                ,SUM( CASE WHEN                               B.범죄     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별범죄판매금액총액'
                ,SUM( CASE WHEN                               B.스릴러   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별스릴러판매금액총액'
                ,SUM( CASE WHEN                               B.다큐     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별다큐판매금액총액'
                ,SUM( CASE WHEN                               B.모험     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별모험판매금액총액'
                ,SUM( CASE WHEN                               B.판타지   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별판타지판매금액총액'
                ,SUM( CASE WHEN                               B.가족     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별가족판매금액총액'
                ,SUM( CASE WHEN                               B.로맨스   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별로맨스판매금액총액'
                ,SUM( CASE WHEN                               B.음악     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별음악판매금액총액'
                ,SUM( CASE WHEN                               B.호러     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별호러판매금액총액'
                ,SUM( CASE WHEN                               B.전쟁     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별전쟁판매금액총액'
                ,SUM( CASE WHEN                               B.서부     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별서부판매금액총액'
                ,SUM( CASE WHEN                               B.미스테리 = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별미스테리판매금액총액'
                ,SUM( CASE WHEN                               B.단편     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별단편판매금액총액'
                ,SUM( CASE WHEN                               B.뮤지컬   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별뮤지컬판매금액총액'
                ,SUM( CASE WHEN                               B.스포츠   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별스포츠판매금액총액'
                ,SUM( CASE WHEN                               B.공상과학 = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별공상과학판매금액총액'
                ,SUM( CASE WHEN                               B.전기     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별전기판매금액총액'
                ,SUM( CASE WHEN                               B.뉴스     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별뉴스판매금액총액' 
                ,AVG( CASE WHEN                               B.애니     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별애니판매금액평균'
                ,AVG( CASE WHEN                               B.드라마   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별드라마판매금액평균'
                ,AVG( CASE WHEN                               B.역사     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별역사판매금액평균'
                ,AVG( CASE WHEN                               B.코미디   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별코미디판매금액평균'
                ,AVG( CASE WHEN                               B.액션     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별액션판매금액평균'
                ,AVG( CASE WHEN                               B.범죄     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별범죄판매금액평균'
                ,AVG( CASE WHEN                               B.스릴러   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별스릴러판매금액평균'
                ,AVG( CASE WHEN                               B.다큐     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별다큐판매금액평균'
                ,AVG( CASE WHEN                               B.모험     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별모험판매금액평균'
                ,AVG( CASE WHEN                               B.판타지   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별판타지판매금액평균'
                ,AVG( CASE WHEN                               B.가족     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별가족판매금액평균'
                ,AVG( CASE WHEN                               B.로맨스   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별로맨스판매금액평균'
                ,AVG( CASE WHEN                               B.음악     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별음악판매금액평균'
                ,AVG( CASE WHEN                               B.호러     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별호러판매금액평균'
                ,AVG( CASE WHEN                               B.전쟁     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별전쟁판매금액평균'
                ,AVG( CASE WHEN                               B.서부     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별서부판매금액평균'
                ,AVG( CASE WHEN                               B.미스테리 = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별미스테리판매금액평균'
                ,AVG( CASE WHEN                               B.단편     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별단편판매금액평균'
                ,AVG( CASE WHEN                               B.뮤지컬   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별뮤지컬판매금액평균'
                ,AVG( CASE WHEN                               B.스포츠   = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별스포츠판매금액평균'
                ,AVG( CASE WHEN                               B.공상과학 = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별공상과학판매금액평균'
                ,AVG( CASE WHEN                               B.전기     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별전기판매금액평균'
                ,AVG( CASE WHEN                               B.뉴스     = 1                  THEN B.판매금액 ELSE 0 END)                    AS '월별뉴스판매금액평균'         
                ,SUM( CASE WHEN B.애니      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별애니시청횟수'
                ,SUM( CASE WHEN B.애니      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별애니탐색횟수'
                ,SUM( CASE WHEN B.드라마    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별드라마시청횟수'
                ,SUM( CASE WHEN B.드라마    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별드라마탐색횟수'
                ,SUM( CASE WHEN B.역사      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별역사시청횟수'
                ,SUM( CASE WHEN B.역사      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별역사탐색횟수'
                ,SUM( CASE WHEN B.코미디    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별코미디시청횟수'
                ,SUM( CASE WHEN B.코미디    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별코미디탐색횟수'
                ,SUM( CASE WHEN B.액션      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별액션시청횟수'
                ,SUM( CASE WHEN B.액션      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별액션탐색횟수'
                ,SUM( CASE WHEN B.범죄      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별범죄시청횟수'
                ,SUM( CASE WHEN B.범죄      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별범죄탐색횟수'
                ,SUM( CASE WHEN B.스릴러    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별스릴러시청횟수'
                ,SUM( CASE WHEN B.스릴러    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별스릴러탐색횟수'
                ,SUM( CASE WHEN B.다큐      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별다큐시청횟수'
                ,SUM( CASE WHEN B.다큐      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별다큐탐색횟수'
                ,SUM( CASE WHEN B.모험      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별모험시청횟수'
                ,SUM( CASE WHEN B.모험      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별모험탐색횟수'
                ,SUM( CASE WHEN B.판타지    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별판타지시청횟수'
                ,SUM( CASE WHEN B.판타지    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별판타지탐색횟수'
                ,SUM( CASE WHEN B.가족      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별가족시청횟수'
                ,SUM( CASE WHEN B.가족      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별가족탐색횟수'
                ,SUM( CASE WHEN B.로맨스    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별로맨스시청횟수'
                ,SUM( CASE WHEN B.로맨스    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별로맨스탐색횟수'
                ,SUM( CASE WHEN B.음악      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별음악시청횟수'
                ,SUM( CASE WHEN B.음악      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별음악탐색횟수'
                ,SUM( CASE WHEN B.호러      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별호러시청횟수'
                ,SUM( CASE WHEN B.호러      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별호러탐색횟수'
                ,SUM( CASE WHEN B.전쟁      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별전쟁시청횟수'
                ,SUM( CASE WHEN B.전쟁      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별전쟁탐색횟수'
                ,SUM( CASE WHEN B.서부      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별서부시청횟수'
                ,SUM( CASE WHEN B.서부      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별서부탐색횟수'
                ,SUM( CASE WHEN B.미스테리  = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별미스테리시청횟수'
                ,SUM( CASE WHEN B.미스테리  = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별미스테리탐색횟수'
                ,SUM( CASE WHEN B.단편      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별단편시청횟수'
                ,SUM( CASE WHEN B.단편      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별단편탐색횟수'
                ,SUM( CASE WHEN B.뮤지컬    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬시청횟수'
                ,SUM( CASE WHEN B.뮤지컬    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬탐색횟수'
                ,SUM( CASE WHEN B.스포츠    = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별스포츠시청횟수'
                ,SUM( CASE WHEN B.스포츠    = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별스포츠탐색횟수'
                ,SUM( CASE WHEN B.공상과학  = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별공상과학시청횟수'
                ,SUM( CASE WHEN B.공상과학  = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별공상과학탐색횟수'
                ,SUM( CASE WHEN B.전기      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별전기시청횟수'
                ,SUM( CASE WHEN B.전기      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별전기탐색횟수'
                ,SUM( CASE WHEN B.뉴스      = 1           AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별뉴스시청횟수'
                ,SUM( CASE WHEN B.뉴스      = 1           AND B.탐색     = 1                           THEN 1 ELSE 0 END)                    AS '월별뉴스탐색횟수'
                ,SUM( CASE WHEN B.애니      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별애니추천횟수'
                ,SUM( CASE WHEN B.애니      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별애니만족횟수'
                ,SUM( CASE WHEN B.드라마    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별드라마추천횟수'
                ,SUM( CASE WHEN B.드라마    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별드라마만족횟수'
                ,SUM( CASE WHEN B.역사      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별역사추천횟수'
                ,SUM( CASE WHEN B.역사      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별역사만족횟수'
                ,SUM( CASE WHEN B.코미디    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별코미디추천횟수'
                ,SUM( CASE WHEN B.코미디    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별코미디만족횟수'
                ,SUM( CASE WHEN B.액션      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별액션추천횟수'
                ,SUM( CASE WHEN B.액션      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별액션만족횟수'
                ,SUM( CASE WHEN B.범죄      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별범죄추천횟수'
                ,SUM( CASE WHEN B.범죄      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별범죄만족횟수'
                ,SUM( CASE WHEN B.스릴러    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별스릴러추천횟수'
                ,SUM( CASE WHEN B.스릴러    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별스릴러만족횟수'
                ,SUM( CASE WHEN B.다큐      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별다큐추천횟수'
                ,SUM( CASE WHEN B.다큐      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별다큐만족횟수'
                ,SUM( CASE WHEN B.모험      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별모험추천횟수'
                ,SUM( CASE WHEN B.모험      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별모험만족횟수'
                ,SUM( CASE WHEN B.판타지    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별판타지추천횟수'
                ,SUM( CASE WHEN B.판타지    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별판타지만족횟수'
                ,SUM( CASE WHEN B.가족      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별가족추천횟수'
                ,SUM( CASE WHEN B.가족      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별가족만족횟수'
                ,SUM( CASE WHEN B.로맨스    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별로맨스추천횟수'
                ,SUM( CASE WHEN B.로맨스    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별로맨스만족횟수'
                ,SUM( CASE WHEN B.음악      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별음악추천횟수'
                ,SUM( CASE WHEN B.음악      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별음악만족횟수'
                ,SUM( CASE WHEN B.호러      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별호러추천횟수'
                ,SUM( CASE WHEN B.호러      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별호러만족횟수'
                ,SUM( CASE WHEN B.전쟁      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별전쟁추천횟수'
                ,SUM( CASE WHEN B.전쟁      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별전쟁만족횟수'
                ,SUM( CASE WHEN B.서부      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별서부추천횟수'
                ,SUM( CASE WHEN B.서부      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별서부만족횟수'
                ,SUM( CASE WHEN B.미스테리  = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별미스테리추천횟수'
                ,SUM( CASE WHEN B.미스테리  = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별미스테리만족횟수'
                ,SUM( CASE WHEN B.단편      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별단편추천횟수'
                ,SUM( CASE WHEN B.단편      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별단편만족횟수'
                ,SUM( CASE WHEN B.뮤지컬    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬추천횟수'
                ,SUM( CASE WHEN B.뮤지컬    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬만족횟수'
                ,SUM( CASE WHEN B.스포츠    = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별스포츠추천횟수'
                ,SUM( CASE WHEN B.스포츠    = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별스포츠만족횟수'
                ,SUM( CASE WHEN B.공상과학  = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별공상과학추천횟수'
                ,SUM( CASE WHEN B.공상과학  = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별공상과학만족횟수'
                ,SUM( CASE WHEN B.전기      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별전기추천횟수'
                ,SUM( CASE WHEN B.전기      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별전기만족횟수'
                ,SUM( CASE WHEN B.뉴스      = 1           AND B.추천     = 1                           THEN 1 ELSE 0 END)                    AS '월별뉴스추천횟수'
                ,SUM( CASE WHEN B.뉴스      = 1           AND B.만족     = 1                           THEN 1 ELSE 0 END)                    AS '월별뉴스만족횟수'
                ,SUM( CASE WHEN B.애니      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별애니불만족횟수'
                ,SUM( CASE WHEN B.드라마    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별드라마불만족횟수'
                ,SUM( CASE WHEN B.역사      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별역사불만족횟수'
                ,SUM( CASE WHEN B.코미디    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별코미디불만족횟수'
                ,SUM( CASE WHEN B.액션      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별액션불만족횟수'
                ,SUM( CASE WHEN B.범죄      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별범죄불만족횟수'
                ,SUM( CASE WHEN B.스릴러    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별스릴러불만족횟수'
                ,SUM( CASE WHEN B.다큐      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별다큐불만족횟수'
                ,SUM( CASE WHEN B.모험      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별모험불만족횟수'
                ,SUM( CASE WHEN B.판타지    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별판타지불만족횟수'
                ,SUM( CASE WHEN B.가족      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별가족불만족횟수'
                ,SUM( CASE WHEN B.로맨스    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별로맨스불만족횟수'
                ,SUM( CASE WHEN B.음악      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별음악불만족횟수'
                ,SUM( CASE WHEN B.호러      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별호러불만족횟수'
                ,SUM( CASE WHEN B.전쟁      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별전쟁불만족횟수'
                ,SUM( CASE WHEN B.서부      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별서부불만족횟수'
                ,SUM( CASE WHEN B.미스테리  = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별미스테리불만족횟수'
                ,SUM( CASE WHEN B.단편      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별단편불만족횟수'
                ,SUM( CASE WHEN B.뮤지컬    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별뮤지컬불만족횟수'
                ,SUM( CASE WHEN B.스포츠    = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별스포츠불만족횟수'
                ,SUM( CASE WHEN B.공상과학  = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별공상과학불만족횟수'
                ,SUM( CASE WHEN B.전기      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별전기불만족횟수'
                ,SUM( CASE WHEN B.뉴스      = 1           AND B.만족     = 0                           THEN 1 ELSE 0 END)                    AS '월별뉴스불만족횟수'   
                ,SUM( CASE WHEN B.흥행액    = '흥행0등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별흥행0등급시청횟수'
                ,SUM( CASE WHEN B.흥행액    = '흥행1등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별흥행1등급시청횟수'
                ,SUM( CASE WHEN B.흥행액    = '흥행2등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별흥행2등급시청횟수'
                ,SUM( CASE WHEN B.흥행액    = '흥행3등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별흥행3등급시청횟수'
                ,SUM( CASE WHEN B.흥행액    = '흥행4등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별흥행4등급시청횟수'
                ,SUM( CASE WHEN B.예산액    = '예산0등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별예산0등급시청횟수'
                ,SUM( CASE WHEN B.예산액    = '예산1등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별예산1등급시청횟수'
                ,SUM( CASE WHEN B.예산액    = '예산2등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별예산2등급시청횟수'
                ,SUM( CASE WHEN B.예산액    = '예산3등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별예산3등급시청횟수'
                ,SUM( CASE WHEN B.예산액    = '예산4등급' AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별예산4등급시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '1950이전'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별1950이전시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '1965이전'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별1965이전시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '1980이전'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별1980이전시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '1999이전'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별1999이전시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '2010이전'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별2010이전시청횟수'
                ,SUM( CASE WHEN B.제작년도  = '2010이후'  AND B.시청     = 1                           THEN 1 ELSE 0 END)                    AS '월별2010이후시청횟수'
            FROM (   
                SELECT
                    DISTINCT
                    A.회원번호
                   ,A.년월                                                                                                                   AS 기준년월
                   ,A.년월일
                   ,B.역사
                   ,B.애니
                   ,B.드라마
                   ,B.코미디
                   ,B.액션
                   ,B.범죄
                   ,B.스릴러
                   ,B.다큐
                   ,B.모험
                   ,B.판타지
                   ,B.가족
                   ,B.로맨스
                   ,B.음악
                   ,B.호러
                   ,B.전쟁
                   ,B.서부
                   ,B.미스테리
                   ,B.단편
                   ,B.뮤지컬
                   ,B.스포츠
                   ,B.공상과학
                   ,B.전기
                   ,B.뉴스
                   ,A.제작년도ORGNL
                   ,A.제작년도
                   ,A.흥행액ORGNL
                   ,A.흥행액
                   ,A.예산액ORGNL
                   ,A.예산액
                   ,A.시청
                   ,A.만족
                   ,A.탐색
                   ,A.추천
                   ,A.평점
                   ,A.판매금액 
                FROM (
                    SELECT
                        DISTINCT
                        A.회원번호
                       ,A.영화코드
                       ,A.년월
                       ,A.년월일
                       ,B.제작년도ORGNL
                       ,B.제작년도
                       ,B.흥행액ORGNL
                       ,B.흥행액
                       ,B.예산액ORGNL
                       ,B.예산액
                       ,A.시청
                       ,A.만족
                       ,A.탐색
                       ,A.추천
                       ,A.평점  
                       ,A.판매금액 
                    FROM (
                        SELECT
                            회원번호
                           ,영화코드
                           ,년월
                           ,년월일
                           ,Max(시청)                                                                                                        AS 시청
                           ,Max(만족)                                                                                                        AS 만족
                           ,Max(탐색)                                                                                                        AS 탐색
                           ,Max(추천)                                                                                                        AS 추천
                           ,Max(평점)                                                                                                        AS 평점
                           ,Max(판매금액)                                                                                                    AS 판매금액
                        FROM(
                            SELECT
                                회원번호
                               ,영화코드
                               ,년월
                               ,년월일
                               ,CASE WHEN 활동코드 in  (11,4,2,1) THEN 1 ELSE 0 END                                                          AS 시청
                               ,CASE WHEN 활동코드 in  (5) THEN 1 ELSE 0 END                                                                 AS 탐색
                               ,CASE WHEN 평점     in  (3,4,5)    THEN 1 ELSE 0 END                                                          AS 만족
                               ,추천여부                                                                                                     AS 추천
                               ,평점
                               ,판매금액
                            FROM MOVIE_FACT   
                        )
                        GROUP BY 회원번호, 영화코드, 년월, 년월일           
                    )                                                                                                                        AS A                                                                          
                    LEFT JOIN (
                        SELECT
                            영화코드
                           ,제작년도                                                                                                         AS 제작년도ORGNL
                           ,CASE
                                WHEN 
                                    CAST(제작년도 AS INT) >= 1900 and CAST(제작년도 AS INT)<1950 
                                THEN '1950이전'
                                WHEN 
                                    CAST(제작년도 AS INT) >= 1950 and CAST(제작년도 AS INT)<1965 
                                THEN '1965이전'  
                                WHEN 
                                    CAST(제작년도 AS INT) >= 1965 and CAST(제작년도 AS INT)<1980 
                                THEN '1980이전'     
                                WHEN 
                                    CAST(제작년도 AS INT) >= 1980 and CAST(제작년도 AS INT)<1999
                                THEN '1999이전'   
                                WHEN 
                                    CAST(제작년도 AS INT) >= 1999 and CAST(제작년도 AS INT)<2010
                                THEN '2010이전'         
                                WHEN 
                                    CAST(제작년도 AS INT) >= 2010 and CAST(제작년도 AS INT)<2020
                                THEN '2010이후'
                                    
                                ELSE '미상'
                            END                                                                                                              AS 제작년도
                           ,흥행액                                                                                                           AS 흥행액ORGNL
                           ,CASE
                                WHEN 
                                    CAST(흥행액 AS INT) >=         0 and CAST(흥행액 AS INT)<             1000
                                THEN '흥행액0등급'
                                WHEN 
                                    CAST(흥행액 AS INT) >=      1000 and CAST(흥행액 AS INT)<          5000000
                                THEN '흥행액1등급'
                                WHEN 
                                    CAST(흥행액 AS INT) >=   5000000 and CAST(흥행액 AS INT)<         25000000
                                THEN '흥행액2등급'
                                WHEN 
                                    CAST(흥행액 AS INT) >=  25000000 and CAST(흥행액 AS INT)<        130000000
                                THEN '흥행액3등급'
                                WHEN 
                                    CAST(흥행액 AS INT) >= 130000000 and CAST(흥행액 AS INT)< 3000000000000000
                                THEN '흥행액4등급'
                            END                                                                                                              AS 흥행액
                           ,예산액                                                                                                           AS 예산액ORGNL
                           ,CASE
                                WHEN 
                                    CAST(예산액 AS INT) >=         0 and CAST(예산액 AS INT)<             1000
                                THEN '예산액0등급'
                                WHEN 
                                    CAST(예산액 AS INT) >=      1000 and CAST(예산액 AS INT)<          5000000
                                THEN '예산액1등급'
                                WHEN 
                                    CAST(예산액 AS INT) >=   5000000 and CAST(예산액 AS INT)<         25000000
                                THEN '예산액2등급'
                                WHEN 
                                    CAST(예산액 AS INT) >=  25000000 and CAST(예산액 AS INT)<        130000000
                                THEN '예산액3등급'
                                WHEN 
                                    CAST(예산액 AS INT) >= 130000000 and CAST(예산액 AS INT)< 3000000000000000
                                THEN '예산액4등급'
                            END                                                                                                              AS 예산액
                        FROM MOVIE
                    )                                                                                                                        AS B
                        ON A.영화코드=B.영화코드      
                )                                                                                                                            AS A
                LEFT JOIN (
                    SELECT
                        영화코드
                       ,MAX(역사)                                                                                                            AS 역사
                       ,MAX(애니)                                                                                                            AS 애니
                       ,MAX(드라마)                                                                                                          AS 드라마
                       ,MAX(코미디)                                                                                                          AS 코미디
                       ,MAX(액션)                                                                                                            AS 액션
                       ,MAX(범죄)                                                                                                            AS 범죄
                       ,MAX(스릴러)                                                                                                          AS 스릴러
                       ,MAX(다큐)                                                                                                            AS 다큐
                       ,MAX(모험)                                                                                                            AS 모험
                       ,MAX(판타지)                                                                                                          AS 판타지
                       ,MAX(가족)                                                                                                            AS 가족
                       ,MAX(로맨스)                                                                                                          AS 로맨스
                       ,MAX(음악)                                                                                                            AS 음악
                       ,MAX(호러)                                                                                                            AS 호러
                       ,MAX(전쟁)                                                                                                            AS 전쟁
                       ,MAX(서부)                                                                                                            AS 서부
                       ,MAX(미스테리)                                                                                                        AS 미스테리
                       ,MAX(단편)                                                                                                            AS 단편
                       ,MAX(뮤지컬)                                                                                                          AS 뮤지컬
                       ,MAX(스포츠)                                                                                                          AS 스포츠
                       ,MAX(공상과학)                                                                                                        AS 공상과학
                       ,MAX(전기)                                                                                                            AS 전기
                       ,MAX(뉴스)                                                                                                            AS 뉴스
                    FROM(
                        SELECT
                            A.영화코드
                           ,CASE WHEN A.장르코드 =   1        THEN 1 ELSE 0 END                                                              AS 역사
                           ,CASE WHEN A.장르코드 =   2        THEN 1 ELSE 0 END                                                              AS 애니
                           ,CASE WHEN A.장르코드 =   3        THEN 1 ELSE 0 END                                                              AS 드라마
                           ,CASE WHEN A.장르코드 =   6        THEN 1 ELSE 0 END                                                              AS 코미디
                           ,CASE WHEN A.장르코드 =   7        THEN 1 ELSE 0 END                                                              AS 액션
                           ,CASE WHEN A.장르코드 =   8        THEN 1 ELSE 0 END                                                              AS 범죄
                           ,CASE WHEN A.장르코드 =   9        THEN 1 ELSE 0 END                                                              AS 스릴러
                           ,CASE WHEN A.장르코드 =  10        THEN 1 ELSE 0 END                                                              AS 다큐
                           ,CASE WHEN A.장르코드 =  11        THEN 1 ELSE 0 END                                                              AS 모험
                           ,CASE WHEN A.장르코드 =  12        THEN 1 ELSE 0 END                                                              AS 판타지
                           ,CASE WHEN A.장르코드 =  14        THEN 1 ELSE 0 END                                                              AS 가족
                           ,CASE WHEN A.장르코드 =  15        THEN 1 ELSE 0 END                                                              AS 로맨스
                           ,CASE WHEN A.장르코드 =  16        THEN 1 ELSE 0 END                                                              AS 음악
                           ,CASE WHEN A.장르코드 =  17        THEN 1 ELSE 0 END                                                              AS 호러
                           ,CASE WHEN A.장르코드 =  18        THEN 1 ELSE 0 END                                                              AS 전쟁
                           ,CASE WHEN A.장르코드 =  19        THEN 1 ELSE 0 END                                                              AS 서부
                           ,CASE WHEN A.장르코드 =  20        THEN 1 ELSE 0 END                                                              AS 미스테리
                           ,CASE WHEN A.장르코드 =  24        THEN 1 ELSE 0 END                                                              AS 단편
                           ,CASE WHEN A.장르코드 =  25        THEN 1 ELSE 0 END                                                              AS 뮤지컬
                           ,CASE WHEN A.장르코드 =  30        THEN 1 ELSE 0 END                                                              AS 스포츠
                           ,CASE WHEN A.장르코드 =  45        THEN 1 ELSE 0 END                                                              AS 공상과학
                           ,CASE WHEN A.장르코드 in (46, 53)  THEN 1 ELSE 0 END                                                              AS 전기
                           ,CASE WHEN A.장르코드 in (47, 51)  THEN 1 ELSE 0 END                                                              AS 뉴스
                        FROM MOVIE_GENRE                                                                                                     AS A

                    )
                    GROUP BY 영화코드
                )                                                                                                                            AS B
                    ON A.영화코드 = B.영화코드
                
            )                                                                                                                            AS B
            WHERE B.기준년월 = {YYYYMM}
                GROUP BY B.회원번호  
      
        )                                                                                                                                    AS B
            ON A.회원번호 = B.회원번호
    '''


    cur.execute(f"""delete from {input_str} where 기준년월 = {YYYYMM}""")
    cur.execute(f"""
        INSERT INTO {input_str} 
            {sql_ftr_mart}
        
    """
    )
print(pd.read_sql('''PRAGMA table_info(FTRMART)''',conn))



######################################################################################################################## 
###### 4차 타겟마트 생성 및 적재
########################################################################################################################


####################################################################################################
#### 타겟마트 생성 및 데이터 적재 - 기준년월 기준, 해당월 범죄장르 시청여부 기준 타겟마트 생성
####################################################################################################

input_str='TRGTMART'
cur.execute(f"""drop table if exists {input_str}""")
cur.execute(f"""
    create table if not exists {input_str} (
         기준년월                                                     TEXT
        ,회원번호                                                     TEXT
        ,타겟년월                                                     TEXT
        ,Y_애니                                                       INT
        ,Y_드라마                                                     INT
        ,Y_역사                                                       INT
        ,Y_코미디                                                     INT
        ,Y_액션                                                       INT
        ,Y_범죄                                                       INT
        ,Y_스릴러                                                     INT
        ,Y_다큐                                                       INT
        ,Y_모험                                                       INT
        ,Y_판타지                                                     INT
        ,Y_가족                                                       INT
        ,Y_로맨스                                                     INT
        ,Y_음악                                                       INT
        ,Y_호러                                                       INT
        ,Y_전쟁                                                       INT
        ,Y_서부                                                       INT
        ,Y_미스테리                                                   INT
        ,Y_단편                                                       INT
        ,Y_뮤지컬                                                     INT
        ,Y_스포츠                                                     INT
        ,Y_공상과학                                                   INT
        ,Y_전기                                                       INT
        ,Y_뉴스                                                       INT
        ,나이                                                         INT
        ,성별                                                         TEXT
        ,결혼여부                                                     TEXT
        ,PRIMARY KEY(기준년월, 회원번호, 성별, 결혼여부) 
    )
"""



for idx, YYYYMMBSE in enumerate(list_YYYYMM):
    YYYYMMTRGT = datetime.strftime(datetime.strptime(YYYYMMBSE, '%Y%m') + relativedelta(months = 1), '%Y%m')
    sql_trgt_mart= f'''
        SELECT
             {YYYYMMBSE}                                                                                          AS 기준년월
            ,회원번호                                                                                                           
            ,{YYYYMMTRGT}                                                                                         AS 타겟년월                                                                                         
            ,( CASE WHEN  월별애니시청여부        = 1  THEN 월별애니시청여부      ELSE 0 END)                     AS Y_애니     
            ,( CASE WHEN  월별드라마시청여부      = 1  THEN 월별드라마시청여부    ELSE 0 END)                     AS Y_드라마 
            ,( CASE WHEN  월별역사시청여부        = 1  THEN 월별역사시청여부      ELSE 0 END)                     AS Y_역사 
            ,( CASE WHEN  월별코미디시청여부      = 1  THEN 월별코미디시청여부    ELSE 0 END)                     AS Y_코미디 
            ,( CASE WHEN  월별액션시청여부        = 1  THEN 월별액션시청여부      ELSE 0 END)                     AS Y_액션 
            ,( CASE WHEN  월별범죄시청여부        = 1  THEN 월별범죄시청여부      ELSE 0 END)                     AS Y_범죄 
            ,( CASE WHEN  월별스릴러시청여부      = 1  THEN 월별스릴러시청여부    ELSE 0 END)                     AS Y_스릴러 
            ,( CASE WHEN  월별다큐시청여부        = 1  THEN 월별다큐시청여부      ELSE 0 END)                     AS Y_다큐 
            ,( CASE WHEN  월별모험시청여부        = 1  THEN 월별모험시청여부      ELSE 0 END)                     AS Y_모험 
            ,( CASE WHEN  월별판타지시청여부      = 1  THEN 월별판타지시청여부    ELSE 0 END)                     AS Y_판타지 
            ,( CASE WHEN  월별가족시청여부        = 1  THEN 월별가족시청여부      ELSE 0 END)                     AS Y_가족 
            ,( CASE WHEN  월별로맨스시청여부      = 1  THEN 월별로맨스시청여부    ELSE 0 END)                     AS Y_로맨스 
            ,( CASE WHEN  월별음악시청여부        = 1  THEN 월별음악시청여부      ELSE 0 END)                     AS Y_음악 
            ,( CASE WHEN  월별호러시청여부        = 1  THEN 월별호러시청여부      ELSE 0 END)                     AS Y_호러 
            ,( CASE WHEN  월별전쟁시청여부        = 1  THEN 월별전쟁시청여부      ELSE 0 END)                     AS Y_전쟁 
            ,( CASE WHEN  월별서부시청여부        = 1  THEN 월별서부시청여부      ELSE 0 END)                     AS Y_서부 
            ,( CASE WHEN  월별미스테리시청여부    = 1  THEN 월별미스테리시청여부  ELSE 0 END)                     AS Y_미스테리 
            ,( CASE WHEN  월별단편시청여부        = 1  THEN 월별단편시청여부      ELSE 0 END)                     AS Y_단편 
            ,( CASE WHEN  월별뮤지컬시청여부      = 1  THEN 월별뮤지컬시청여부    ELSE 0 END)                     AS Y_뮤지컬 
            ,( CASE WHEN  월별스포츠시청여부      = 1  THEN 월별스포츠시청여부    ELSE 0 END)                     AS Y_스포츠 
            ,( CASE WHEN  월별공상과학시청여부    = 1  THEN 월별공상과학시청여부  ELSE 0 END)                     AS Y_공상과학 
            ,( CASE WHEN  월별전기시청여부        = 1  THEN 월별전기시청여부      ELSE 0 END)                     AS Y_전기 
            ,( CASE WHEN  월별뉴스시청여부        = 1  THEN 월별뉴스시청여부      ELSE 0 END)                     AS Y_뉴스         
            ,나이
            ,성별
            ,결혼여부
        FROM FTRMART
        WHERE 기준년월 = '{YYYYMMTRGT}'
    '''
    cur.execute(f"""delete from {input_str} where 기준년월 = '{YYYYMMTRGT}' """)
    cur.execute(f"""
        INSERT INTO {input_str}  
            {sql_trgt_mart}
        
    """
    )
pd.read_sql('''PRAGMA table_info(TRGTMART)''',conn)


