import pandas as pd  # 기저질환 보유자. 협심증(I20), 당뇨병(E10 ~ E14) 

condition_occurrence = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\sample_data\\condition_occurrence.csv")


# 데이터가 모두 기록되어 있는지 확인
# 1. NTM 초진일 구하기
# 1-1) NTM 상병입력일 추출(A31 상병입력일 추출)
condition_comorbid = condition_occurrence[["person_id", "condition_start_date", "condition_source_value", "진단명", "상병구분"]]
condition_A31 = condition_comorbid[condition_comorbid["condition_source_value"].str.startswith("A31", na= False)]

# 1-2) NTM 첫 상병입력일 추출(person_id를 기준으로 중복 제거)
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True,True])
condition_A31 = condition_A31.drop_duplicates(subset= "person_id", ignore_index= True)

# 1-3) 컬럼명 A31_start_date로 변경
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]

# 1-4) 환자별 A31 초진일만 별도의 컬럼으로 구성
condition_A31 = condition_A31[["person_id", "NTM진단일"]]


# 타병원에서 당뇨를 진단받은 경력은 전북대병원 EMR에 반영 안됨, 상병이력 외에 생각해볼 부분은??
 
# 기저질환 보유여부

conditions = [
    {"상병코드": ["E10", "E11", "E12", "E13", "E14", "O24"], "초진일": "당뇨"},
    {"상병코드": ["I10", "I150", "I151", "I152"], "초진일": "고혈압"},
    {"상병코드": ["I21", "I22", "I252"], "초진일": "심근경색"},  
    {"상병코드": ["I20"], "초진일": "협심증"},
    {"상병코드": ["I50"], "초진일": "심부전"},
    {"상병코드": ["A504", "B220", "F00", "F01", "F02", "F03", "F107", "F117", "F127", "F137", "F147", "F157", "F167", "F177", "F187", "F197", "G3100", "G3182", "F028"], "초진일": "치매"},
    {"상병코드": ["N18"], "초진일": "만성신질환"},
    {"상병코드": ["K700", "K701", "K702", "K703", "K760", "K758", "B180", "B181", "B182", "E880", "E830", "E831", "K754", "K743", "K830", "I820"], "초진일": "만성간질환"},
    {"상병코드": ["J440", "J441", "J449"], "초진일": "COPD"},
    {"상병코드": ["J448", "J45", "J46", "J82"], "초진일": "천식"},
    {"상병코드": ["J47"], "초진일": "기관지확장증"},    # A15, A16 원발성만 포함할지 여부 확인 필요
    {"상병코드": ["B441", "B440"], "초진일": "CPA"},    # CPA에 대한 별도 코드 존재하지 않음
    {"상병코드": ["B448"], "초진일": "ABPA"},           # ABPA에 대한 별도 코드 존재하지 않음
    {"상병코드": ["K21"], "초진일": "GERD"},
    {"상병코드": ["M05"], "초진일": "류마티스"},         # 지나치게 광범위함, 범위에 대한 구체적인 설정 필요
    {"상병코드": ["C", "D0"], "초진일": "악성종양"},
    ]

# 반복 작업 수행
for cond in conditions:
    temp_df = condition_comorbid[
        condition_comorbid["condition_source_value"].str.startswith(tuple(cond["상병코드"]), na=False)
        ]

    temp_df = temp_df.sort_values(by=["person_id", "condition_start_date"], ascending=[True, True])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
    
    # 새로운 컬럼 생성 및 필요한 컬럼만 유지
    temp_df[cond["초진일"]] = temp_df["condition_start_date"]
    temp_df = temp_df[["person_id", cond["초진일"]]]
    
    # 원래 데이터프레임과 병합
    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")

condition_A31.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\기저질환.csv", index=False)


# NTM진단일이 기저질환 진단일보다 빠른 경우



























