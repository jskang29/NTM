import pandas as pd
import numpy as np

# 파일 경로
# measurement_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\measurement.csv"
measurement_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_Lab\\measurement_stexmrst.csv"
condition_occurrence_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\condition_occurrence(regtime추가).csv"

# Lab 검사 코드 매핑
lab_code = {
    "WBC": ["L200201", "L2003201", "L700101"],
    "HEMOGLOBIN": ["L200203", "L2003203", "L700103"],
    "PLATELET": ["L200209", "L2003209", "L700109"],
    "ESR": ["L2005"],
    "CRP": ["L3052"],
    "GLUCOSE": ["L3033", "L303301", "L7115"],
    "ALBUMIN": ["L3021", "L321107", "L7121"],
    "TOTAL_PROTEIN": ["L3020", "L7120"],
    "TOTAL_BILIRUBIN": ["L3018"],
    "AST": ["L3015", "L7118"],
    "ALT": ["L3016", "L7119"],
    "BUN": ["L3024", "L7116"],
    "CREATININE": ["L3025", "L3025C", "L7125"],
    "URIC_ACID": ["L3026"],
    "VITAMIN_D": ["L32731"],
    "PT": ["L210201"],
    "APTT": ["L2103", "L210301", "L7009"]
}


# Lab 코드 변환 함수
def map_lab_code(measurement_source_value):
    for key, values in lab_code.items():
        if measurement_source_value in values:
            return key
    return measurement_source_value

# condition_occurrence 데이터 로드
condition_occurrence = pd.read_csv(condition_occurrence_path)
condition_occurrence = condition_occurrence[["person_id", "condition_start_date", "condition_source_value"]]

# A31 진단 데이터 필터링
condition_A31 = condition_occurrence[condition_occurrence["condition_source_value"].str.startswith("A31", na=False)]
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True, True])
condition_A31 = condition_A31.drop_duplicates(subset="person_id", ignore_index=True)
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

# Measurement 데이터를 청크 단위로 처리
chunk_size = 100000
lab_values = [value for values in lab_code.values() for value in values] 
'''
lab_values = []
for values in lab_code.values():  # 딕셔너리의 값(리스트)을 하나씩 가져옴
    for value in values:  # 그 리스트 내부의 요소를 하나씩 가져옴
        lab_values.append(value)  # 리스트에 추가
'''
# 결과 저장 리스트
measurement_Lab_list = []

# 청크 단위로 measurement 데이터 읽기
for chunk in pd.read_csv(measurement_path, usecols=["person_id", "measurement_date", "measurement_source_value", "value_source_value"], chunksize=chunk_size):
    # measurement_source_value가 Lab 검사 코드에 포함되는 경우만 필터링
    chunk = chunk[chunk["measurement_source_value"].astype(str).isin(lab_values)]
    
    # Lab 검사명을 매핑(검사코드를 검사명으로 변환)
    chunk["measurement_source_value"] = chunk["measurement_source_value"].apply(map_lab_code)
    
    # NTM 진단 데이터와 병합
    measurement_Lab = pd.merge(condition_A31, chunk, on="person_id", how="left")
    
    # 진단일 이전에 시행한 검사만 선택
    # measurement_Lab = measurement_Lab[measurement_Lab["measurement_date"] <= measurement_Lab["NTM진단일"]]
    # measurement_Lab = measurement_Lab.sort_values(by=["person_id", "measurement_source_value", "measurement_date"], ascending=[True, True, False])
    # measurement_Lab = measurement_Lab.groupby(["person_id", "measurement_source_value"]).first().reset_index()

    # 청크 결과 저장
    measurement_Lab_list.append(measurement_Lab)

# 모든 청크 결과 병합   
measurement_Lab = pd.concat(measurement_Lab_list, ignore_index=True)

# 검사 결과 추출_1
measurement_Lab.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_A31_lab_진단일무관.csv", index=False)

# 1) 진단일 이전 데이터 전처리
# 1-1) 진단일 이전 데이터 정렬 (person_id 기준 오름차순, 날짜 기준 내림차순)
before_diagnosis = measurement_Lab[measurement_Lab["measurement_date"] < measurement_Lab["NTM진단일"]]
before_diagnosis = before_diagnosis.sort_values(by=["person_id", "measurement_date"], ascending=[True, False])

# 1-2) 검사결과 중 진단일과 가장 가까운 날짜의 검사 출력
before_diagnosis = before_diagnosis.groupby(["person_id", "measurement_source_value"]).first().reset_index()

# 2) 진단일 이후 데이터 전처리
# 2-1) 진단일 이후 데이터 정렬 (person_id 기준 오름차순, 날짜 기준 오름차순)
after_diagnosis = measurement_Lab[measurement_Lab["measurement_date"] >= measurement_Lab["NTM진단일"]]
after_diagnosis = after_diagnosis.sort_values(by=["person_id", "measurement_date"], ascending=[True, True])

# 2-2) 검사결과 중 진단일과 가장 가까운 날짜의 검사 출력
after_diagnosis = after_diagnosis.groupby(["person_id", "measurement_source_value"]).first().reset_index()

# 두 데이터 병합(after, before)
measurement_Lab = pd.concat([before_diagnosis, after_diagnosis]).reset_index(drop=True)
measurement_Lab = measurement_Lab.sort_values(by=["person_id"], ascending= True)

# 검사 결과 추출_2
measurement_Lab.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_A31_lab_진단일무관_가까운날짜.csv", index=False)

# 검사 결과 피벗 (value 기준)
before_values = before_diagnosis.pivot_table(index="person_id", columns="measurement_source_value", values="value_source_value", aggfunc="first").reset_index()
after_values = after_diagnosis.pivot_table(index="person_id", columns="measurement_source_value", values="value_source_value", aggfunc="first").reset_index()

# 검사 날짜 피벗 (measurement_date 기준)
before_dates = before_diagnosis.pivot_table(index="person_id", columns="measurement_source_value", values="measurement_date", aggfunc="first").reset_index()
after_dates = after_diagnosis.pivot_table(index="person_id", columns="measurement_source_value", values="measurement_date", aggfunc="first").reset_index()

# 날짜 컬럼명 정리 (before, after 구분)
before_dates = before_dates.add_suffix("_date")
after_dates = after_dates.add_suffix("_date")
before_dates.rename(columns={"person_id_date": "person_id"}, inplace=True)
after_dates.rename(columns={"person_id_date": "person_id"}, inplace=True)

# 4) 검사 결과 병합
measurement_wide_before = pd.merge(before_values, before_dates, on="person_id", how="left")
measurement_wide_after = pd.merge(after_values, after_dates, on="person_id", how="left")

# 5) before & after 최종 병합
measurement_final = pd.concat([measurement_wide_before, measurement_wide_after]).reset_index(drop=True)
measurement_A31_lab = pd.merge(condition_A31, measurement_final, on= "person_id", how= "left")

# 날짜 컬럼명
date_columns = [    
    "ALT_date", "AST_date", "BUN_date", "CRP_date", "ESR_date",
    "WBC_date", "PLATELET_date", "GLUCOSE_date", "HEMOGLOBIN_date",
    "TOTAL_PROTEIN_date", "ALBUMIN_date", "CREATININE_date",
    "TOTAL_BILIRUBIN_date", "URIC_ACID_date", "VITAMIN_D_date", "PT_date", "APTT_date"
]

# 날짜 데이터 변환
measurement_A31_lab["NTM진단일"] = pd.to_datetime(measurement_A31_lab["NTM진단일"], errors="coerce")

for col in date_columns:
    measurement_A31_lab[col] = pd.to_datetime(measurement_A31_lab[col], errors="coerce")


# 각 행에 대해 가장 가까운 날짜를 찾아 유지하는 함수
def keep_closest_date(row):
    ntm_date = row["NTM진단일"]
    
    # 진단일과의 차이를 계산 (NaT 값은 제외)
    closest_col = min(
        date_columns,
        key=lambda col: abs((row[col] - ntm_date).days) if pd.notna(row[col]) else float("inf")
    )
    
    # 가장 가까운 날짜값 저장
    closest_value = row[closest_col]
    
    # 모든 날짜 컬럼을 NaN으로 만들고, 가장 가까운 날짜만 유지
    for col in date_columns:
        if row[col] != closest_value:
            row[col] = np.nan
            
    return row

# 적용
measurement_A31_lab = measurement_A31_lab.apply(keep_closest_date, axis=1)

# 모든 검사 컬럼에 대해 날짜 컬럼이 존재하면 값 유지, 그렇지 않으면 NaN 처리
for lab in lab_code.keys():
    date_col = f"{lab}_date"
    value_col = lab
    
    # 날짜 값이 존재하면 검사 결과 유지, 그렇지 않으면 NaN 설정
    measurement_A31_lab[value_col] = np.where(
        measurement_A31_lab[date_col].notna(), measurement_A31_lab[value_col], np.nan
    )

value_columns = list(lab_code.keys())

# 숫자가 아닌 값은 NaN으로 변환
# for col in value_columns:
#    measurement_A31_lab[col] = pd.to_numeric(measurement_A31_lab[col], errors="coerce")


# "검사일자" 컬럼에 첫 번째로 발견된 비어있지 않은 날짜 값을 저장
measurement_A31_lab["시행날짜"] = measurement_A31_lab[date_columns].bfill(axis=1).iloc[:, 0]

# 결과 출력
measurement_A31_lab.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_A31_lab_진단일무관_가까운날짜_동일날짜.csv", index=False)
