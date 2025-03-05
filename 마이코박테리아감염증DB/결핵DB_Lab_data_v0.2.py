import pandas as pd
import numpy as np

# 파일 경로
measurement_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\measurement.csv"
condition_occurrence_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\condition_occurrence(regtime추가).csv"

# 감수성 검사 코드
dst_code = ["L440502"] 

# Lab 검사 코드 매핑
lab_code = {
    "WBC": ["L200201", "L700101"],  # 진단혈액   # "L200101(일반검사)", "L700101(응급일반검사)" , L100203, L2000301, L602101, L603101
    "Hemoglobin": ["L200203", "L700103"],    # 진단혈액, L602103   
    "Platelet": ["L200209", "L700109"],  # 진단혈액, L602109, L2003209
    "ESR": ["L2005"],   # 진단혈액
    "CRP": ["L3052", "L5108", "L5109", "LTCRP", "L305299"],     # 임상화학
    "Glucose": ["L3033", "L7115"],    # 임상화학  # "L7115(응급화학검사)", L100206, L100206(값: +/-),L700206(값 : +/-) 
    "Albumin": ["L3021", "L7121"],   # 임상화학, 응급화학
    "Protein": ["L3020", "L7120"],   # 임상화학, 응급화학
    "Total_bilirubin": ["L3018", "L7117"],   # 임상화학, 응급화학
    "AST": ["L3015", "L7118"],  # 임상화학, 응급화학  
    "ALT": ["L3016", "L7119"],  # 임상화학, 응급화학
    "BUN": ["L3024", "L7116"],  # 임상화학, 응급화학
    "Creatinine": ["L3025", "L3025C", "L7125"],  # 임상화학, 응급화학
    "Uric_acid": ["L100312", "L2366D", "L3026", "L39040F", "L39040Q", "L700312", "L7139", "L802806", "X000506", "X000604"], # 임상화학, 응급화학
    "Vitamin_D": ["L32731", "N205030", "N205085"],  # 중금속, 미량원소 
    "PT": ["L210201"],  # 혈액응고
    "aPTT": ["L2103"]   # 혈액응고
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
    
    # 진단일과 가장 가까운 검사만 선택
    measurement_Lab = measurement_Lab[measurement_Lab["measurement_date"] <= measurement_Lab["NTM진단일"]]
    # measurement_Lab = measurement_Lab.sort_values(by=["person_id", "measurement_source_value", "measurement_date"], ascending=[True, True, False])
    # measurement_Lab = measurement_Lab.groupby(["person_id", "measurement_source_value"]).first().reset_index()

    # 청크 결과 저장
    measurement_Lab_list.append(measurement_Lab)

# 모든 청크 결과 병합   
measurement_Lab = pd.concat(measurement_Lab_list, ignore_index=True)
measurement_Lab = measurement_Lab.sort_values(by=["person_id", "measurement_date"], ascending=[True, False]) # measurement_source_value
measurement_Lab = measurement_Lab.groupby(["person_id", "measurement_source_value"]).first().reset_index()

measurement_Lab.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_A31_lab_3.csv", index=False)

# 검사 결과값을 가로 방향으로 변환 (value 기준)
measurement_values = measurement_Lab.pivot(index="person_id", columns="measurement_source_value", values="value_source_value").reset_index()

# 검사 날짜를 가로 방향으로 변환 (measurement_date 기준)
measurement_dates = measurement_Lab.pivot(index="person_id", columns="measurement_source_value", values="measurement_date").reset_index()

# 컬럼명 정리 (날짜 컬럼은 '_date' 접미사 추가)
measurement_dates = measurement_dates.add_suffix("_date")
measurement_dates.rename(columns={"person_id_date": "person_id"}, inplace=True)

# 결과 병합 (검사값 + 측정일)
measurement_wide = pd.merge(measurement_values, measurement_dates, on="person_id", how="left") 
measurement_A31_lab = pd.merge(condition_A31, measurement_wide, on= "person_id", how= "left")


#
date_columns = [
    "ALT_date", "AST_date", "BUN_date", "CRP_date", "ESR_date",
    "WBC_date", "Platelet_date", "Glucose_date", "Hemoglobin_date",
    "Protein_date", "Albumin_date", "Creatinine_date",
    "Total_bilirubin_date", "Uric_acid_date", "Vitamin_D_date", "PT_date", "aPTT_date"
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


# 결과 출력
measurement_A31_lab.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_A31_lab_수정.csv", index=False)
