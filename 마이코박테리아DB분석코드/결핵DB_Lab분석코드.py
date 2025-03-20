# 결측치 분석 코드
import pandas as pd
import numpy as np

lab_1M_quick = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_1개월이내_가까운날짜.csv")
lab_1M_quick_same = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_1개월이내_가까운날짜_동일날짜.csv")
lab_3M_quick = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_3개월이내_가까운날짜.csv")
lab_3M_quick_same = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_3개월이내_가까운날짜_동일날짜.csv")
lab_6M_quick = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_6개월이내_가까운날짜.csv")
lab_6M_quick_same = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\measurement_A31_lab_유효기간미적용_6개월이내_가까운날짜_동일날짜.csv")

# 사용할 데이터프레임 이름 목록
lab_quick = ["lab_1M_quick", "lab_3M_quick", "lab_6M_quick"]
lab_same = ["lab_1M_quick_same", "lab_3M_quick_same", "lab_6M_quick_same"]

# 데이터 출력 결과
test_columns = [
    "WBC", "HEMOGLOBIN", "PLATELET", "ESR", "CRP", "GLUCOSE",
    "ALBUMIN", "TOTAL_PROTEIN", "TOTAL_BILIRUBIN", "AST", "ALT",
    "BUN", "CREATININE", "URIC_ACID", "VITAMIN_D", "PT", "APTT"
]

# 전체 환자 수 
total_patients = 2490

# 검사항목별 환자 수를 저장할 딕셔너리
test_patient_counts_quick = {}
test_patient_counts_same = {}

# 1. lab_quick 데이터프레임 처리 (measurement_source_value 기준 그룹화)
for name in lab_quick:
    if name in globals():  # 데이터프레임이 존재하는지 확인
        df = globals()[name]  # 데이터프레임 할당
        
        # 검사항목별 환자 수 계산
        test_patient_counts_quick[name] = df.groupby("measurement_source_value")["person_id"].nunique()

        # 원하는 순서대로 정렬 (없는 항목은 제외)
        test_patient_counts_quick[name] = test_patient_counts_quick[name].reindex(test_columns).dropna()

        # 추출률(%) 계산
        test_patient_counts_quick[name + "_rate"] = ((test_patient_counts_quick[name] / total_patients) * 100).round(1)

# 2. lab_same 데이터프레임 처리 (각 항목별로 결측값이 없는 환자 수 계산)
for name in lab_same:
    if name in globals():
        df = globals()[name]  # 데이터프레임 할당
        
        # 각 항목별로 값이 존재하는 환자 수 계산
        test_patient_counts_same[name] = {test: df[df[test].notna()]["person_id"].nunique() for test in test_columns}

        # 추출률(%) 계산 (딕셔너리 값마다 계산)
        test_patient_counts_same[name + "_rate"] = {test: round((test_patient_counts_same[name][test] / total_patients) * 100, 1) for test in test_columns}

# 3. 여러 데이터프레임의 결과를 하나의 표로 결합
quick_df = pd.DataFrame(test_patient_counts_quick)
same_df = pd.DataFrame(test_patient_counts_same)

# 4. 컬럼명 변경 (가독성 향상)
# result_quick_df.columns = [f"{col}_quick_patients" for col in result_quick_df.columns]
# result_same_df.columns = [f"{col}_same_patients" for col in result_same_df.columns]

quick_df.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\진단일이전_1_3_6개월.csv")
same_df.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\진단일이전_동일날짜_1_3_6개월.csv")