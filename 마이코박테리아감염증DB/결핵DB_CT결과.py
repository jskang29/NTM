# procedure_occurrence의 finding과 conclusion(결론 및 진단) DB확인
# 빈칸은 전부 온점(.)으로 찍히므로 대쉬(_)로 변경
# 6min walking : S40018, S40099 --> 신체정보 추출 가능여부 확인
# B.D.R --> FEV1, FEV1%, FVC, FVC%, DLCO 추출 가능여부 확인
# ctrl shift L : 같은 단어 선택
import pandas as pd 
import re

# person = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\샘플CDMV2_20241211\\person.csv")
procedure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\샘플CDMV2_20241211\\procedure_occurrence.csv")
condition_A31 = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\기저질환.csv")

# 치료시작일 코드필요(추후 작성)

radiology = [
    {"검사코드": ["RC121", "RC194", "RC314", "RC363", "RC561", "RC994", "RC131", "RC200"], "검사명": "chestCT", "항목": "CT검사날짜"}
    # {"검사코드": ["RG011", "RG012", "RG308"], "검사명": "CXR", "항목": "CXR검사날짜"}          
]   

CT_result = [
    {"검사결과": ["Cavity"],"항목": "Cavity유무"},
    {"검사결과": ["Emphysema"], "항목": "Emphysema유무"},
    {"검사결과": ["Bronchiectasis"], "항목": "Bronchiectasis유무"},
    {"검사결과": ["Consolidation"], "항목": "Consolidation유무"},
    {"검사결과": ["Pleural effusion"], "항목": "Pleural_effusion유무"},
    {"검사결과": ["Aggravation"], "항목": "Aggravation유무"}
]

# 진단 전 결과보고된 CT에서 cavity, bronchiectasis, emphysema 여부 확인
# 실시일시가 빈칸인 경우 해결 방법 생각
# CT검사날짜, cavity, bronchiectasis, emphysema 날짜 --> cavity, bronchiectasis, emphysema 유무
# 결과내역이 EMR의 CT결과를 그대로 저장하는지 확인 필요
# 같은 날의 CT가 아닌 경우 CT 날짜를 출력할 수 있도록??


procedure = procedure[["person_id", "procedure_date", "실시일시", "보고일시", "procedure_source_value", "procedure_source_value_name", "결과내역", "결론 및 진단"]]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]
procedure_radiology = pd.merge(procedure, condition_A31, on= "person_id", how= 'left')

# "NTM"을 포함하는 CT결과내역 추출(코드 작성)
procedure_radiology = procedure_radiology[procedure_radiology["결과내역"].str.contains("NTM", case= False, na= False)] 
# 기관지 내시경 출력되는 문제 : 첫 번째 for문에서 CT결과를 포함하게 만든 temp_df는 첫 번째 for문에서만 작동하여 두 번째 for문에서 필터링 되지 않고
# cavity, bronchiectasis를 포함하는 모든 검사결과 중 "상병입력일"과 가장 가까운 결과값이 출력됨
# NTM을 포함하지 않는


# 진단 전 CT보고일 및 결과내역
for key in radiology:
    procedure_radiology = procedure_radiology[procedure_radiology["NTM진단일"] >= procedure_radiology["보고일시"]]
    procedure_radiology = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(key["검사코드"]), na= False)
    ]
    
    temp_df = procedure_radiology
    temp_df = temp_df.sort_values(by= ["person_id", "보고일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
    
    temp_df[key["항목"]] = temp_df["실시일시"]
    temp_df = temp_df[["person_id", key["항목"]]]
    
    # 원래 데이터프레임과 병합
    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")


# 결과내역에 단어를 포함하는 CT결과 추출(EMR에서 검증한 것을 기반으로 실제 DB구축에 적용한 사례?)
for key in CT_result:
    key_change = "|".join(map(re.escape, key["검사결과"]))
    
    temp_df = procedure_radiology[
        procedure_radiology["결과내역"].str.contains(key_change, case= False, na= False)
        ]
    temp_df = temp_df.sort_values(by= ["person_id", "보고일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)

    temp_df[key["항목"]] = temp_df["결과내역"]
    temp_df = temp_df[["person_id", key["항목"]]]

    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")

condition_A31.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\CT결과.csv", index=False)
    




'''
# 치료시작 전 CT검사일
for key in radiology:
    procedure_radiology = procedure_radiology[procedure_radiology["NTM진단일"] >= procedure_radiology["검사일시"]]

    temp_df = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(radiology["검사코드"]), na=False)
        ]
    
    temp_df = temp_df.sort_values(by= ["person_id", "검사일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
'''




# CT cavity, emphysema, bronchiectasis


