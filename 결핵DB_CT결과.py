# procedure_occurrence의 finding과 conclusion(결론 및 진단) DB확인
# 빈칸은 전부 온점(.)으로 찍히므로 대쉬(_)로 변경
# 6min walking : S40018, S40099 --> 신체정보 추출 가능여부 확인
# B.D.R --> FEV1, FEV1%, FVC, FVC%, DLCO 추출 가능여부 확인
# ctrl shift L : 같은 단어 선택
import pandas as pd 

person = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\샘플CDMV2_20241211\\person.csv")
procedure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\샘플CDMV2_20241211\\procedure_occurrence.csv")
condition_A31 = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\기저질환.csv")
# 치료시작일 코드필요


radiology = [
    {"검사코드": ["RC121", "RC194", "RC314", "RC363", "RC561", "RC994", "RC131", "RC200"], "검사명": "chestCT"},
    {"검사코드": ["RG011", "RG012", "RG308"], "검사명": "CXR"}
]   

# 진단 전 결과보고된 CT에서 cavity, bronchiectasis, emphysema 여부 확인
# 실시일시가 빈칸인 경우 해결 방법 생각
procedure = procedure[["person_id", "procedure_date", "실시일시", "보고일시", "procedure_source_value", "procedure_source_value_name", "결과내역", "결론 및 진단"]]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]
procedure_radiology = pd.merge(procedure, condition_A31, on= "person_id", how= 'left')

# 진단 전 CT보고일 및 결과내역
for key in radiology:
    procedure_radiology = procedure_radiology[procedure_radiology["NTM진단일"] >= procedure_radiology["보고일시"]]
    
    temp_df = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(radiology["검사코드"]), na=False)
        ]
    
    temp_df = temp_df.sort_values(by= ["person_id", "보고일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)

# 치료시작 전 CT검사일
for key in radiology:
    procedure_radiology = procedure_radiology[procedure_radiology["NTM진단일"] >= procedure_radiology["검사일시"]]

    temp_df = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(radiology["검사코드"]), na=False)
        ]
    
    temp_df = temp_df.sort_values(by= ["person_id", "검사일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)






# CT cavity, emphysema, bronchiectasis


