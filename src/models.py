# This file must be synched between sched-api and analysis-svc
from pydantic import BaseModel


class PatientRequest(BaseModel):
    APPT_TIME: str | None = None
    CE_data: bool | None = False
    PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST: str | None = None
    PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST: str | None = None
    PROSTATE_CANCER_ENC_VISIT_NAME_FIRST: str | None = None
    PROSTATE_CANCER_REFERRAL_CLASS: str | None = None
    PROSTATE_CANCER_REQUEST_METHOD_C_FIRST: str | None = None
    PROSTATE_CANCER_VISIT_AGE_FIRST: float | None = None
    all_orders: str | None = None
    biopsy_1: str | None = None
    biopsy_1_days: float | None = None
    biopsy_1_abnormal: bool | None = None
    biopsy_2: str | None = None
    biopsy_2_days: float| None = None
    biopsy_2_abnormal: bool | None = None
    icd_10_1: str | None = None
    icd_10_2: str | None = None
    imaging_1: str | None = None
    imaging_1_days: float | None = None
    imaging_1_abnormal: bool | None = None
    imaging_2: str | None = None
    imaging_2_days: float | None = None
    imaging_2_abnormal: bool | None = None
    imaging_3: str | None = None
    imaging_3_days: float | None = None
    imaging_3_abnormal: bool | None = None
    psa_1: str | None = None
    psa_1_days: float | None = None
    psa_1_value: float | None = None
    psa_1_unit: str | None = None
    psa_1_abnormal: bool | None = None
    psa_2: str | None = None
    psa_2_days: float | None = None
    psa_2_value: float | None = None
    psa_2_unit: str | None = None
    psa_2_abnormal: bool | None = None
    psa_3: str | None = None
    psa_3_days: float | None = None
    psa_3_value: float | None = None
    psa_3_unit: str | None = None
    psa_3_abnormal: bool | None = None
    psa_4: str | None = None
    psa_4_days: float | None = None
    psa_4_value: float | None = None
    psa_4_unit: str | None = None
    psa_4_abnormal: bool | None = None
    psa_recent_increase_percent: float | None = None
    biopsy_1_days_cat: str | None = None
    biopsy_2_days_cat: str | None = None
    imaging_1_days_cat: str | None = None
    imaging_2_days_cat: str | None = None
    imaging_3_days_cat: str | None = None
    psa_1_days_cat: str | None = None
    psa_2_days_cat: str | None = None
    psa_3_days_cat: str | None = None
    psa_4_days_cat: str | None = None
    psa_1_value_cat: str | None = None
    psa_2_value_cat: str | None = None
    psa_3_value_cat: str | None = None
    psa_4_value_cat: str | None = None
    psa_recent_increase_percent_cat: str | None = None
    PROSTATE_CANCER_VISIT_AGE_FIRST_cat: str | None = None

    description: str | None = None


class SimilarPatient(PatientRequest):
    PAT_ID: str | None = None
    PAT_MRN_ID: str | None  # remove this PHI
    target_1: str | None = None
    target_1_days: int | None = None
    target_2: str | None = None
    target_2_days: int | None = None
    target_3: str | None = None
    target_3_days: int | None = None
    target_CE_data: int | None = None
    target_all_orders: int | None = None
    target_all_orders_after_appt_for30d: int | None = None
    target_appt_days_from_contact: int | None = None

class PatientResponse(BaseModel):
    similar_patients: list[SimilarPatient] = []


class ValueRFD(BaseModel):
    value: str | None = None
    RFD: float | None = None


class Similarity(BaseModel):
    days_RMSE: float | None = None
    value_RMSE: float | None = None
    values: list[ValueRFD] | None = None


class SimilarityMetric(BaseModel):
    biopsy_1: Similarity | None = None
    biopsy_2: Similarity | None = None
    imaging_1: Similarity | None = None
    imaging_2: Similarity | None = None
    imaging_3: Similarity | None = None
    psa_1: Similarity | None = None
    psa_2: Similarity | None = None
    psa_3: Similarity | None = None
    psa_4: Similarity | None = None
    psa_recent_increase_percent: Similarity | None = None
    PROSTATE_CANCER_VISIT_AGE_FIRST: Similarity | None = None


class RecommendationItem(BaseModel):
    name: str | None = None
    score: float | None = None
    median_days: float | None = None


class Recommendations(BaseModel):
    target_1: list[RecommendationItem] = None
    target_2: list[RecommendationItem] = None
    target_3: list[RecommendationItem] = None


class PatientAnalysis(BaseModel):
    CURRENT_PATIENT: PatientRequest
    SIMILAR_PATIENTS: list[SimilarPatient] = []
    SIMILARITY_METRIC: SimilarityMetric = {}
    RECOMMENDATIONS: Recommendations

# Misc classes
class UserInfo (BaseModel):
    userEmail: str
    userId: str

class SQLConnection(BaseModel):
    con_type: str
    db_host: str
    db_name: str
