from pydantic import BaseModel, Field
from typing import Optional

class ScheduleResponse (BaseModel):
    clinic_num: int
    scheduling_actions: list [str]= []
    similar_patients: list [str] = []

class SimilarPatient (BaseModel):
    clinic_num: int
    callin_date: str
    appt_date: str
    PSA: float
    imaging: bool
    biopsy: str
    actions: list [str] = []

# this class must match analysis-svc's
class PatientRequest (BaseModel):
    APPT_TIME: str | None = None
    CE_data: bool | False = False
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


class UserInfo (BaseModel):
    userEmail: str
    userId: str

class SQLConnection (BaseModel):
    con_type: str
    db_host: str
    db_name: str
