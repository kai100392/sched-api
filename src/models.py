from pydantic import BaseModel, Field


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

class SimilarPatientsResponse (BaseModel):
    similar_patients: list [SimilarPatient] = []


class PatientRequest (BaseModel):
    PROSTATE_CANCER_VISIT_AGE_FIRST: float
    biopsy_1: str
    biopsy_1_days: int
    biopsy_1_abnormal: str
    biopsy_2: str
    biopsy_2_days: int
    biopsy_2_abnormal: str
    imaging_1: str
    imaging_1_days: int
    imaging_1_abnormal: str
    imaging_2: str
    imaging_2_days: int
    imaging_2_abnormal: str
    imaging_3: str
    imaging_3_days: int
    imaging_3_abnormal: str
    psa_1: str
    psa_1_days: int
    psa_1_value: float
    psa_1_unit: str
    psa_1_abnormal: str
    psa_2: str
    psa_2_days: int
    psa_2_value: float
    psa_2_unit: str
    psa_2_abnormal: str
    psa_3: str
    psa_3_days: int
    psa_3_value: float
    psa_3_unit: str
    psa_3_abnormal: str
    psa_4: str
    psa_4_days: int
    psa_4_value: float
    psa_4_unit: str
    psa_4_abnormal: str
    psa_recent_increase_percent: float   