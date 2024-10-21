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


class PatientRequest (BaseModel):
    PROSTATE_CANCER_VISIT_AGE_FIRST: Optional[float] = None
    biopsy_1: Optional[str] = None
    biopsy_1_days: Optional[int] = None
    biopsy_1_abnormal: Optional[str] = None
    biopsy_2: Optional[str] = None
    biopsy_2_days: Optional[int] = None
    biopsy_2_abnormal: Optional[str] = None
    imaging_1: Optional[str] = None
    imaging_1_days: Optional[int] = None
    imaging_1_abnormal: Optional[str] = None
    imaging_2: Optional[str] = None
    imaging_2_days: Optional[int] = None
    imaging_2_abnormal: Optional[str] = None
    imaging_3: Optional[str] = None
    imaging_3_days: Optional[int] = None
    imaging_3_abnormal: Optional[str] = None
    psa_1: Optional[str] = None
    psa_1_days: Optional[int] = None
    psa_1_value: Optional[float] = None
    psa_1_unit: Optional[str] = None
    psa_1_abnormal: Optional[str] = None
    psa_2: Optional[str] = None
    psa_2_days: Optional[int] = None
    psa_2_value: Optional[float] = None
    psa_2_unit: Optional[str] = None
    psa_2_abnormal: Optional[str] = None
    psa_3: Optional[str] = None
    psa_3_days: Optional[int] = None
    psa_3_value: Optional[float] = None
    psa_3_unit: Optional[str] = None
    psa_3_abnormal: Optional[str] = None
    psa_4: Optional[str] = None
    psa_4_days: Optional[int] = None
    psa_4_value: Optional[float] = None
    psa_4_unit: Optional[str] = None
    psa_4_abnormal: Optional[str] = None
    psa_recent_increase_percent: Optional[float] = None   