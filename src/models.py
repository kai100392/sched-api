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