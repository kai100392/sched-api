from pydantic import BaseModel, Field


class ScheduleResponse (BaseModel):
    clinic_number: int
    scheduling_actions: []
    similar_patients: []

class SimilarPatient (BaseModel):
    clinic_number: int
    callin_date: str
    appt_date: str
    PSA: float
    imaging: bool
    biopsy: str
    actions: list [str] = []

class SimilarPatientsResponse (BaseModel):
    similar_patients: list [SimilarPatient] = []