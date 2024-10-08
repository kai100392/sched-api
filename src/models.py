from pydantic import BaseModel, Field


class ScheduleResponse (BaseModel):
    clinic_number: int
    scheduling_actions: []
    similar_patients: []

