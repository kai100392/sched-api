
from fastapi import FastAPI, Request, Body
import os

ANALYSIS_URL = os.getenv("ANALYSIS_URL", "http://localhost:8000/patient_like_me")

app = FastAPI(docs_url="/api/docs", openapi_url="/api/openapi.json")

@app.get("/api")
def hello():
    return {"message": "Welcome to AZ Scheduling API"}

@app.get("/api/status")
def health_check():
    return {"message": "Health check ok"}

@app.get("/api/schedule/{clinic_num}")
async def schedule(clinic_num: str):
    # retrieve patient data from Clarity using clinic_num
    
    # call analysis svc
    response = Body (SchedulingResponse, ...)
    # {"clinic_num": clinic_num, "appts_to_schedule": ["consult"], similar_patients: []}
    
    print(response)
    return response
