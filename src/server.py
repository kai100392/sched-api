
from fastapi import FastAPI, Request, Body
import os
from patient_search import find_patients


VERTEX_ENDPOINT_ID = os.getenv("VERTEX_ENDPOINT_ID", "6314658006038478848")
VERTEX_PROJECT_ID = os.getenv("VERTEX_PROJECT_ID", "aif-usr-p-az-schedul-efe3")
VERTEX_INDEX_ID = os.getenv("VERTEX_INDEX_ID", "all_embedding_clarity_patient_deployed_09232024_1")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

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
