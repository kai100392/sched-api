
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
async def schedule(clinic_num: str) -> SimilarPatientResponse:
    # retrieve patient data from Clarity using clinic_num
    
    # call analysis svc
    analysis_response = { similar_patients: [3303923, 3303925] }
    similar_patients = [ SimilarPatient(clinic_num: 3303923, callin_date: "2020-01-01 10:00", appt_date: "2020-01-05 8:00", PSA: 5)},
                         SimilarPatient(clinic_num: 3303925, callin_date: "2020-02-04 13:00", appt_date: "2020-02-15 9:00", PSA: 9)}
                    ]
    response = SimilarPatientsReponse (clinic_num: clinic_num, similar_patients: similar_patients)

    print(response)
    return response
