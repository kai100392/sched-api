
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from google.oauth2 import id_token
from google.auth.transport.requests import Request
from models import SimilarPatient, PatientRequest
import os
import requests
import json
from google.cloud import bigquery
    
print("AZ Sched API - 0.0.1")

ANALYSIS_URL = os.getenv("ANALYSIS_URL", "https://sched-analysis-svc-493590485586.us-central1.run.app/analysis")
PROJECT_ID = os.getenv("PROJECT_ID", "cdh-az-sched-n-328641622107")
IAP_CLIENT_ID = os.getenv(
    "IAP_CLIENT_ID",
    "493590485586-tsb5ibt6kcp2ojm8nvpt9r54p9pp77c9.apps.googleusercontent.com",
)

app = FastAPI(docs_url="/api/docs", openapi_url="/api/openapi.json")

@app.get("/api")
def hello():
    return {"message": "Welcome to AZ Scheduling API"}

@app.get("/api/status")
def health_check():
    return {"message": "Health check ok"}

@app.get("/api/cap2/patient-state/{clinic_num}")
def get_patient(clinic_num: str) -> PatientRequest:
    """Retrieve and assess patient state, use MRN# 3303923 or 3303925

    """
    print (f"retrieving patient data for {clinic_num}")
    pat = None
    bq_client = bigquery.Client(project=PROJECT_ID)
    QUERY_TEMPLATE = f"""
            SELECT * FROM `{PROJECT_ID}.phi_azsched_us.expanded_patient_cohort`
            where PAT_MRN_ID = "{clinic_num}";
            """
    query = QUERY_TEMPLATE
    query_job = bq_client.query(query)
    for row in query_job:
        pat = dict(row.items())
    if pat == None:
        raise HTTPException(status_code=404, detail="Patient not found")
    print(pat)
    return pat

@app.get("/api/cap2/patient-state2/{clinic_num}")
def get_patient2(clinic_num: str) -> PatientRequest:
    """Retrieve and assess patient state from analysis svc, use MRN# 3303923 or 3303925

    """
    analysis_response = call_analysis_service ("GET", "", f"{ANALYSIS_URL}/cap2/patient-state/{clinic_num}", IAP_CLIENT_ID)
    print(analysis_response)
    return analysis_response

@app.get("/api/cap3/patient-like-me/{clinic_num}")
def find_similar_patient(clinic_num: str) -> list [PatientRequest]:
    """Find similar patients, use MRN# 3303923 or 3303925

    """
    req = get_patient(clinic_num)
    analysis_response = call_analysis_service ("POST", req, f"{ANALYSIS_URL}/cap3/patient-like-me", IAP_CLIENT_ID)

    # retrieve patient data from Clarity for the patients returned above
    # similar_patients = [ SimilarPatient(clinic_num = 3303923, callin_date = "2020-01-01 10:00", appt_date = "2020-01-05 8:00", PSA = 5, imaging = True, biopsy = "YES", actions = ["consult"]),
    #                      SimilarPatient(clinic_num = 3303925, callin_date = "2020-02-04 13:00", appt_date = "2020-02-15 9:00", PSA = 9, imaging = True, biopsy = "NO", actions = ["Biopsy", "consult"])
    #                 ]

    print("analysis_response")
    print(json.dumps(analysis_response))

    response = []
    for r in analysis_response["similar_patients"]:
        # p = SimilarPatient(
        #     clinic_num = r ["clinic_num"],
        #     callin_date = "callin date",
        #     appt_date = "appt date",
        #     PSA = 8.5,
        #     imaging = False,
        #     biopsy = "biopsy",
        #     actions = ["biopsy", "consult"]
        # )
        p = get_patient(clinic_num)
        response.append(p)
    print(response)
    return response

 

def call_analysis_service (method: str, data, analysis_svc_url, client_id):
    input = jsonable_encoder(data)
    print(f"Getting open_id_connect_token for {client_id} to call url {analysis_svc_url}")
    open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    print(f"submitting {method} request to url {analysis_svc_url}")
    print(input)

    try:
        resp = requests.request(
            method,
            analysis_svc_url,
            headers={
                "Authorization": f"Bearer {open_id_connect_token}",
                "Content-Type": "application/json",
            },
            json=input
        )
        print("response received")
        print(resp)

        if resp.status_code == 403:
            raise Exception(
                "Service account does not have permission to "
                "access the IAP-protected application."
            )
        elif resp.status_code != 200:
            raise Exception(
                f"Bad response from application: {resp.status_code!r} / {resp.headers!r} / {resp.text!r}"
            )
        else:
            # format output
            print(resp.text)
            prediction_response = json.loads(resp.text)
            return prediction_response
    except Exception as e:
        print(f"Error calling ${analysis_svc_url}: {e}")
        raise Exception(
                f"Error calling ${analysis_svc_url}: {e}"
            )