
from fastapi import FastAPI, HTTPException, Body
from fastapi.encoders import jsonable_encoder
from google.oauth2 import id_token
from google.auth.transport.requests import Request
from fastapi import Request as fRequest

from models import SimilarPatient, PatientRequest, UserInfo, SQLConnection
import os
import requests
import json
from google.cloud import bigquery
from bq_utils import get_expanded_patient
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

print("AZ Sched API - 0.0.1")

ANALYSIS_URL = os.getenv("ANALYSIS_URL", "https://sched-analysis-svc-493590485586.us-central1.run.app/analysis")
PROJECT_ID = os.getenv("PROJECT_ID", "cdh-az-sched-n-328641622107")
IAP_CLIENT_ID = os.getenv(
    "IAP_CLIENT_ID",
    "493590485586-tsb5ibt6kcp2ojm8nvpt9r54p9pp77c9.apps.googleusercontent.com",
)
ENV = os.getenv("ENV", "d")
UI_URL_BASE = os.getenv("UI_URL_BASE", "https://dev.cdh-az-sched-n.caf.mccapp.com")

app = FastAPI(docs_url="/api/docs", openapi_url="/api/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[UI_URL_BASE],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api")
def hello():
    return {"message": "Welcome to AZ Scheduling API"}

@app.get("/api/status")
def health_check():
    return {"message": "Health check ok"}

@app.get("/api/userinfo")
def get_userinfo(req: fRequest) -> UserInfo:
    dictHeaders = dict(req.headers)
    print (dictHeaders)
    prefix, userEmail = dictHeaders.get("x-goog-authenticated-user-email").split(":", 1)
    prefix, userId = dictHeaders.get("x-goog-authenticated-user-id").split(":", 1)
    return { "userEmail": userEmail, "userId": userId }


@app.get("/api/cap2/patient-state/{clinic_num}")
def get_patient(clinic_num: str, callin_date: datetime | None = None, mock: str | None = None) -> PatientRequest:
    """Retrieve and assess patient state, use MRN# 3303923 or 3303925

    """

    print (f"retrieving patient data for {clinic_num}, callin_date={callin_date}, mock={mock}")
    if not callin_date:
        callin_date = datetime.now()

    pat = None
    if mock and mock == 'Y':
        bq_client = bigquery.Client(project=PROJECT_ID)
        QUERY_TEMPLATE = f"""
                SELECT * FROM `{PROJECT_ID}.phi_azsched_us.expanded_patient_cohort`
                where PAT_MRN_ID = "{clinic_num}";
                """
        query = QUERY_TEMPLATE
        query_job = bq_client.query(query)
        for row in query_job:
            pat = dict(row.items())
    else:
        patlist = get_expanded_patient (clinic_num, callin_date, ENV)
        print(patlist)
        if len(patlist) > 0:
            pat = patlist[0]
    if pat == None:
        raise HTTPException(status_code=404, detail="Patient not found")
    print(pat)
    return pat


@app.get("/api/cap3/patient-like-me/{clinic_num}")
def find_similar_patient(clinic_num: str, mock: str | None = None) -> list [PatientRequest]:
    """Find similar patients, use MRN# 3303923 or 3303925 in dev.  Pass optional parameter mock=Y for mock response

    """

    print(f"find_similar_patient ({clinic_num}, mock={mock})")

    if mock and mock == 'Y':
        return find_similar_patient_mock()
    
    req = get_patient(clinic_num)
    analysis_response = call_analysis_service ("POST", req, f"{ANALYSIS_URL}/cap3/patient-like-me", IAP_CLIENT_ID)
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
        p = req
        # p = get_patient(clinic_num)
        response.append(p)
    print(response)
    return response

def find_similar_patient_mock () -> list [PatientRequest]:
    """Find similar patients calling analysis mock endpoint (not calling vertex search)

    """
    req = { 
        "PROSTATE_CANCER_VISIT_AGE_FIRST": 75.0, 
        "biopsy_1": "URO MR Fusion",
        "biopsy_1_days": 58,
        "biopsy_1_abnormal": "",
        "biopsy_2": "",
        "biopsy_2_days": 0,
        "biopsy_2_abnormal": "",
        "imaging_1": "PET CT SKULL TO THIGH PSMA",
        "imaging_1_days": 36, 
        "imaging_1_abnormal": "",
        "imaging_2": "CT ABDOMEN PELVIS WITH IV CONTRAST",
        "imaging_2_days": 41,
        "imaging_2_abnormal": "",
        "imaging_3": "MR PROSTATE WITHOUT AND WITH IV CONTRAST",
        "imaging_3_days": 86.0,
        "imaging_3_abnormal": "",
        "psa_1": "PROSTATE-SPECIFIC AG (PSA) DIAGNOSTIC, S",
        "psa_1_days": 105.0,
        "psa_1_value": 43.7,
        "psa_1_unit": "ng/mL",
        "psa_1_abnormal": "Y",
        "psa_2": "",
        "psa_2_days": 0.0,
        "psa_2_value": 0.0,
        "psa_2_unit": "",
        "psa_2_abnormal": "",
        "psa_3": "",
        "psa_3_days": 0.0,
        "psa_3_value": 0.0,
        "psa_3_unit": "",
        "psa_3_abnormal": "",
        "psa_4": "",
        "psa_4_days": 0.0,
        "psa_4_value": 0.0,
        "psa_4_unit": "",
        "psa_4_abnormal": "",
        "psa_recent_increase_percent": 0.0
    }

    print("find_similar_patient_mock")
    analysis_response = call_analysis_service ("POST", req, f"{ANALYSIS_URL}/cap3/patient-like-me/mock", IAP_CLIENT_ID)
    print("analysis_response")
    print(json.dumps(analysis_response))

    response = []
    for r in analysis_response["similar_patients"]:
        print(r)
        p = get_patient(r["clinic_num"], mock="Y")
        response.append(p)
    print(response)
    return response 

def call_analysis_service (method: str, data, analysis_svc_url, client_id):
    print("Inside call_analysis_service")
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

@app.get("/api/test")
def test():
    return call_analysis_service ("GET", "", f"{ANALYSIS_URL}/cap1/test", IAP_CLIENT_ID)

@app.post("/api/connect/")
def test_con(sql_con: SQLConnection = Body(...)):
    return call_analysis_service ("POST", sql_con, f"{ANALYSIS_URL}/cap1/connect", IAP_CLIENT_ID)
        
    