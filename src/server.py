
from fastapi import FastAPI, HTTPException, Body
from fastapi.encoders import jsonable_encoder
from google.oauth2 import id_token
from google.auth.transport.requests import Request
from fastapi import Request as fRequest

from models import PatientRequest, PatientAnalysis, UserInfo, SQLConnection
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
def get_patient(clinic_num: str, callin_date: datetime | None = None) -> PatientRequest:
    """Retrieve and assess patient state, use MRN# 3-303-923 or 3-303-925 in dev (dash required)

    """

    print (f"retrieving patient data for {clinic_num}, callin_date={callin_date}")
    if not callin_date:
        callin_date = datetime.now()

    pat = None
    patlist = get_expanded_patient (clinic_num, callin_date, ENV)
    print(patlist)
    if len(patlist) > 0:
        pat = patlist[0]
    if pat == None:
        raise HTTPException(status_code=404, detail="Patient not found")
    print(pat)
    return pat


@app.get("/api/cap3/patient-like-me/{clinic_num}")
def find_similar_patient(clinic_num: str) -> PatientAnalysis:
    """Find similar patients, use MRN# 3-303-923 or 3-303-925 in dev (dash required).

    """

    print(f"find_similar_patient ({clinic_num})")

    req = get_patient(clinic_num)
    analysis_response = call_analysis_service ("POST", req, f"{ANALYSIS_URL}/cap3/patient-like-me", IAP_CLIENT_ID)
    print("analysis_response")
    print(json.dumps(analysis_response))
    return analysis_response

@app.get("/api/cap1/load")
def load_db():
    return call_analysis_service ("GET", "", f"{ANALYSIS_URL}/cap1/load", IAP_CLIENT_ID)

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
        
    