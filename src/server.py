
<<<<<<< Updated upstream
=======
from fastapi import FastAPI, Request
import os
from patient_search import find_patients


VERTEX_ENDPOINT_ID = os.getenv("VERTEX_ENDPOINT_ID", "6314658006038478848")
VERTEX_PROJECT_ID = os.getenv("VERTEX_PROJECT_ID", "aif-usr-p-az-schedul-efe3")
VERTEX_INDEX_ID = os.getenv("VERTEX_INDEX_ID", "all_embedding_clarity_patient_deployed_09232024_1")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

>>>>>>> Stashed changes
app = FastAPI()

@app.get("/")
def hello():
    return {"message": "Welcome to AZ Scheduling RAG Service"}

@app.get("/status")
def health_check():
    return {"message": "Health check ok"}

@app.post("/patient-like-me")
async def find_patient_like_me(req: Request):

    pat = await req.json()
    print (pat)

    response = find_patients(VERTEX_PROJECT_ID, VERTEX_INDEX_ID, VERTEX_LOCATION, VERTEX_ENDPOINT_ID, pat)

    print(response)
    return response
