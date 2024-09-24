
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def hello():
    return {"message": "Welcome to AZ Scheduling RAG Service"}

@app.get("/status")
def health_check():
    return {"message": "Health check ok"}

