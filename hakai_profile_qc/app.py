import os

import fastapi
import pandas as pd
import toml
import uvicorn
from fastapi.responses import RedirectResponse
from fastapi import Header, HTTPException, Depends

from hakai_profile_qc.__main__ import main as qc_profiles


def get_version_from_pyproject():
    with open("pyproject.toml") as f:
        pyproject = toml.load(f)
    return pyproject["tool"]["poetry"]["version"]


API_URL = os.getenv("HAKAI_API_SERVER_ROOT", "https://goose.hakai.org/api")
API_HOST = os.getenv("HAKAI_API_SERVER_HOST", "127.0.0.1")
API_PORT = os.getenv("HAKAI_API_SERVER_PORT", 8000)
DEBUG = os.getenv("DEBUG", False)

TOKENS = os.getenv("HAKAI_API_TOKENS", "")

LAST_QC_RUN = None

app = fastapi.FastAPI(
    title="Hakai Profile QC",
    description="Quality control of Hakai Institute CTD profiles",
    version=get_version_from_pyproject(),
    debug=DEBUG,
    docs_url="/",
)


def run_default_qc():
    global LAST_QC_RUN
    response = qc_profiles(api_root=API_URL, upload_flag=False)
    LAST_QC_RUN = {"timestamp": pd.Timestamp.utcnow().isoformat(), **response}
    return LAST_QC_RUN

@app.middleware("http")
def token_check(request: Request, call_next):
    if not TOKENS:
        return call_next(request)
    token = request.headers.get("Authorization")
    if token not in TOKENS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return call_next(request)


@app.get("/status")
async def get_status():
    return {"status": "ok"}


@app.get("/last-run")
async def get_last_run_of_quality_control_on_all_newly_processed_profiles():
    global LAST_QC_RUN
    return LAST_QC_RUN or "No QC run yet"


@app.get("/qc/{hakai_id}")
async def run_quality_control_on_hakai_id(hakai_id: str):
    """QC a single profile by Hakai ID"""
    return qc_profiles(hakai_ids=[hakai_id], api_root=API_URL, upload_flag=False)


@app.get("/qc")
async def run_quality_control_on_all_newly_processed_profiles():
    """QC all processed profiles that haven't been QC'd yet"""
    return run_default_qc()


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
