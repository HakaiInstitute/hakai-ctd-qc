import os
from contextlib import asynccontextmanager

import fastapi
import pandas as pd
import toml
import uvicorn
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, Header, HTTPException
from loguru import logger

from hakai_profile_qc.__main__ import main as qc_profiles


def get_version_from_pyproject():
    with open("pyproject.toml") as f:
        pyproject = toml.load(f)
    return pyproject["tool"]["poetry"]["version"]


version = get_version_from_pyproject()

API_ROOT = os.getenv("HAKAI_API_ROOT", "https://goose.hakai.org/api")
HOST = os.getenv("HOST", "127.0.0.1")
PORT = os.getenv("PORT", 8000)
DEBUG = os.getenv("DEBUG", False)
TOKENS = os.getenv("TOKENS", "").split(",")
QC_CRON = os.getenv("QC_CRON")


JOBS_MESSAGES = {}
jobstores = {"default": MemoryJobStore()}
# Initialize an AsyncIOScheduler with the jobstore
scheduler = AsyncIOScheduler(
    jobstores=jobstores, jobs_default={"max_instances": 1}, timezone="UTC"
)


def run_qc(**kwargs):
    logger.info("Running default QC")
    id = kwargs.pop("id")
    JOBS_MESSAGES[id] = {"timestamp": str(pd.Timestamp.utcnow().isoformat()), "status": "running"}
    response = qc_profiles(**kwargs)
    JOBS_MESSAGES[id] = {"timestamp": str(pd.Timestamp.utcnow().isoformat()), **response}
    return JOBS_MESSAGES

if QC_CRON:
    logger.info(
        f"Running default QC {QC_CRON=}"
    )
    trigger = CronTrigger.from_crontab(QC_CRON, timezone="UTC")
    schedule_job_id = f"scheduled:{QC_CRON}"
    scheduler.add_job(
        run_qc,
        kwargs={"id": schedule_job_id},
        trigger=trigger,
        id=schedule_job_id,
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600,
    )


@asynccontextmanager
async def schedule_task(app: fastapi.FastAPI):
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()


app = fastapi.FastAPI(
    title="Hakai Profile QC",
    description="Quality control of Hakai Institute CTD profiles",
    version=version,
    debug=DEBUG,
    docs_url="/",
    lifespan=schedule_task,
)


def token_check(token: str = Header(... if TOKENS else None)):
    """Block unauthorized access to the API"""
    if TOKENS and token not in TOKENS:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized access<br>`token` not found in the list of authorized tokens",
        )


@app.get("/status")
async def get_status():
    return {"status": "ok", "version": version, "cron": QC_CRON}


@app.get("/jobs/status")
async def get_jobs_status():
    return JOBS_MESSAGES or "No jobs have been run"


if QC_CRON:
    app.description += (
        f"<br>Running default QC every cron:`{QC_CRON}`"
    )

    @app.get("/jobs/schedule")
    def get_schedule():
        return [str(job) for job in scheduler.get_jobs()]
    

    @app.post("/job/pause")
    async def pause_scheduled_jobs(
        token=Depends(token_check), id: str = "scheduled_qc"
    ):
        logger.info("Pausing {} QC", id)
        scheduler.pause_job(id)
        return f"Job {id=} paused"

    @app.post("/job/resume")
    async def resume_schedule_jobs(
        token=Depends(token_check), id: str = "scheduled_qc"
    ):
        logger.info("Resuming {} QC", id)
        scheduler.resume_job(id)
        return f"Job {id=} resumed"


@app.post("/qc")
async def run_quality_control_on_hakai_profiles(
    request: fastapi.Request,
    hakai_ids: str = None,
    processing_stages: str = "8_binAvg,8_rbr_processed",
    api_root=API_ROOT,
    upload_flag: bool = True,
    test_suite: bool = False,
    token: str = Depends(token_check),
):
    """Run QC on hakai profile(s)"""
    if hakai_ids:
        id = f"hakai_id={hakai_ids}"
    elif test_suite:
        id = "test_suite"
    else:
        id = f"processing_stages={processing_stages}"
    scheduler.add_job(
        run_qc,
        kwargs={
            "id": id,
            "hakai_ids": hakai_ids,
            "processing_stages": processing_stages,
            "api_root": api_root,
            "upload_flag": upload_flag,
            "test_suite": test_suite,
        },
        id=id,
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info(f"Job {id=} added to scheduler: {scheduler.get_jobs()}")
    return {"id": id, "status": "scheduled"}


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
