import os
from contextlib import asynccontextmanager
from pathlib import Path

import fastapi
import pandas as pd
import toml
import uvicorn
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from cron_descriptor import get_description
from fastapi import Depends, Header, HTTPException
from fastapi.responses import HTMLResponse
from loguru import logger
from hakai_api import Client

from hakai_ctd_qc.__main__ import main as qc_profiles
import panel as pn


def get_version_from_pyproject():
    with open("pyproject.toml") as f:
        pyproject = toml.load(f)
    return pyproject["tool"]["poetry"]["version"]


version = get_version_from_pyproject()

API_ROOT = os.getenv("HAKAI_API_ROOT", "https://goose.hakai.org/api")
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", 8000))
DEBUG = os.getenv("DEBUG", False)
TOKENS = os.getenv("TOKENS", "").split(",")
QC_CRON = os.getenv("QC_CRON")

logger.info(f"Starting Hakai CTD QC API {version=}")
logger.info("HAKAI API ROOT: {}", API_ROOT)
logger.info("HOST: {}", HOST)
logger.info("PORT: {}", PORT)
logger.info("DEBUG: {}", DEBUG)
logger.info("N TOKENS: {}", len(TOKENS))
logger.info("QC_CRON: {}", QC_CRON)


JOBS_MESSAGES = {}
jobstores = {"default": MemoryJobStore()}
# Initialize an AsyncIOScheduler with the jobstore
scheduler = AsyncIOScheduler(
    jobstores=jobstores, jobs_default={"max_instances": 1}, timezone="UTC"
)


def run_qc(**kwargs):
    logger.info("Running default QC")
    id = kwargs.pop("id")
    JOBS_MESSAGES[id] = {
        "timestamp": str(pd.Timestamp.utcnow().isoformat()),
        "status": "running",
    }
    response = qc_profiles(**kwargs)
    JOBS_MESSAGES[id] = {
        "timestamp": str(pd.Timestamp.utcnow().isoformat()),
        **response,
    }
    return JOBS_MESSAGES


if QC_CRON:
    logger.info(f"Running default QC {QC_CRON=}")
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


app_description = f"""
This is the {os.getenv("ENVIRONMENT", '')} Hakai Institute ctd quality control tool.

- Hakai-API-root: `{API_ROOT}`.
- Cron schedule: {(get_description(QC_CRON) + " =`" + QC_CRON + '`') if QC_CRON else None}

The following endpoints are available:
"""

app = fastapi.FastAPI(
    title="Hakai CTD QC",
    description=app_description,
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
    return {
        "status": "ok",
        "version": version,
        "cron": QC_CRON,
        "hakai-api-root": API_ROOT,
    }


@app.get("/jobs/status")
async def get_jobs_status():
    return JOBS_MESSAGES or "No jobs have been run"


if QC_CRON:

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


@app.get("/manual-qc-status")
async def get_manual_qced():
    client = Client(credentials=os.getenv("HAKAI_API_TOKEN"))
    logger.info("Getting manual QCed data")
    response = client.get(API_ROOT + "/eims/views/output/ctd_qc?limit=-1")
    response.raise_for_status()
    df_qc = pd.DataFrame(response.json())

    logger.info("Get cast data")
    response = client.get(API_ROOT + "/ctd/views/file/cast?limit=-1&fields=organization,work_area,station,hakai_id,start_dt")
    response.raise_for_status()
    df_cast = pd.DataFrame(response.json())

    # combine the two dataframes
    df = pd.merge(df_qc, df_cast, on=["work_area","hakai_id"], how="outer")
    flag_columns = df.filter(like="_flag").columns
    summary = []
    for index,df_group in df.groupby(['organization', 'work_area', 'station']):

        qced = df_group.dropna(how='all', subset=flag_columns)
        if qced.empty:
            continue

        summary.append({
            "organization": index[0],
            "work_area": index[1],
            "station": index[2],
            "n_drops": len(df_group),
            "n_qced": len(qced),
            **qced[flag_columns].count().to_dict(),
            "last_drop_qced": qced['start_dt'].max()
        })

    df_summary = pd.DataFrame(summary)
    html_table = df_summary.to_html(index=False, classes='display', table_id='dataTable')

    # Embed DataTables JavaScript and CSS
    html_string = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Manual QCed Data</title>
        <link rel="stylesheet" href="https://cdn.datatables.net/1.10.21/css/jquery.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
        <script src="https://cdn.datatables.net/1.10.21/js/jquery.dataTables.min.js"></script>
        <script>
            $(document).ready(function() {{
                $('#dataTable').DataTable();
            }});
        </script>
    </head>
    <body>
        <h1>Hakai CTD Profiles Manually QCed Status</h1>
        <p>Number of drops and number of drops that have been manually QCed</p>
        {html_table}
    </body>
    </html>
    """

    return HTMLResponse(content=html_string, status_code=200)

    

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)

