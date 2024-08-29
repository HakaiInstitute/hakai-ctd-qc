import os

from fastapi.testclient import TestClient

from hakai_ctd_qc.api import app

token = os.getenv("TOKENS", "").split(",")[0]
client = TestClient(app)


def test_base_page():
    response = client.get("/")
    assert response.status_code == 200


def test_jobs_status():
    response = client.get("/jobs/status")
    assert response.status_code == 200
    assert "No jobs have been run" in response.text


def test_jobs_schedule():
    response = client.get("/jobs/schedule", headers={"token": token})
    assert response.status_code == 200
    assert response.text
