FROM  --platform=linux/amd64 python:3.11-slim AS base

WORKDIR /app

RUN pip install "poetry==1.6.1"


COPY . .

RUN poetry config virtualenvs.in-project false && \
    poetry install

EXPOSE 80
    
CMD ["uvicorn", "hakai_ctd_qc/api.py","--host", "0.0.0.0", "--port", "80"]
