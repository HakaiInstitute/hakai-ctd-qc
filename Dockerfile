FROM  --platform=linux/amd64 python:3.11-slim AS base

WORKDIR /app

RUN pip install "poetry==1.6.1"


COPY . .

RUN poetry config virtualenvs.in-project true
RUN poetry install --without dev

EXPOSE 80
    
CMD ["poetry","run","uvicorn", "hakai_ctd_qc.api:app","--host", "0.0.0.0", "--port", "80"]
