# fakinflu — worker de smoke tests Phase 0 (R2 + streaming). CPU, aucun modele, image minimale.
# Build par RunPod (integration GitHub) a chaque GitHub Release : make worker-release W=r2smoke
FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install "runpod==1.7.*"
COPY transfer.py handler.py /app/
CMD ["python3", "-u", "handler.py"]
