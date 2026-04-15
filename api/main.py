# api/main.py
#
# API de jobs asíncronos para HIBLOOMS — estado en memoria (sin base de datos).
# Arrancar con:
#   uvicorn api.main:app --host 0.0.0.0 --port 8000
#
# La variable de entorno GEE_SERVICE_ACCOUNT_JSON debe contener el JSON
# de la cuenta de servicio de Google Earth Engine (el mismo que en secrets.toml).

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Estado en memoria
# Estructura de cada job:
#   {
#     "job_id":   str,
#     "workflow": str,
#     "state":    "pending" | "running" | "done" | "error",
#     "progress": int (0-100),
#     "step":     str,
#     "error":    str,
#     "results":  dict,
#     "config":   dict,
#   }
# ---------------------------------------------------------------------------
_JOBS: Dict[str, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# App FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="HIBLOOMS Jobs API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # En producción restringir al dominio de Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Modelos Pydantic
# ---------------------------------------------------------------------------
class JobSubmitRequest(BaseModel):
    workflow: str                  # "visualization" | "calibration"
    model_config = {"extra": "allow"}

    def full_config(self) -> Dict[str, Any]:
        return self.model_dump()


class JobProgressUpdate(BaseModel):
    step:     str
    progress: int   # 0-100


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
def _new_job(workflow: str, config: Dict[str, Any]) -> str:
    job_id = str(uuid.uuid4())
    _JOBS[job_id] = {
        "job_id":   job_id,
        "workflow": workflow,
        "state":    "pending",
        "progress": 0,
        "step":     "",
        "error":    "",
        "results":  {},
        "config":   config,
    }
    return job_id


def _update_progress(job_id: str, step: str, progress: int) -> None:
    if job_id in _JOBS:
        _JOBS[job_id].update({"state": "running", "step": step, "progress": progress})


def _complete(job_id: str, results: Dict[str, Any]) -> None:
    if job_id in _JOBS:
        _JOBS[job_id].update({"state": "done", "progress": 100, "step": "Completed", "results": results})


def _fail(job_id: str, error: str) -> None:
    if job_id in _JOBS:
        _JOBS[job_id].update({"state": "error", "error": error})


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/jobs/submit", status_code=202)
async def submit_job(body: JobSubmitRequest, background_tasks: BackgroundTasks):
    """
    Recibe la configuración desde la app Streamlit, crea el job en memoria
    y lo lanza en background. Devuelve el job_id inmediatamente.
    """
    workflow = body.workflow
    if workflow not in ("visualization", "calibration"):
        raise HTTPException(status_code=400, detail=f"Unknown workflow: {workflow}")

    config = body.full_config()
    job_id = _new_job(workflow, config)
    log.info(f"Job created: {job_id} ({workflow})")

    if workflow == "visualization":
        from api.worker import run_visualization_job
        background_tasks.add_task(run_visualization_job, job_id, config, _update_progress, _complete, _fail)
    else:
        from api.worker import run_calibration_job
        background_tasks.add_task(run_calibration_job, job_id, config, _update_progress, _complete, _fail)

    return {"job_id": job_id, "state": "pending"}


@app.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """
    Devuelve el estado actual del job.
    Llamado por la app cada 5 s mediante st_autorefresh.
    """
    job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    response: Dict[str, Any] = {
        "job_id":   job_id,
        "workflow": job["workflow"],
        "state":    job["state"],
        "progress": job["progress"],
        "step":     job["step"],
        "error":    job["error"],
    }

    if job["state"] == "done":
        response["results"] = job["results"]

    return response


@app.patch("/jobs/{job_id}")
async def patch_job_progress(job_id: str, body: JobProgressUpdate):
    """
    Permite actualizar el progreso desde un proceso externo
    (útil si el worker corre en un contenedor separado en LifeWatch/NaaVRE).
    """
    if job_id not in _JOBS:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    _update_progress(job_id, body.step, body.progress)
    return {"job_id": job_id, "step": body.step, "progress": body.progress}
