# api/main.py
#
# API de jobs asíncronos para HIBLOOMS.
# Arrancar con:
#   uvicorn api.main:app --host 0.0.0.0 --port 8000
#
# Requiere en variables de entorno (o .env):
#   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PASSWORD

from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db_utils import (
    create_jobs_table,
    create_job,
    get_job,
    update_job_progress,
    complete_job,
    fail_job,
    get_engine_from_config,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración de base de datos desde variables de entorno
# ---------------------------------------------------------------------------
def _db_config() -> Dict[str, Any]:
    return {
        "host":     os.environ["POSTGRES_HOST"],
        "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
        "dbname":   os.environ["POSTGRES_DBNAME"],
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


# ---------------------------------------------------------------------------
# Startup: crear tabla si no existe
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        engine = get_engine_from_config(_db_config())
        create_jobs_table(engine)
        log.info("Jobs table ready.")
    except Exception as e:
        log.error(f"Could not initialise jobs table: {e}")
    yield


# ---------------------------------------------------------------------------
# App FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="HIBLOOMS Jobs API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # En producción, restringir al dominio de Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Modelos Pydantic
# ---------------------------------------------------------------------------
class JobSubmitRequest(BaseModel):
    workflow: str                   # "visualization" | "calibration"
    config:   Dict[str, Any] = {}   # payload completo desde la app


class JobProgressUpdate(BaseModel):
    step:     str
    progress: int                   # 0-100


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/jobs/submit", status_code=202)
async def submit_job(body: JobSubmitRequest, background_tasks: BackgroundTasks):
    """
    Recibe la configuración desde la app Streamlit, crea el job en la DB
    y lo lanza en background. Devuelve el job_id inmediatamente.
    """
    if body.workflow not in ("visualization", "calibration"):
        raise HTTPException(status_code=400, detail=f"Unknown workflow: {body.workflow}")

    engine = get_engine_from_config(_db_config())

    # Combinar workflow + config en un único dict para almacenar
    full_config = {"workflow": body.workflow, **body.config}
    job_id = create_job(engine, body.workflow, full_config)
    log.info(f"Job created: {job_id} ({body.workflow})")

    # Lanzar el worker en background (no bloquea la respuesta HTTP)
    background_tasks.add_task(_run_job, job_id, body.workflow, full_config)

    return {"job_id": job_id, "state": "pending"}


@app.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """
    Devuelve el estado actual del job: state, progress, step y results si done.
    Llamado por la app cada 5 s mediante st_autorefresh.
    """
    engine = get_engine_from_config(_db_config())
    job = get_job(engine, job_id)

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

    # Solo incluir resultados cuando el job ha terminado
    if job["state"] == "done":
        response["results"] = job["results"]

    return response


@app.patch("/jobs/{job_id}")
async def patch_job_progress(job_id: str, body: JobProgressUpdate):
    """
    Permite que el worker actualice el progreso desde un proceso externo
    (útil cuando el worker corre en un contenedor separado en LifeWatch/NaaVRE).
    """
    engine = get_engine_from_config(_db_config())
    job = get_job(engine, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    update_job_progress(engine, job_id, body.step, body.progress)
    return {"job_id": job_id, "step": body.step, "progress": body.progress}


# ---------------------------------------------------------------------------
# Worker interno (BackgroundTask)
# Importa el worker real para no duplicar lógica.
# ---------------------------------------------------------------------------
async def _run_job(job_id: str, workflow: str, config: Dict[str, Any]) -> None:
    """
    Wrapper async que llama al worker síncrono en un hilo separado
    para no bloquear el event loop de FastAPI.
    """
    import asyncio
    from functools import partial

    loop = asyncio.get_event_loop()

    if workflow == "visualization":
        from api.worker import run_visualization_job
        fn = partial(run_visualization_job, job_id, config, _db_config())
    elif workflow == "calibration":
        from api.worker import run_calibration_job
        fn = partial(run_calibration_job, job_id, config, _db_config())
    else:
        engine = get_engine_from_config(_db_config())
        fail_job(engine, job_id, f"Unknown workflow: {workflow}")
        return

    try:
        await loop.run_in_executor(None, fn)
    except Exception as e:
        log.error(f"Job {job_id} crashed outside worker: {e}")
        engine = get_engine_from_config(_db_config())
        fail_job(engine, job_id, str(e))
