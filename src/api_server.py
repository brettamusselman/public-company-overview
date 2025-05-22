from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List
import subprocess
import logging
import os

# Setup logging
logger = logging.getLogger(__name__)

app = FastAPI()

class ArgList(BaseModel):
    args: List[str]

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Completed response: {response.status_code}")
    return response

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/run-job")
async def run_job(arg_list: ArgList):
    logger.info(f"Received job args: {arg_list.args}")

    if not arg_list.args:
        raise HTTPException(status_code=400, detail="Missing required CLI args")

    try:
        MAIN_PATH = os.path.join(os.path.dirname(__file__), "main.py")
        cmd = ["python", MAIN_PATH] + arg_list.args
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        logger.info("Job executed successfully")

        return {
            "status": "success",
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        raise HTTPException(status_code=500, detail={
            "error": "Command execution failed",
            "stdout": e.stdout.strip(),
            "stderr": e.stderr.strip(),
        })

    except Exception as e:
        logger.exception("Unexpected error while running job")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/daily-update")
async def daily_update():
    try:
        MAIN_PATH = os.path.join(os.path.dirname(__file__), "main.py")
        cmd = ["python", MAIN_PATH, "--daily-update"]
        logger.info(f"Executing daily update command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("Daily update executed successfully")

        return {
            "status": "success",
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Daily update command failed: {e.stderr}")
        raise HTTPException(status_code=500, detail={
            "error": "Daily update execution failed",
            "stdout": e.stdout.strip(),
            "stderr": e.stderr.strip(),
        })
    
    except Exception as e:
        logger.exception("Unexpected error while running daily update")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/standard-workflow")
async def standard_workflow(arg_list: ArgList):
    logger.info(f"Received standard workflow args: {arg_list.args}")

    if not arg_list.args:
        raise HTTPException(status_code=400, detail="Missing required CLI args")

    try:
        MAIN_PATH = os.path.join(os.path.dirname(__file__), "main.py")
        cmd = ["python", MAIN_PATH] + arg_list.args
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        logger.info("Standard workflow executed successfully")

        return {
            "status": "success",
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        raise HTTPException(status_code=500, detail={
            "error": "Command execution failed",
            "stdout": e.stdout.strip(),
            "stderr": e.stderr.strip(),
        })

    except Exception as e:
        logger.exception("Unexpected error while running standard workflow")
        raise HTTPException(status_code=500, detail=str(e))