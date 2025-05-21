from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import subprocess

app = FastAPI()

class ArgList(BaseModel):
    args: List[str]

"""
Next Steps:
- Once main.py is finalized, finalize functions with command line args
- Add pydantic validation for the args for each function
- Host on Cloud Run using Docker
- Add logging
- Validatoin through IAM Cloud Run Invoker then in the local scripts use gcloud cli
"""

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/run-job")
async def run_job(arg_list: ArgList):
    try:
        cmd = ["python", "main.py"] + arg_list.args
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        return {
            "status": "success",
            "stdout": result.stdout,
            "stderr": result.stderr
        }

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail={
            "error": "Command failed",
            "stdout": e.stdout,
            "stderr": e.stderr
        })
