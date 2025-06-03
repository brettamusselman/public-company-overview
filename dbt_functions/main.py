import os, subprocess, logging, json
from flask import Request, make_response

PROJECT_DIR  = os.getenv("PROJECT_DIR",   "/workspace")
PROFILES_DIR = os.getenv("PROFILES_DIR",  "/workspace/profiles")
TARGET       = os.getenv("DBT_TARGET",    "prod")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("dbt_fn")

def run_dbt(args: list[str]) -> str:
    """Run dbt CLI and return stdout; raise on non-zero exit."""
    cmd = ["dbt", *args,
           "--project-dir", PROJECT_DIR,
           "--profiles-dir", PROFILES_DIR,
           "--target", TARGET]

    log.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode:
        log.error(proc.stderr)
        raise RuntimeError(proc.stderr)
    log.info(proc.stdout)
    return proc.stdout

def dbt_handler(request: Request):
    """
    POST /full-refresh   → seeds + stage_ext + full run
    POST /daily-update   → incremental run (exclude static dims)
    """
    path = request.path.rstrip("/")
    try:
        if path.endswith("/full-refresh"):
            run_dbt(["seed"])
            run_dbt(["run-operation", "stage_external_sources"])
            run_dbt(["run"])
            payload = {"status": "ok", "flow": "full-refresh"}

        elif path.endswith("/daily-update"):
            run_dbt([
                "run",
                "--exclude", "dim__date", "dim__interval", "dim__time"
            ])
            payload = {"status": "ok", "flow": "daily-update"}

        else:
            return make_response(
                f"Unknown path {path}. Use /full-refresh or /daily-update",
                404,
            )

        return make_response(json.dumps(payload), 200)

    except Exception as exc:
        log.exception("dbt invocation failed")
        return make_response(json.dumps({"error": str(exc)}), 500)
