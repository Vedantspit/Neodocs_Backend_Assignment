import sqlite3
import os
import uuid
import json
import logging
from datetime import datetime
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_URL", "records.db")
if not DB_PATH:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# Logger Config
logger = logging.getLogger("backend_assessment")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'
)
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.propagate = False
# Logger Config Ends

app = FastAPI()


# custom data validation
def validate_data(data):
    fields = ["test_id", "patient_id", "clinic_id", "test_type", "result"]
    for f in fields:
        if f not in data or not isinstance(data[f], str) or not data[f].strip():
            return f"Invalid or missing field: {f}"
    return None


def initialise_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tests(
                test_id TEXT PRIMARY KEY,
                patient_id TEXT NOT NULL,
                clinic_id TEXT NOT NULL,
                test_type TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        logger.info("Database initialized")
    finally:
        conn.close()


initialise_db()


# Routes
@app.post("/tests")
async def create_test(request: Request):
    req_id = str(uuid.uuid4())

    try:
        body = await request.json()
    except Exception:
        logger.warning(json.dumps({"request_id": req_id, "error": "Invalid JSON"}))
        return Response(
            content='{"error":"Invalid Json"}',
            status_code=400,
            media_type="application/json",
        )

    val_error = validate_data(body)
    if val_error:
        logger.warning(
            json.dumps(
                {
                    "event": "validation_failed",
                    "request_id": req_id,
                    "reason": val_error,
                }
            )
        )
        return Response(
            content=f'{{"error":"{val_error}"}}',
            status_code=400,
            media_type="application/json",
        )

    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN")
        cursor.execute(
            """
            INSERT INTO tests
            (test_id, patient_id, clinic_id, test_type, result, created_at)
            VALUES (?,?,?,?,?,?)
            """,
            (
                body["test_id"],
                body["patient_id"],
                body["clinic_id"],
                body["test_type"],
                body["result"],
                datetime.now().isoformat(),
            ),
        )
        cursor.execute("COMMIT")

        logger.info(
            json.dumps(
                {
                    "event": "test_created",
                    "request_id": req_id,
                    "test_id": body["test_id"],
                }
            )
        )

        return {"status": "success"}

    except sqlite3.IntegrityError:
        cursor.execute("ROLLBACK")

        logger.error(
            json.dumps(
                {
                    "event": "duplicate_test",
                    "request_id": req_id,
                    "test_id": body["test_id"],
                }
            )
        )

        return Response(
            content='{"error":"Conflict: test_id exists"}',
            status_code=409,
            media_type="application/json",
        )

    except Exception as e:
        cursor.execute("ROLLBACK")

        logger.error(
            json.dumps(
                {
                    "event": "internal_server_error",
                    "request_id": req_id,
                    "error": str(e),
                }
            )
        )

        return Response(
            content='{"error":"Internal Error"}',
            status_code=500,
            media_type="application/json",
        )

    finally:
        conn.close()


@app.get("/tests")
async def get_tests(clinic_id: str = None):
    # Requirement: Handle missing query parameter
    if not clinic_id:
        logger.warning(json.dumps({"event": "missing_clinic_id"}))
        return Response(
            content='{"error":"clinic_id is required"}',
            status_code=400,
            media_type="application/json",
        )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM tests WHERE clinic_id = ?", (clinic_id,))
        rows = cursor.fetchall()

        # Returning an empty list [] if 'no results found'
        results = [dict(row) for row in rows]

        logger.info(
            json.dumps(
                {
                    "event": "fetch_tests",
                    "clinic_id": clinic_id,
                    "count": len(results),
                }
            )
        )

        return results

    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "event": "fetch_failed",
                    "clinic_id": clinic_id,
                    "error": str(e),
                }
            )
        )
        return Response(
            content='{"error":"Internal server error"}',
            status_code=500,
            media_type="application/json",
        )
    finally:
        conn.close()


# below are code snippets to disable fastAPI logs, so that our custom logs are visible upfront in terminal
logging.getLogger("uvicorn").handlers.clear()
logging.getLogger("uvicorn.error").handlers.clear()
logging.getLogger("uvicorn.access").handlers.clear()

logging.getLogger("uvicorn").propagate = False
logging.getLogger("uvicorn.error").propagate = False
logging.getLogger("uvicorn.access").propagate = False
