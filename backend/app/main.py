# app/main.py
from fastapi import FastAPI, UploadFile, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from app.routers import schema_router
from app.agents import schema_agent, mapping_agent
from app.agents.mapping_agent import invoke_mapping, download_mapping_file
from fastapi import FastAPI, UploadFile, Form, File
from typing import Optional
from app.agents.mapping_agent import parse_excel, map_bronze_to_silver
app = FastAPI(title="Schema DDL Generator API")

# ----------------------------
# CORS: allow React frontend
# ----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change to frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Logging middleware
# ----------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"[REQUEST] {request.method} {request.url}")
    response = await call_next(request)
    print(f"[RESPONSE] {response.status_code}")
    return response

# ----------------------------
# Include existing schema routes
# ----------------------------
app.include_router(schema_router.router, prefix="/api", tags=["Schema"])

# ----------------------------
# Endpoint: Analyze Bronze table
# ----------------------------
@app.post("/api/generate-schema/")
async def analyze_bronze(
    file: UploadFile,
    target: str = Form(...),
    table_name: str = Form("uploaded_table")
):
    try:
        file_bytes = await file.read()
        state = {"file": file_bytes, "target": target, "table_name": table_name}
        result = schema_agent.invoke(state)
        return result
    except Exception as e:
        return {"error": str(e)}

# ----------------------------
# Endpoint: Map Bronze → Silver
# ----------------------------
@app.post("/api/map_bronze_to_silver/")
async def map_bronze_to_silver_endpoint(
    bronze_file: UploadFile = File(...),
    bronze_filename: str = Form(...),
    bronze_name: str = Form(...),
    silver_file: Optional[UploadFile] = File(None),  # make optional
    silver_filename: Optional[str] = Form(None),     # make optional
    silver_name: str = Form(...),
):
    # Read Bronze DataFrame
    bronze_bytes = await bronze_file.read()
    bronze_df = parse_excel(bronze_bytes, bronze_filename)

    # Read Silver DataFrame if provided, else create automatically
    if silver_file is not None:
        silver_bytes = await silver_file.read()
        silver_df = parse_excel(silver_bytes, silver_filename)
    else:
        # Auto-create Silver: just copy Bronze column names & default types
        silver_df = bronze_df.copy()
        # optionally you can allow LLM to suggest new columns or types

    result = map_bronze_to_silver(bronze_df, silver_df, bronze_name, silver_name)
    return result
    

# --- Download Excel Mapping File ---
@app.get("/api/download_mapping/{file_name}")
async def download_excel(file_name: str):
    return download_mapping_file(file_name)

# ----------------------------
# Health check (optional)
# ----------------------------
@app.get("/api/health")
async def health_check():
    return {"status": "ok"}
