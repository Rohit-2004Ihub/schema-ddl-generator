from fastapi import APIRouter, UploadFile, Form
from app.agents.schema_agent import invoke

router = APIRouter()

@router.post("/generate-schema")
async def generate_schema(file: UploadFile, target: str = Form(...)):
    """
    Receives Excel file and target database type, returns inferred columns + DDL.
    """
    content = await file.read()
    state = {"file": content, "target": target}
    try:
        result = invoke(state)
        return result
    except Exception as e:
        return {"error": str(e)}
