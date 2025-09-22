from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import schema_router

app = FastAPI(title="Schema DDL Generator API")

# Allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(schema_router.router, prefix="/api", tags=["Schema"])
