from fastapi import FastAPI
import rules

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}
