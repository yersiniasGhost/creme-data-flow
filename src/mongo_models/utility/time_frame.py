from pydantic import BaseModel


class TimeFrame(BaseModel):
    start: str
    end: str
