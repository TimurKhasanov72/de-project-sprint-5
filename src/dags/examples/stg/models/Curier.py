from pydantic import BaseModel


class CurierObj(BaseModel):
    id: str
    name: str