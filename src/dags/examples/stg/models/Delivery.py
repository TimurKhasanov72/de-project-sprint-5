import datetime
from typing import Dict, Any
from pydantic import BaseModel


class DeliveryObj(BaseModel):    
    delivery_id: str
    delivery_ts: datetime.datetime
    payload:  Dict[str, Any]