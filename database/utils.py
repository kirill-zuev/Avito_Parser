from pydantic import BaseModel


class Item(BaseModel):
    address: str = ''
    radius: float = 0.0