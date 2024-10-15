from pydantic import BaseModel
from typing import List, Optional

# Schema for a single product
class ProductBase(BaseModel):
    channel_id: int
    channel_username: str
    channel_title: str
    product_id: int
    product_name: str
    price_in_birr: Optional[float] = None  # Ensure this is Optional to handle None values
    
class Product(ProductBase):
    # This can be used for response models if needed
    pass

class ProductCreate(ProductBase):
    # This can be used for creating new products, you can add validations if necessary
    pass

class ProductResponse(BaseModel):
    products: List[Product]  # List of products in response

    class Config:
        orm_mode = True  # Enable ORM mode to work with SQLAlchemy models


# Example for other potential schemas
class Channel(BaseModel):
    channel_id: int
    channel_name: str
    channel_title:str

class ChannelResponse(BaseModel):
    channels: List[Channel]


# Sample Pydantic model for detection results
class Detection(BaseModel):
    id: int
    product_id: int
    confidence: float
    image_path: str