# main.py
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from app.database import get_db  # Adjusted import
from app.crud import get_product_by_name  # Ensure this function is correctly imported
from app.schemas import ProductBase, ProductResponse
app = FastAPI()

# Mount static files directory
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Set up templates and static files
templates = Jinja2Templates(directory="app/templates")




# Root endpoint to serve the HTML page
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Endpoint for "Get Raw Data"
@app.get("/raw-data")
def get_raw_data():
    # Replace with actual data retrieval logic later
    return {"message": "Raw data placeholder"}

# Endpoint for "Clean Raw Data"
@app.get("/clean-raw-data")
def clean_raw_data():
    # Replace with actual cleaning logic later
    return {"message": "Cleaned raw data placeholder"}

# Endpoint for "Transform Data"
@app.get("/transform-data")
def transform_data():
    # Replace with actual transformation logic later
    return {"message": "Data transformation placeholder"}

# Endpoint for "Load Data"
@app.get("/load-data")
def load_data():
    # Replace with actual loading logic later
    return {"message": "Data loading placeholder"}

# Endpoint for "Explore Data"
@app.get("/explore-data")
def explore_data():
    # Replace with actual exploration logic later
    return {"message": "Data exploration placeholder"}

# Endpoint for "Search by Product"
from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List

@app.get("/search-product/", response_model=List[ProductBase])
def search_product(product_name: str, db: Session = Depends(get_db)):
    print(f"Search requested for: {product_name}")  # Debug log
    try:
        # Retrieve products by name, including associated channel info
        products = get_product_by_name(db=db, product_name=product_name)

        # Log the retrieved products
        if products:
            product_dicts = [
                {
                    "channel_id": p.channel.channel_id,  # Include channel_id
                    "channel_username": p.channel.channel_username,  # Include channel_username
                    "channel_title": p.channel.channel_title,  # Include channel_title
                    "product_id": p.product_id,
                    "product_name": p.product_name,
                    "price_in_birr": p.price_in_birr,
                }
                for p in products
            ]
            print(f"Products found: {product_dicts}")  # Log the products found
            return product_dicts  # Return the structured response
        else:
            raise HTTPException(status_code=404, detail="Product not found")

    except Exception as e:
        print(f"Error: {e}")  # Log the exception
        raise HTTPException(status_code=500, detail="Internal Server Error")


