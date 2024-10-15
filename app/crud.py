# crud.py
from sqlalchemy.orm import Session
from app.models import TransformedProduct, Channel # Make sure this is your correct model

def get_product_by_name(db: Session, product_name: str):
    try:
        results = db.query(TransformedProduct).join(Channel).filter(
            TransformedProduct.product_name.ilike(f"%{product_name}%")
        ).all()
        print(f"Query results: {results}")  # Log the raw results
        return results
    except Exception as e:
        print(f"Database query error: {e}")  # Log any DB query errors
        raise