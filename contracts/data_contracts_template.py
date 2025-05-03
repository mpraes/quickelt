"""
data_contracts.py
-----------------

Contratos de dados para ingestão de diferentes fontes (API, CSV, Database, etc.)
Data contracts for data ingestion from different sources (API, CSV, Database, etc.)

⚡ Organização:
- API Contracts
- CSV Contracts
- Database Contracts
- WebScraping Contracts
"""

# Base
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

# -------------------------------
# API Contracts
# -------------------------------

class ProductAPIContract(BaseModel):
    id: int
    name: str
    price: float
    created_at: datetime
    active: bool

class UserAPIContract(BaseModel):
    user_id: int
    username: str
    registered_at: datetime
    is_active: bool

# -------------------------------
# CSV Contracts
# -------------------------------

class ProductCSVContract(BaseModel):
    product_id: int
    product_name: str
    category: Optional[str]
    price: float
    available: bool

class SaleCSVContract(BaseModel):
    sale_id: int
    product_id: int
    quantity: int
    sale_date: datetime
    total_value: float

class UserCSVContract(BaseModel):
    user_id: int
    username: str
    email: str
    signup_date: datetime
    is_active: bool

# -------------------------------
# Database Contracts
# -------------------------------

class CustomerDatabaseContract(BaseModel):
    customer_id: int
    customer_name: str
    registration_date: datetime
    active: bool

# -------------------------------
# WebScraping Contracts
# -------------------------------

class ArticleWebScrapingContract(BaseModel):
    article_id: int
    title: str
    published_at: datetime
    url: str
