from datetime import datetime
from peewee import Model, BigAutoField, DateField
from app import db


class BaseModel(Model):
    id = BigAutoField(unique=True, primary_key=True)
    created_at = DateField(null=False, default=datetime.now())
    updated_at = DateField(null=False, default=datetime.now())

    class Meta:
        database = db
