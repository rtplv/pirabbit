from app.models.base_model import BaseModel
from playhouse.postgres_ext import *


# PostgreSQL example
class _ExampleBaseModel(BaseModel):
    class Meta:
        schema = 'public'


class User(_ExampleBaseModel):
    email = CharField(max_length=255)

    class Meta:
        db_table = 'persons'
