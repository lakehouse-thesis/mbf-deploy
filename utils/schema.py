## THIS FILE IS SHARED WITH THE STATISTIC SERVICE TO MAKE SURE THEY HAVE THE SAME BEHAVIOUR
# from ..services.helper import SchemaConstructor
from uuid import uuid4


from pyspark.sql.types import *
class Field:
    def __init__(self, name, typ, description, nullable):
        self.name = name 
        self.typ = typ
        self.description = description
        self.nullable=nullable
    @classmethod
    def fromdict(self,dict: dict):
        if dict.get('name'):
            return Field(dict['name'],dict['typ'],dict['description'],True)
        elif dict.get('fieldname'):
            return Field(dict['fieldname'],dict['datatype'],dict['description'],True)
        else:
            return Field('','','',True)
        # self.name = dict['fieldname']
        # self.typ = dict['datatype']
        # self.description = dict['description']
        # self.nullable=True# dict['nullable']
    
    @classmethod
    def from_StructField(self, structfield: StructField):
        return Field(structfield.name, structfield.dataType.simpleString(),structfield.metadata, structfield.nullable)
    
    def get_datatype(self):
        if (self.typ == "STRING"):
            return StringType()
        elif (self.typ == "INT"):
            return IntegerType()
        elif (self.typ == "FLOAT"):
            return FloatType()
        elif (self.typ == "BOOLEAN"):
            return BooleanType()
        elif (self.typ == "DATE"):
            return DateType()

        # default
        return StringType()
    
    def get_struct_field(self):
        return StructField(self.name, self.get_datatype(),self.nullable)


    def to_dict(self):
        return {
             'name' : self.name ,
        'typ' : self.typ,
       'description':  self.description,
       'nullable': self.nullable,
        }


class Table:
    def __init__(self, project, typ, name, stream_id, description="", fields=[], tmp_alias=None):
        self.id = uuid4().hex
        self.project = project
        self.typ = typ
        self.name = name
        self.description = description
        self.schema = fields
        self.stream_id = stream_id
        self.path = self.get_hdfs_path()
        # self.schema = SchemaConstructor(fields)

    def get_hdfs_path(self):
        return "/data/{project}/{table_type}/{table_name}".format(
            project=self.project, table_type=self.typ, table_name=self.name
        )

    def get_mongo_name(self):
        # TODO: editable
        return f'{self.project}_{self.typ}_{self.name}'

    def get_checkpoint_location(self):
        return f"/tmp/delta/{self.project}/{self.typ}_{self.name}_{self.id}/_checkpoints/"

    @classmethod
    def from_dict(self, table: dict):
        # TODO edit lại description và schema
        f_table = Table(
            table.get("project"), table.get("typ"), table.get("name"), table.get("stream_id"), "", table.get('schema'), table.get("alias")
        )
        if table.get('id'):
            f_table.id = table.get('id')
            
        return f_table

    def get_schema(self):
        return SchemaConstructor(self.schema)

    def to_dict(self):
        return {
            "id": self.id,
            "project": self.project,
            "typ": self.typ,
            "name": self.name,
            "stream_id": self.stream_id,
            "description": self.description,
            "schema": self.schema,
            "path": self.path,
        }

    @classmethod
    def get_delta_name(self, project, typ, name):
        return f"/data/{project}/{typ}/{name}"
# THIS FILE IS THE SAME WITH UTILS

def SchemaConstructor( fields):
    schema = StructType(list(map(
        lambda field: field.get_struct_field(),fields
        )))
    return schema

def SchemaParser(schema: StructType):
    return list(map(lambda field: Field.from_StructField(field).to_dict(),schema))



req={
    "project_name":"MBF",
    "table_name":"t3",
    "schema":[
  {
    "fieldname": "ID",
    "datatype": "Float",
    "description": ""
  }
]
}


table = Table.from_dict(req)

