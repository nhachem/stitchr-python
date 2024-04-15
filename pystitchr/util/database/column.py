from pyspark.sql.types import *
from dataclasses import dataclass

# schema stuff
#@dataclass(frozen=True)
#class SchemaType:
#    dataType: DataType
#    nullable: bool



class ColumnTypeMap:

    # schema stuff
    #@dataclass(frozen=True)
    #class SchemaType:
    #    dataType: DataType
    #    nullable: bool

    #def toSqlType(stype: str, precision: int, scale: int) ->  SchemaType:

    def case_varchar(self, name: str):
        return StructField(name, StringType(), False)

    def case_serial(self, name: str):
        return StructField(name, IntegerType(), False)

    def case_integer(self, name):
        return StructField(name, IntegerType(), False)

    def case_string(self, name: str):
        return StructField(name, StringType(), False)

    def case_character_varying(self, name: str):
        return StructField(name, StringType(), False)

    def case_boolean(self, name: str):
        return StructField(name, BooleanType(), False)

    def case_bool(self, name: str):
        return StructField(name, BooleanType(), False)

    def case_bytes(self, name: str):
        return StructField(name, BinaryType(), nullable = False)

    def case_double(self, name: str):
        return  StructField(name, DoubleType(), nullable = False)

    def case_float(self, name: str):
        return StructField(name, FloatType(), nullable = False)

    def case_long(self, name: str):
        return StructField(name, LongType(), nullable = False)

    def case_decimal(self, name: str):
        # return StructField(name, DecimalType(precision, scale), nullable = False)
        return StructField(name, FloatType(), nullable=False)

    def case_numeric(self, name: str):
        return StructField(name, FloatType(), nullable = False)

    def case_timestamp(self, name: str):
        return StructField(name, TimestampType(), nullable = False)

    def case_time_without_time_zone(self, name: str):
        return StructField(name, TimestampType(), nullable = False)

    def case_date(self, name: str):
        return StructField(name, DateType(), nullable = False)

    def case_otherwise(self, name: str):
        return StructField(name, StringType(), nullable=True)

    def switch(self, case_type: str, name: str, precision: int = None, scale: int = None):

        # need to add attribute passing 
        # return getattr(self, 'case_' + str(case_type), lambda: default)(name)
        return getattr(self, 'case_' + str(case_type), self.case_otherwise)(name) # this also works (*[name])

    #def toSqlType(stype: str, precision: int, scale: int) -> SchemaType:
    #    return SchemaType


"""
test=ColumnTypeMap()

print(test.switch('double', "foo"))

print(test.switch('hello', "bar"))
"""