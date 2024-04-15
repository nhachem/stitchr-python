import pyspark
import os
import sys
from pyspark.sql.types import *
from pystitchr.util.spark_utils import *
# from pystitchr.derivation_service import *
from pystitchr.derivation.service import *
from pystitchr.registry.service import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import substr, col, collect_list, from_json, regexp_replace, size, split, struct, to_json, trim, when
import json

""" setting up the path to include Stitchr and other project related imports"""
#sys.path.append(os.environ['STITCHR_ROOT'] + '/pyspark-app/app')
#sys.path.append("/Users/nabilhachem/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars")
#print(sys.path)

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
spark.sparkContext.setLogLevel('WARN')
print("Spark Version:", spark.sparkContext.version)
# spark.sql.legacy.parquet.int96RebaseModeInWrite = CORRECTED
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

rs = RegistrySchema()
rs.initialize_datacatalog_views()
q_object_name = 'q2'
q_object_ref = 'file0__tpcds__q2'

r_df, b_df = get_all_query_dependencies(q_object_ref, spark.table("dc_datasets") )
display_df(r_df)
display_df(b_df)


def init_base_table(table_spec: dict) -> DataFrame:
    print(table_spec["table_identifier"])
    _r = RegistrySchema()
    csv_schema =  _r.get_schema(_r.get_schema_df(1))
    print(csv_schema)
    (spark.read.format(table_spec["data_source"])
     .schema(csv_schema)
     .option("inferSchema", False)
    # .option("header", True)
     .option("delimiter", f"{table_spec['delimiter']}")
     .load(f"{table_spec['file_path']}.dat")
     .write.format("parquet").mode("overwrite").saveAsTable(table_spec["table_identifier"])
    )
    return spark.table(table_spec["table_identifier"])


def init_table(table_spec: dict, object_ref: str) -> DataFrame:
    print(table_spec["table_identifier"])
    _r = RegistrySchema()
    csv_schema =  _r.get_schema(_r.get_schema_by_object_ref(object_ref))
    print(csv_schema)
    (spark.read.format(table_spec["data_source"])
     .schema(csv_schema)
     .option("inferSchema", False)
    # .option("header", True)
     .option("delimiter", f"{table_spec['delimiter']}")
     .load(f"{table_spec['file_path']}.dat")
     .write.format("parquet").mode("overwrite").saveAsTable(table_spec["table_identifier"])
    )
    return spark.table(table_spec["table_identifier"])


from pystitchr.util.spark_utils import *
import logging


def derive_query(input_df: DataFrame, object_ref: str, logging_level: str = "ERROR"): # -> DataFrame:
    """
    assumes dc has been instantiated. So we have access to
    dc_catalog,
    we iterate over each dependency and start with the ones that are ready to initialize
    and progress until all are initialized as temp views are
    ...
    @param input_df:
    @type input_df:
    @param object_ref:
    @type object_ref:
    @param logging_level:
    @type logging_level:
    @return:
    @rtype:
    """
    # logger = LoggerSingleton().get_logger("run_pipeline")
    import importlib
    # ToDo: control logging level globally from outside
    logging.basicConfig(level=logging_level)
    logger = logging.getLogger("logging")

    # ToDo: control logging level globally from outside
    # logging.basicConfig(level=logging_level)
    # log = logging.getLogger("logging")

    # df_p = input_df

    _dependencies_df, _base_df = get_all_query_dependencies(object_ref, spark.table("dc_datasets"))

    # first initialize the base objects (assuming files)
    template = f""""""
    for r in _base_df.collect():
        # NH object specs would be built from catalog info but for testing we keep it simple
        # path should be a map from the catalog info
        format = "csv"
        delimiter = "|"
        root_folder = "/Users/nabilhachem/data/demo"
        base_object_ref = r["base_object_ref"]
        base_object_params = base_object_ref.split('__')
        print(base_object_params)
        table_spec = {
            "table_identifier": f"{base_object_params[2]}",
            "file_path": f"{root_folder}/{base_object_params[1]}/{base_object_params[2]}",
            "data_source": f"{format}",
            "delimiter": f"{delimiter}"
        }
        # source__container_object_name
        print(type(table_spec))
        # table_df = init_base_table(table_spec)
        table_df = init_table(table_spec, base_object_ref)
        table_df.printSchema()

    logger.debug(f"initial dependency set count is {_dependencies_df.count()}")
    iterate = True
    iteration = 1
    while iterate:
        logger.info(iteration)
        # step 1. get the set of objects we can instantiate
        derivable_df = _dependencies_df.groupBy("object_ref").count().filter("count = 1")
        derivable_set = [r["object_ref"] for r in derivable_df.collect()]
        print(derivable_set)
        # iterate over the set and instantiate each
        for _ref_object in derivable_set:
            # instantiate by creating a temp view
            print(f"creating a view for {_ref_object}")
        # then delete the instantiated entries from the dependencies df and repeat until all done
        # not really needed as we have a small list
        # derivable_set_broadcasted = spark.sparkContext.broadcast(derivable_set)
        _dependencies_df = _dependencies_df.filter(~_dependencies_df.depends_on.isin(derivable_set))
        logger.debug(f"dependency set count after {iteration} is {_dependencies_df.count()}")
        if _dependencies_df.count() == 0:
            iterate = False
    return


derive_query(get_empty_df(["hello"]), q_object_ref)

# now we can perform a select * from {q_object_name}

'''
display_sql("""select * 
                from dc_datasets
                where 1=1
                and format not in ('postgresql') and object_name like '%q2%'
            """)
'''

from pystitchr.registry.service import *

r = RegistrySchema()
r.initialize_datacatalog_views()
r.get_schema_df(1)

sc = r.get_schema(r.get_schema_df(1))
print(sc)

# display_sql("""select * from file0__tpcds__q2""")