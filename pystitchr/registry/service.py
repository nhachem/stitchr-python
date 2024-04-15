


"""
import com.stitchr.core.common.Encoders.{ DataSet, dataSetEncoder , SchemaColumn}
import com.stitchr.util.EnvConfig._ // { baseRegistryFolder, dataCatalogPersistence, dataCatalogSchema, props }
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.util.Convert.config2JdbcProp
import com.stitchr.util.SharedSession.spark
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.functions.{concat, lit, coalesce}
"""
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, concat, coalesce
from pyspark.sql import SparkSession, DataFrame
from pystitchr.util.database.column import *


class RegistrySchema:
    """
    from pyspark.sql.types import *
    from pyspark.sql.functions import lit, col, concat, coalesce
    from pyspark.sql import SparkSession, DataFrame
    from pystitchr.util.database.column import *
    """

    spark = SparkSession.builder.getOrCreate()

    """
        schema_column.csv looks
        [id, column_name,ordinal_position,data_type,numeric_precision,character_maximum_length,is_nullable]
    """
    # NH: need to use?!
    """
    schemaDef: StructType = StructType()
    (schemaDef.append("name", StringType)
    .append("df_type", StringType))
    .append("engine", StringType)
    .append("dataset_container", StringType)
    .append("table_name", StringType)
    .add("storage", StringType)
    .add("relative_url", StringType)
    .add("description", StringType)
    case class DatasetMeta(
      name: String,
      df_type: String,
      engine: String,
      dataset_container: String,
      table_name: String,
      storage: String,
      relative_url: String,
      description: String,
      data_persistence_id: String
  )

    // may use later
    val datasetSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("object_ref", StringType)
    .add("format", StringType)
    .add("storage_type", StringType)
    .add("mode", StringType)
    .add("container", StringType)
    .add("object_type", StringType)
    .add("object_name", StringType)
    .add("query", StringType)
    .add("partition_key", StringType)
    .add("number_partitions", IntegerType)
    .add("schema_id", IntegerType)
    .add("data_persistence_id", IntegerType)
    .add("add_run_time_ref", BooleanType)
    .add("write_mode", StringType)

  /* schemas column defs
     .add("id", IntegerType)
    .add("column_name", StringType)
    .add("column_position", IntegerType)
    .add("column_type", StringType)
    .add("column_precision", IntegerType)
    .add("string_length", IntegerType)
    .add("is_nullable", BooleanType)
   */
  val schemasSchema: StructType = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("column_name", StringType, nullable = false)
    .add("column_position", IntegerType, nullable = false)
    .add("column_type", StringType, nullable = false)
    .add("column_precision", IntegerType, nullable = true) // need to fix null representation
    .add("string_length", IntegerType, nullable = true) // need to fix null representation
    .add("is_nullable", BooleanType, nullable = false)
    //.add("c_type", StringType)
    /* had problem decyphering nulls in integer fields and also boolean....
had to edit and replace nulls with -q for now and bypass the use of boolean ype
   */
    /* val schemasDF = spark.read
    .schema(schemasSchema)
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .load(baseFolder + "schema_column.csv")
    .cache() */

  //this has an issue working with nulls!!
    /*  val schemasDF = spark.read.format("csv")
    .option("header", true)
    .option("nullValue", "")
    .load(baseFolder + "schema_columns.csv").cache()
   */

    /* we should try to abstract further with a common api?!
   * to do we would use DS and not DF to enforce proper typing?! */
    /*
    NH: 7/26/2019. the schema_column.string_length is also used to capture the decimal precision of a numeric.
    We need to adjust the names of the attributes as the current naming is wrong and leads to confusion
   */ 
    """

    base_registry_folder = '/Users/nabilhachem/git/nhachem/pystitchr/registry_data/'
    objectRefDelimiter = '__'
    spark = SparkSession.builder.getOrCreate()
    """
    (dataSetDF,
    schemasDF,
    dataPersistenceDF,
    batchGroupDF,
    batchGroupMembersDF
    ): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) =
    """
    dfP = (spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ",")
                .load(f"{base_registry_folder}data_persistence.csv")
            )

    dfP0 = dfP.withColumnRenamed("id", "pers_id").select("pers_id", "name")
    dfD = (spark.read
            #.schema(datasetSchema)
            .format("csv")
            .option("header", True)
            .option("quote", "\"")
            .option("multiLine", True)
            .option("inferSchema", "True")
            .option("delimiter", ",")
            .load(f"{base_registry_folder}dataset.csv")
            .select("id",
                "format",
                "storage_type",
                "mode",
                "container",
                "object_type",
                "object_name",
                "query",
                "partition_key",
                "number_partitions",
                "schema_id",
                "data_persistence_id",
                "add_run_time_ref",
                "write_mode",
                "object_key"
            )
        )
    #convoluted but fine for now... maybe better to use cast straight in the select above?
    dataset_df = (dfD.withColumn("id_", dfD["id"])
                    # .cast(IntegerType))
                    .drop("id")
                    .withColumnRenamed("id_", "id")
                    .join(dfP0, dfD["data_persistence_id"] == dfP0["pers_id"], "inner")
                    .withColumn ("object_ref",
                                 coalesce(dfD["object_key"], concat(dfP0["name"], lit(objectRefDelimiter),
                                                dfD["container"], lit(objectRefDelimiter), dfD["object_name"])))
                    .select("id",
                            "object_ref",
                            "format",
                              "storage_type",
                              "mode",
                              "container",
                              "object_type",
                              "object_name",
                              "query",
                              "partition_key",
                              "number_partitions",
                              "schema_id",
                              "data_persistence_id",
                              "add_run_time_ref",
                              "write_mode")
    )

    """select id,
                | name,
                | persistence_type,
                | storage_type ,
                | host,
                | port,
                | db,
                | "user",
                | pwd,
                | driver,
                | fetchsize,
                | sslmode,
                | db_scope
    """
    data_persistence_df = dfP

    schema_df = (spark.read
              #.schema(schemasSchema)
              .format("csv")
              .option("header", "True")
              .option("inferSchema", "True")
              .option("delimiter", ",")
              .load(f"{base_registry_folder}schema_column.csv")
    )
    # .cache(),

    # NH: maybe will add in V0.2 but files are only for demo purposes and are not transactional
    # this is a placeholder for now...
    batch_group_df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .load(f"{base_registry_folder}batch_group.csv")
                         )
    batch_group_members_df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .load(f"{base_registry_folder}batch_group_members.csv")
        )

    group_list_df = (batch_group_df
        .join(batch_group_members_df, batch_group_df["id"] == batch_group_members_df["group_id"])
        .join(dataset_df, dataset_df["id"] == batch_group_members_df["dataset_id"])
        .select("group_id", "name", "dataset_id", "object_name", "data_persistence_id", "object_ref")
    )

    # val extendedDataSetDs: Dataset[ExtendedDataSet] =
    #  dataSetDS.map{ r: DataSet => extendedFromDataSet(r) }

    def initialize_datacatalog_views(self):
        self.dataset_df.createOrReplaceTempView("dc_datasets")
        self.schema_df.createOrReplaceTempView("dc_schema_columns")
        self.data_persistence_df.createOrReplaceTempView("dc_data_persistence")
        self.batch_group_df.createOrReplaceTempView("dc_batch_group")
        self.batch_group_members_df.createOrReplaceTempView("dc_batch_group_members")

    def get_schema_by_object_ref(self, object_ref: str, delim: str = "__") ->  DataFrame:
        object_params = object_ref.split(delim)
        return (self.spark.sql(f"""select s.id, column_name, column_position, column_type, column_precision, string_length, is_nullable 
                                from dc_datasets d, dc_schema_columns s, dc_data_persistence dp
                                where d.schema_id= s.id
                                and container = '{object_params[1]}'
                                and object_name = '{object_params[2]}'
                                and d.data_persistence_id = dp.id
                                and dp.name = '{object_params[0]}'
                                order by column_position
                                """
                          )
                )

    def get_schema_df(self, schema_id: int) ->  DataFrame:
        return (self.schema_df
                .filter(f"id = '{schema_id}' ")
                .orderBy("column_position")
                .select("id", "column_name", "column_position", "column_type", "column_precision", "string_length", "is_nullable")
                # .as[SchemaColumn]
                )

    def get_schema(self, schema_df: DataFrame) -> StructType:
        # print(schema_df.count())
        # print(self.schema_df.collect())
        #for r in schema_df.collect():
        #    print(r["column_type"])
        if schema_df.count() == 0:
            return StructType(None) # .asInstanceOf[StructType] // not something encouraged in Scala... maybe change to Option/Some

        c = ColumnTypeMap()
        schema = (
            StructType([c.switch(s["column_type"], s["column_name"]) for s in schema_df.collect()])
        )
        #, s.column_precision, s.string_length),# s.is_nullable
        # print(schema)
        return schema
