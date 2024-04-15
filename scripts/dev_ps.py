import pyspark
import os
import sys
from pyspark.sql.types import *
from pystitchr.util.spark_utils import *
from pystitchr.util.utils import *
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

# query = "select * from foo union select * from bar"
query_ = f"""select * from (WITH avg_students AS (
            SELECT district_id, AVG(students) as average_students
            FROM hello
            GROUP BY district_id),
            avg_students1 AS (
            SELECT district_id, AVG(students) as average_students
            FROM hello1 h1, hello2 h2
            where h1.id = h2.id
            GROUP BY district_id)
            SELECT s.school_name, s.district_id, avg.average_students
            FROM 
            (select * from schools1 where dept in (select dept from all_departments)) s
            JOIN (select * from avg_students union select * from avg_students1) avg
            ON s.district_id = avg.district_id
            )
            """
query0 = f"""WITH avg_students AS (
            SELECT district_id, AVG(students) as average_students
            FROM hello
            GROUP BY district_id),
            avg_students1 AS (
            SELECT district_id, AVG(students) as average_students
            FROM hello1 h1, hello2 h2
            where h1.id = h2.id
            GROUP BY district_id)
            SELECT s.school_name, s.district_id, avg.average_students
            FROM 
            (select * from schools1 where dept in (select dept from all_departments)) s
            JOIN (select * from avg_students union select * from avg_students1) avg
            ON s.district_id = avg.district_id
            """
query1 = """
select d_week_seq1
       ,round(sun_sales1/sun_sales2,2) sun
       ,round(mon_sales1/mon_sales2,2) mon
       ,round(tue_sales1/tue_sales2,2) tue
       ,round(wed_sales1/wed_sales2,2) wed
       ,round(thu_sales1/thu_sales2,2) thu
       ,round(fri_sales1/fri_sales2,2) fri
       ,round(sat_sales1/sat_sales2,2) sat
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from file0__tpcds__wswscs wswscs,file3__tpcds__date_dim date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from file0__tpcds__wswscs wswscs
      ,file3__tpcds__date_dim date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1
"""

plan_df = get_dependency_objects(query_, "query")
plan_df.printSchema()
print(plan_df.count())
plan_df.show(truncate=False)

plan_df = get_dependency_objects(query0, "query0")
plan_df.printSchema()
print(plan_df.count())
plan_df.show(truncate=False)


plan_df = get_dependency_objects(query1, "query1")
plan_df.printSchema()
print(plan_df.count())
plan_df.show(truncate=False)

from pystitchr.registry.service import *
rs = RegistrySchema()
rs.initialize_datacatalog_views()
q_object_name = 'q2'
q_object_ref = 'file0__tpcds__q2'

r_df, r1_df = get_all_query_dependencies(q_object_ref, spark.table("dc_datasets") )
display_df(r_df)
display_df(r1_df)

display_sql("""select * 
                from dc_datasets
                where 1=1
                and format not in ('postgresql') and object_name like '%q2%'
            """)
