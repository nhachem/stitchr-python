from pprint import pprint

from pyspark.sql import DataFrame

# from stitchr.util.stitchr_logging import LoggerSingleton, put_log
from pystitchr.util.simple_logging import log
from pystitchr.util.spark_utils import *
from pystitchr.util.utils import replace_brackets
import logging


# class QueryDerivation:


def get_all_query_dependencies(object_ref: str, dataset_df: DataFrame) -> (DataFrame, DataFrame):
    """
    idea is to build a reachability graph of derived objects starting from the initial object to instantiate
    We use an iterative method that converges and stops when no new non-final vertices are generated.
    Using recursion is more elegant but in python tail recursion is not readily available.
    There are ways to do that for example see

    The counterpart in scala uses tail recursion which is optimized to an iteration but not in python.
    @param object_ref:
    @type object_ref:
    @param dataset_df:
    @type dataset_df:
    @return:
    @rtype:
    """
    _object_ref = object_ref
    iterate = True
    _df_dependencies = get_empty_df(["depends_on", "object_ref"])
    _base_objects_df = get_empty_df(["base_object_ref"])
    _dc_base_objects_df = dataset_df.filter("mode in ('base') and format not in ('postgresql')")
    iteration = 1

    while iterate:
        # print(_object_ref)
        # print(iteration)
        # only focusing on files/spark tables for now
        rows = spark.sql(f"""select query 
                        from dc_datasets
                        where 1=1
                        and format not in ('postgresql') and object_ref = '{_object_ref}'
                    """).collect()

        # can use __getItem__('query') as well
        q_str = rows[0].asDict()['query']
        _query = replace_brackets(q_str)
        # print(_query)
        _df0 = get_dependency_objects(_query, _object_ref)
        pre_count = _df_dependencies.count()
        # add new dependencies building the reachability graph.
        # we exclude need base objects as they are final vertices
        _df_dependencies = (_df_dependencies.unionAll(_df0.exceptAll(dataset_df.filter("mode in ('base')")
                                                .selectExpr("object_ref as depends_on")
                                                .crossJoin(_df0.select("object_ref")))
                                      ).unionAll(spark.sql(f"select '{_object_ref}', '{_object_ref}'")).distinct()
                    )
        _base_objects_df = _base_objects_df.union(_df0.join(_dc_base_objects_df, _dc_base_objects_df["object_ref"] == _df0["depends_on"], "inner").select(_dc_base_objects_df["object_ref"].alias("base_object_ref"))).distinct()
        display_df(_df_dependencies)
        iterate_df = _df_dependencies.select("depends_on").distinct().exceptAll(_df_dependencies.select("object_ref").distinct())
        # print(iterate_df.count())
        iteration += 1
        if iterate_df.count() > 0:
            _object_ref = iterate_df.collect()[0].asDict()['depends_on']
        else:
            iterate = False
    _base_objects_df.show(truncate=False)
    return (_df_dependencies, _base_objects_df)


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

    _dependencies_df, b_df = get_all_query_dependencies(object_ref, spark.table("dc_datasets"))

    # first initialize the base objects (assuming files)
    for r in b_df.collect():
        base_object = r["base_object_ref"]
        print(base_object)

    logger.debug(f"initial dependency set count is {_dependencies_df.count()}")
    iterate = True
    iteration = 1
    while iterate:
        logger.info(iteration)
        # step 1. get the set of objects we can instantiate
        derivable_df = _dependencies_df.groupBy("object_ref").count().filter("count =1")
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


