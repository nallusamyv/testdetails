# Description       : This Glue job executes DELETE, UPDATE or RESTORE SQLs against delta lake tables


# Limitations       : * delete or update predicates in job parameter shouldn't exceed
#                       4000 chars in length.
#                     * to update fields/columns of timestamp or date type - use
#                       constant instead of current_timestamp or current_date

# Instructions      : * specify mode=update to create new partitions or update existing rows
#                     * specify mode=delete to delete existing partitions or rows
#                     * specify mode=restore to restore a delta tabe to a specific datetime
#                     * for delete mode: specify valid value for --delete_predicate job parm.
#                       total # of required valid job parms for delete = 5 (excluding mode job parm)
#                     * for update mode: specify valid values for --update_cond_predicate
#                       & --update_set_predicate job parms.
#                       total # of required valid job parms for update = 6 (excluding mode job parm)
#                     * for restore operation: specify valid values for --restore_timestamp
#                       & --partition_by job parms
#                       * --restore_timestamp should be UTC value
#                       * --partition_by should be a comma seperated value for partitioned tables. For non-partitioned table, leave it as `None`
#                       * to revert back to older schema, --delta_table_schema_override value should be  set to `true`
#                       total # of required valid job parms for restore = 6 (excluding mode job parm)
#                     * valid values for --delta_table_name, --delta_table_schema, --athena_table &
#                       --athena_table_schema should be entered for all modes

# Examples          : job parm key: job parm value
#                     --mode: delete/update/restore
#                     --delta_table_name: policy_detail
#                     --athena_table: policy_detail
#                       ** update_set_predicate should be a valid JSON
#                       ** restore_timestamp should be UTC value
#                     --partition_by: part_yr,src_sys_cd
#                       ** partition_by should be comma seperated value


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from delta.tables import *
from pyspark.sql.functions import *
import awswrangler as wr
import boto3
import json
import pandas as pd
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "mode",
                                     "delta_table_path", "delta_table_name", "delta_table_schema",
                                     "athena_table", "athena_table_schema",
                                     "delete_predicate", "update_cond_predicate", "update_set_predicate",
                                     "restore_timestamp", "partition_by",
                                     "delta_table_schema_override", "user_metadata",
                                     "kms_key", "output_results_location"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
session = boto3.Session()

mode = args["mode"]
delta_table_path = args["delta_table_path"]
delta_table_name = args["delta_table_name"]
delta_table_schema = args["delta_table_schema"]
athena_table_schema = args["athena_table_schema"]
athena_table = args["athena_table"]
delete_predicate = args["delete_predicate"]
update_cond_predicate = args["update_cond_predicate"]
update_set_predicate = args["update_set_predicate"]
restore_timestamp = args["restore_timestamp"]
partition_by = args["partition_by"]
delta_table_schema_override = args["delta_table_schema_override"]
user_metadata = args["user_metadata"]
kms_key = args["kms_key"]
output_results_location = args["output_results_location"]

logger = glueContext.get_logger()
logger.info(f"mode={mode}")
logger.info(f"delta_table_path={delta_table_path}")
logger.info(f"delta_table_name={delta_table_name}")
logger.info(f"delta_table_schema={delta_table_schema}")
logger.info(f"athena_table={athena_table}")
logger.info(f"athena_table_schema={athena_table_schema}")
logger.info(f"delete_predicate={delete_predicate}")
logger.info(f"update_cond_predicate={update_cond_predicate}")
logger.info(f"update_set_predicate={update_set_predicate}")
logger.info(f"restore_timestamp={restore_timestamp}")
logger.info(f"partition_by={partition_by}")
logger.info(f"delta_table_schema_override={delta_table_schema_override}")
logger.info(f"user_metadata={user_metadata}")
logger.info(f"kms_key={kms_key}")
logger.info(f"output_results_location={output_results_location}")

print(f"mode={mode}")
print(f"delta_table_path={delta_table_path}")
print(f"delta_table_name={delta_table_name}")
print(f"delta_table_schema={delta_table_schema}")
print(f"athena_table={athena_table}")
print(f"athena_table_schema={athena_table_schema}")
print(f"delete_predicate={delete_predicate}")
print(f"update_cond_predicate={update_cond_predicate}")
print(f"update_set_predicate={update_set_predicate}")
print(f"restore_timestamp={restore_timestamp}")
print(f"partition_by={partition_by}")
print(f"delta_table_schema_override={delta_table_schema_override}")
print(f"user_metadata={user_metadata}")
print(f"kms_key={kms_key}")
print(f"output_results_location={output_results_location}")

#Legacy Calendar Compatability
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead","CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","CORRECTED")

spark.conf.set("spark.databricks.delta.commitInfo.userMetadata",
               f"{user_metadata}")


def perform_delete_or_update(dml_action=None):
    """
    function to perform the following
        1. delete or update rows in the targeted delta table
        2. generate symlink mainfest
        3. msck repair the targeted Athena table
    """

    print(f"start of perform_delete_or_update for {dml_action}")
    if dml_action == "delete":
        deltaTable.delete(delete_predicate)
    elif dml_action == "update":
        deltaTable.update(
            condition=update_cond_predicate, set=update_set_predicate)

    deltaTable.generate("symlink_format_manifest")

    msck_query = f"""
    MSCK REPAIR TABLE {athena_table}
    """

    print(f"msck_query={msck_query}")
    wr.athena.start_query_execution(sql=msck_query,
                                    database=athena_table_schema,
                                    s3_output=output_results_location,
                                    encryption="SSE_KMS",
                                    kms_key=kms_key,
                                    boto3_session=session,
                                    wait=True)

    print(f"end of perform_delete_or_update for dml_action={dml_action} ")

    return


def restore_table():
    """
    function to perform the following
        1. restores targeted delta table to targeted datetime
        2. generate symlink mainfest
        3. msck repair the targeted Athena table
    """

    print(f"start of restore_table")

    dataDF = (
        spark.read
        .format("delta")
        .option("timestampAsOf", restore_timestamp)
        .table(f"{delta_table_schema}.{delta_table_name}")
    )

    # print(dataDF.count())

    if partition_by != "None":
        partition_by_lst = partition_by.split(",")
        partition_by_lst = [partition.strip()
                            for partition in partition_by_lst]
        print(partition_by_lst)
        (
            dataDF.write
            .partitionBy(*partition_by_lst)
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", f"{delta_table_schema_override}")
            .saveAsTable(f"{delta_table_schema}.{delta_table_name}")
        )
    else:
        (
            dataDF.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", f"{delta_table_schema_override}")
            .saveAsTable(f"{delta_table_schema}.{delta_table_name}")
        )

    deltaTable.generate("symlink_format_manifest")

    msck_query = f"""
    MSCK REPAIR TABLE {athena_table}
    """

    print(f"msck_query={msck_query}")
    wr.athena.start_query_execution(sql=msck_query,
                                    database=athena_table_schema,
                                    s3_output=output_results_location,
                                    encryption="SSE_KMS",
                                    kms_key=kms_key,
                                    boto3_session=session,
                                    wait=True)

    print(f"end of restore_table")

    return


def validate_job_parms():

    errors = []
    if mode == "delete" or mode == "update" or mode == "restore":
        if delta_table_name == "None" or delta_table_name == "none" or len(delta_table_name.strip()) == 0:
            errors.append(
                f"invalid input for delta_table_name. >>value={delta_table_name}<<")
        if delta_table_schema == "None" or delta_table_schema == "none" or len(delta_table_schema.strip()) == 0:
            errors.append(
                f"invalid input for delta_table_schema. >>value={delta_table_schema}<<")
        if athena_table == "None" or athena_table == "none" or len(athena_table.strip()) == 0:
            errors.append(
                f"invalid input for athena_table. >>value={athena_table}<<")
        if athena_table_schema == "None" or athena_table_schema == "none" or len(athena_table_schema.strip()) == 0:
            errors.append(
                f"invalid input for athena_table_schema. >>value={athena_table_schema}<<")
        if mode == "delete":
            if delete_predicate == "None" or delete_predicate == "none" or len(delete_predicate.strip()) == 0:
                errors.append(
                    f"invalid input for delete_predicate. >>value={delete_predicate}<< for mode={mode}")
        elif mode == "update":
            if update_cond_predicate == "None" or update_cond_predicate == "none" or len(update_cond_predicate.strip()) == 0:
                errors.append(
                    f"invalid input for update_cond_predicate. >>value={update_cond_predicate}<< for mode={mode}")
            if update_set_predicate == "None" or update_set_predicate == "none" or len(update_set_predicate.strip()) == 0:
               errors.append(
                   f"invalid input for update_set_predicate. >>value={update_set_predicate}<< for mode={mode}")
        elif mode == "restore":
            if restore_timestamp == "None" or restore_timestamp == "none" or len(restore_timestamp.strip()) == 0:
                errors.append(
                    f"invalid input for restore_timestamp. >>value={restore_timestamp}<< for mode={mode}")
            if delta_table_schema_override != "false" and delta_table_schema_override != "true":
                errors.append(
                    f"invalid input for delta_table_schema_override. >>value={delta_table_schema_override}<< for mode={mode}")

    else:
        errors.append(
            f"invalid input for mode. >>value={mode}<<. acceptable values are delete/update/restore")

    print(f"errors={errors}")
    if len(errors) > 0:
        raise ValueError("|".join(errors))

    return


mode = mode.lower()
delta_table_name = delta_table_name.strip()
delta_table_schema = delta_table_schema.strip()
athena_table = athena_table.strip()
athena_table_schema = athena_table_schema.strip()
delete_predicate = delete_predicate.strip()
update_cond_predicate = update_cond_predicate.strip()
update_set_predicate = update_set_predicate.strip()
restore_timestamp = restore_timestamp.strip()
partition_by = partition_by.strip()

validate_job_parms()

print(type(update_set_predicate))
try:
    update_set_predicate = json.loads(update_set_predicate)
except Exception as e:
    if mode == "update":
        print(f"json load failure - {str(e)}")
        raise ValueError(
            f"invalid input for update_set_predicate >>value={update_set_predicate}<<. \
            cannot convert string to python dict.")
print(type(update_set_predicate))

try:
    deltaTable = DeltaTable.forName(
        spark, f"{delta_table_schema}.{delta_table_name}")
    dt_count = spark.sql(
        f"select count(*) from {delta_table_schema}.{delta_table_name}")
    dt_count.show()
    historyDF = deltaTable.history()
    historyDF.show(1000000, False)
except:
    raise ValueError(f"invalid input for delta_table_schema or delta_table_name. \
                        >>value={delta_table_schema}.{delta_table_name}<<. delta table doesn't exist")

if mode == "delete" or mode == "update":
    perform_delete_or_update(mode)
elif mode == "restore":
    restore_table()

dt_count = spark.sql(
    f"select count(*) from {delta_table_schema}.{delta_table_name}")
dt_count.show()
historyDF = deltaTable.history()
historyDF.show(1000000, False)

audit_log = {
    "datamaintenance-glue-job-inputs-audit": {
        "mode": mode,
        "delta_table_name": delta_table_name,
        "delta_table_schema": delta_table_schema,
        "athena_table_schema": athena_table_schema,
        "athena_table": athena_table,
        "delete_predicate": delete_predicate,
        "update_cond_predicate": update_cond_predicate,
        "update_set_predicate": update_set_predicate,
        "restore_timestamp": restore_timestamp,
        "partition_by": partition_by,
        "delta_table_schema_override": delta_table_schema_override,
        "user_metadata": user_metadata,
    }
}

print(audit_log)

job.commit()