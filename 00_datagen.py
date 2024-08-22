#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import os
import numpy as np
from datetime import datetime
import dbldatagen as dg
from pyspark.sql import SparkSession
from dbldatagen import DataGenerator
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class DataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def dataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 10000):

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("col1", values=["USD", "EUR", "KWD", "BHD", "GBP", "CHF", "MEX"])
                    .withColumn("col2", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col3", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col4", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col5", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col6", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col7", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col8", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col9", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col10", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col11", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col12", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col13", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col14", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col15", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col16", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col17", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col18", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col19", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col20", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col21", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col22", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col23", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col24", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col25", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col26", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col27", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col28", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col29", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col30", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col31", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col32", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col33", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col34", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col35", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col36", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col37", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col38", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col39", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col40", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col41", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col42", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col43", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col44", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col45", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col46", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col47", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col48", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col49", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col50", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col51", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col52", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col53", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col54", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col55", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col56", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col57", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col58", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col59", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col60", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col61", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col62", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col63", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col64", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col65", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col66", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col67", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col68", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col69", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col70", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col71", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col72", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col73", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col74", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col75", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col76", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col77", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col78", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col79", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col80", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col81", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col82", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col83", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col84", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col85", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col86", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col87", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col88", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col89", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col90", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col91", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col92", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col93", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col94", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col95", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col96", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col97", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col98", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col99", "float", minValue=1, maxValue=10000000, random=True)
                    .withColumn("col100", "float", minValue=1, maxValue=10000000, random=True)
                    )

        df = fakerDataspec.build()

        return df

spark = SparkSession \
    .builder \
    .appName("DATA GENERATION") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

modinDG = DataGen(spark)

STORAGE="s3a://paul-tests-buk-8cfc06be/data/"
sparkDf = modinDG.dataGen()
sparkDf.write.format("parquet").mode("overwrite").save(STORAGE+"pdefusco/modin/data")
#transactionsDf.write.format("json").mode("overwrite").save("/home/cdsw/jsonData2.json")
