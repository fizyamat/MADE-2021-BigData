from pyspark import SparkContext
from pyspark.sql import SparkSession
from SparkNestedCRUD import update_df
from pyspark.sql.functions import *

def test_check_results():
    
    sc = SparkContext()
    spark = SparkSession(sc)

    input_df = \
        spark.range(0,1) \
            .select(
                struct(
                    lit("bmw").alias("brand"),
                    lit("220i").alias("model"),
                    struct(
                        lit("rear").alias("wheel_drive"),
                        lit("automatic").alias("gear_box")
                    ).alias("transmission")
                ).alias("car")
            )

    input_df.printSchema()
    input_df.show(1, False)
    updates = {
        "car.brand": lit("audi"),
        "car.transmission.wheel_drive": lit("all"),
        "car.color": lit("black"),
        "owner.first_name": lit("Ivan"),
        "owner.last_name": lit("Ivanov"),
    }

    valid_results = \
        spark.range(0,1) \
            .select(
                struct(
                    lit("audi").alias("brand"),
                    lit("220i").alias("model"),
                struct(
                    lit("all").alias("wheel_drive"),
                    lit("automatic").alias("gear_box")
                ).alias("transmission"),
                lit("black").alias("color")
            ).alias("car"),
        struct(
            lit("Ivan").alias("first_name"),
            lit("Ivanov").alias("last_name")
        ).alias("owner")
    )
    valid_results.printSchema()
    check_results(valid_results)
    
    check_results(update_df(input_df, updates))
    sc.stop()


def check_results(df):
    assert set(df.columns) == set(["car", "owner"])
    assert set(df.select(col("car.*")).columns) == set(["brand",
        "model", "transmission", "color"])
    assert set(df.select(col("owner.*")).columns) == set(["first_name",
        "last_name"])
    assert df.filter(col("`car`.`transmission`.`wheel_drive`") ==
        "all").count() == 1
    assert df.filter(col("`car`.`transmission`.`gear_box`") ==
        "automatic").count() == 1
    print("All tests passed!")
