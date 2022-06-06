from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import argparse
from pathlib import Path
import shutil
import os

DATA_PATH = "data/biographies.list.gz"

def RenameCSV(data_location: str) -> None:
    """
    Group the files in one CSV
    """
    files = os.listdir(data_location)
    csv_file = [Path(data_location, x) for x in files if x.endswith(".csv")][0]
    os.rename(csv_file, data_location.rstrip('/') + ".csv")

    shutil.rmtree(data_location)


def main(spark: SparkSession, input_path : str, output_path : str) -> None:
    """
    Main function
    """

    # Read the data from the file and create a DataFrame
    df = spark.read.text(input_path)

    # Split the lines into words
    words = df.filter(df.value.startswith("BG"))\
              .select(f.explode(f.split(df.value, " "))
              .alias("word"))

    # Count the words
    wordCounts = words.groupBy("word").count().orderBy("count", ascending=False)

    # write one csv file for the results (less efficient but the data is still small)
    wordCounts.coalesce(1).write.csv(path=output_path, header = "true", mode="overwrite")

    # For more visibility, write the results in a .csv and not in a nested folder
    RenameCSV(output_path)


if __name__ == "__main__":

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--input", help="Input file path", default=DATA_PATH)
    argparser.add_argument("--output", help="Output file path", default="output/wordCount")
    args = argparser.parse_args()

    if args.output.endswith(".csv"):
        args.output = args.output.rstrip(".csv")

    main(SparkSession.builder.getOrCreate(), args.input, args.output)