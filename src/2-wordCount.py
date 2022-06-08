from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import argparse
from pathlib import Path
import shutil
import os


DATA_PATH = "data/biographies.list.gz"
OUTPUT_PATH = "output/wordCount"


class Solution:
    def __init__(self, spark: SparkSession, input_path : str, output_path : str) -> None:
        """
        Main function
        """

        self._spark = spark
        self._input_path = input_path
        self._output_path = output_path

    def run(self) -> None:
        """
        Main function
        """

        # Read the data from the file and create a DataFrame
        df = self._spark.read.text(self._input_path)

        # Split the lines into words
        words = df.filter(df.value.startswith("BG"))\
                  .select(f.explode(f.split(df.value, " "))
                  .alias("word"))

        # Count the words
        wordCounts = words.groupBy("word").count().orderBy("count", ascending=False)

        # write one csv file for the results (less efficient but the data is still small)
        wordCounts.coalesce(1).write.csv(path=self._output_path, header = "true", mode="overwrite")

        # For more visibility, write the results in a .csv and not in a nested folder
        self.reformatOutput()

    def reformatOutput(self) -> None:
        """
        Group the files in one CSV
        """
        if os.path.exists(self._output_path):
            files = os.listdir(self._output_path)
            csv_file = [Path(self._output_path, x) for x in files if x.endswith(".csv")][0]
            os.rename(csv_file, self._output_path.rstrip('/') + ".csv")

            shutil.rmtree(self._output_path)



def parser_args() -> argparse.Namespace:
    """
    Parse the arguments
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Input file path", default=DATA_PATH)
    parser.add_argument("--output", help="Output file path", default=OUTPUT_PATH)
    args = parser.parse_args()

    if args.output.endswith(".csv"):
        args.output = args.output.rstrip(".csv")

    return args



def main(input_path : str, output_path : str) -> None:
    """
    Main function
    """

    spark = SparkSession.builder.getOrCreate()

    process = Solution(spark, input_path, output_path)

    process.run()

    spark.stop()




if __name__ == "__main__":

    args = parser_args()

    if args.output.endswith(".csv"):
        args.output = args.output.rstrip(".csv")

    main(args.input, args.output)