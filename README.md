# Spark: Scala / Pyspark Exercise
Create a spark application written in pyspark that reads in the provided [data folder](data), processes
the data, and stores the entire output as specified below.

For each entity_id in the given dataset, find the item_id with the oldest and newest month_id.
In some cases it may be the same item. If there are 2 different items with the same month_id then take the item with the lower item_id. 
Finally, sum the count of signals for each entity and output as the total_signals. The correct output should contain 1 row per unique entity_id.

## Requirements:
1) Create a Pyspark Project.
2) Use the Spark Pyspark API and Dataframes/Datasets
    - Do not use Spark SQL with a sql string!
    - If you write spark.sql( “select ….”) you are doing it wrong!!
3) Output format is Parquet
4) Produce a single parquet output file, regardless of parallelism during processing
5) Use window analytics functions to compute final output in a single query
6) Write unit test to cover all possible scenario 

## Input:
```
entity_id: long
item_id: integer
source: integer
month_id: integer
signal_count: integer
 ```

## Output:
 ```
entity_id: long
oldest_item_id: integer
newest_item_id: integer
total_signals: integer
```

## Example partial output:
```
+----------+--------------+--------------+-------------+
| entity_id|oldest_item_id|newest_item_id|total_signals|
+----------+--------------+--------------+-------------+
| 190979372|             2|             1|           20|
| 220897278|             2|             1|           66|
|1146753107|             2|             0|           22|
| 224619015|             0|             3|           12|
| 118083189|             5|             3|          234|
|    162371|             4|             2|           29|
|    555304|             0|             2|            9|
| 118634684|             2|             3|           17|
| 213956643|         10000|             1|           17|
```

## Submission Guidelines:
Please submit the entire Pyspark project with all code necessary to build and run the app. 
You can bundle it as a zip, tarball or github link. Also include a copy of the output file that is generated from running the app.

## Optional Guidelines (Things that we like and give higher preference)
* Use Pyspark.
* If provide solution running using glue job or workflow is highly preferable.
* We prefer if the solution is submited through a git repo. Please include clear instructions for running the assignment in a Markdown file (ReadMe.md), and try to make it as smooth and repeatible as possible.
* Try to package the solution as a proper project and see if some functionality can be split into separate pacakges in either scala or pyspark application for better project organization and maintenance purposes.
 