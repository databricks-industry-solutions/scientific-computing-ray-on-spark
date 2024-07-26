# Databricks notebook source
# MAGIC %md
# MAGIC # Bin Packing Optimization with Ray on Databricks
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 00. Setup Compute Environment
# MAGIC
# MAGIC The example below uses an open-source binpacking implementation called py3dbp - see [enzoruiz/3dbinpacking](https://github.com/enzoruiz/3dbinpacking) for library information and more examples.
# MAGIC
# MAGIC The code below has been tested on AWS Databricks with the following cluster configuration; note that Ray and Spark params may need to be adjusted for different cluster sizes and runtimes:
# MAGIC ``` 
# MAGIC {
# MAGIC   ...
# MAGIC   "spark_version": "15.3.x-cpu-ml-scala2.12",
# MAGIC   "node_type_id": "r6id.xlarge",
# MAGIC   "num_workers": 4
# MAGIC   "data_security_mode": "SINGLE_USER",
# MAGIC   "single_user_name": "...",
# MAGIC   "runtime_engine": "STANDARD"
# MAGIC   ...
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install py3dbp
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01. Ray Solution Overview
# MAGIC
# MAGIC The binpacking function in our `01_` notebook is fast: based on the test cases, we can observe it almost always runs in **<1 second**. 
# MAGIC
# MAGIC However, this task is logically part of a nested modeling process, which we can describe with the following pseudocode: 
# MAGIC
# MAGIC ```
# MAGIC For (i in items):             <-- The process needs to run for every item in inventory (~1000s)
# MAGIC   For (c in containers):      <-- Try the fit for every type of container (~10s) 
# MAGIC     For (o in orientations):  <-- The starting orientations of the first item must each be modeled (==6) 
# MAGIC       Pack container:         <-- Finally, try filling a container with items with a starting orientation
# MAGIC ```
# MAGIC
# MAGIC What if we were to run this looping process sequentially using single-node Python? Well, if we have millions of iterations (e.g. `20,000 items x 20 containers x 6 starting orientations = 2.4M combinations`), this could take **hundreds of hours** to compute (e.g. `2.4M combinations * 1 second each / 3600 seconds per hour = ~660 hours = 27 days`). Our business does not have time to wait around for a month to just get the starting data for a logistics and supply chain use-case. We must come up with a more efficient way to compute rather than a serial/sequential process. 
# MAGIC
# MAGIC To implement this, we will use a part of the Ray framework called [Ray Core](https://docs.ray.io/en/latest/ray-core/walkthrough.html):
# MAGIC > "provides a small number of core primitives (i.e., tasks, actors, objects) for building and scaling distributed applications". 
# MAGIC
# MAGIC Most importantly for our needs in this bin packing optimization case are [Ray Tasks](https://docs.ray.io/en/latest/ray-core/tasks.html): 
# MAGIC > "Ray enables arbitrary functions to be executed asynchronously on separate Python workers. Such functions are called Ray remote functions and their asynchronous invocations are called Ray tasks." 
# MAGIC
# MAGIC This means we can take a cluster of compute instances (what most Databricks users think of as a Spark cluster) and execute as many instances of a process as we want! The next section will show how to develop these task functions. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Setup Ray Cluster
# MAGIC
# MAGIC This notebook relies on setting up a "Ray Cluster" on the Databricks cluster where you are running this code. The setup code below configures this for you, as well as installing dependencies on each of the worker nodes. We need to run this step before executing any Ray logic. Ray comes pre-installed in recent [Databricks Machine Learning Runtimes](https://docs.databricks.com/en/machine-learning/index.html#databricks-runtime-for-machine-learning). 

# COMMAND ----------

from py3dbp import Packer, Bin, Item
import numpy as np
import pandas as pd
from itertools import permutations
import timeit, random, datetime, sys, re

import ray
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster

# Shutdown any running Ray cluster
ray.shutdown()

# Make sure every Ray worker has dependencies
runtime_env = {
    "pip": ["py3dbp"]
}

# Initalize Ray Cluster
setup_ray_cluster(
  num_worker_nodes=2,     # This number should match number of workers in cluster
  num_cpus_worker_node=4, # Default 1, or scale to use all cores on each worker
  num_gpus_worker_node=0, # Set if using GPU nodes
  collect_log_to_path="/dbfs/ray/osk_binpacking"
)
ray.init(runtime_env=runtime_env)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Containers inventory, setup Ray remote variables
# MAGIC In this demonstration, we will set up a variable containing the data for each container we would like to test fills. We use a [remote object](https://docs.ray.io/en/latest/ray-core/walkthrough.html#passing-an-object) for this, as our data set of container dimensions is relatively small (e.g. 10's of records) and can effectively be "broadcast" to each of our Ray workers. 
# MAGIC
# MAGIC The below code creates the variable from a Python list, but comments demonstrate how to dynamically load this data from a Delta table, for example when you want to load the latest data from a dimension table from an ERP system.

# COMMAND ----------

# # Uncomment and customize this code to load container data from a Delta table

# # Load the data from the Delta table
# containers = spark.read.table("my_container_table")

# # Select the required columns
# selected_columns = [
#     "container_id",
#     "container_length_in",
#     "container_width_in",
#     "container_height_in",
#     "fill_capacity_lbs",
#     "bin_vol"
# ]
# df_selected = containers.select(*selected_columns)

# # Convert the DataFrame to a list of dictionaries
# container_list = df_selected.collect()
# container_dicts = [row.asDict() for row in container_list]

# # Display the list of dictionaries, make sure it matches the below example
# display(container_dicts)

# # Broadcast as Ray object
# containers_remote = ray.put(container_dicts)

# COMMAND ----------

# import ray
# Sample list of various-sized containers
containers = [{
    "container_id":"C0001", 
    "container_length_in":9.4,
    "container_width_in":5.5,
    "container_height_in":4.5,
    "fill_capacity_lbs":33.8,
    "bin_vol":232.65
    },{
    "container_id":"C0002", 
    "container_length_in":9.4,
    "container_width_in":13,
    "container_height_in":6.8,
    "fill_capacity_lbs":32.4,
    "bin_vol":830.96
    },{
    "container_id":"C0003", 
    "container_length_in":29.2,
    "container_width_in":27.3,
    "container_height_in":27,
    "fill_capacity_lbs":1800,
    "bin_vol":21523.32
    },{
    "container_id":"C0004", 
    "container_length_in":232,
    "container_width_in":92,
    "container_height_in":93,
    "fill_capacity_lbs":55000,
    "bin_vol":2000000
    }
]
print(containers)

# Broadcast as Ray object
containers_remote = ray.put(containers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Build Ray Binpacking logic
# MAGIC
# MAGIC The cells below construct the `@ray.remote()` functions needed, starting at the innermost loop (e.g. pack one container with one starting orientation) to the outermost loop (e.g. compute results for a single item):

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner-most loop: pack_container_ray
# MAGIC `pack_container_ray()` function is similar to single-node Python, but with some additional error checking and multi-processing usage. 

# COMMAND ----------

@ray.remote(num_returns=2)
def pack_container_ray(args):
    """
    Pack a container with max number of items. Ray implementation.

    Parameters:
    args (tuple): Tuple containing item and bin dimensions

    Returns:
    max_qty: Maximum qty of items that will fit in the bin
    fill_rate: Percent of bin filled by items at max_qty
    """
    
    import numpy as np
    import multiprocessing
    from multiprocessing import Process, Pool, Queue
    import py3dbp
    from py3dbp import Packer, Bin, Item

    # Local function to handle zero division
    def division_zero (x, y):
        return x//y if y else 0
    
    # Unpack args
    item_x, item_y, item_z, item_wt, item_vol, bin_x, bin_y, bin_z, bin_wt, bin_vol = args

    item_attributes = [item_x, item_y, item_z]
    bin_attributes = [bin_x, bin_y, bin_z]
    # max_qty variable = # of items that will fit in bin
    max_qty = None 

    # Compute theoretical max quantity of items to pack by both container volume and weight
    wht_max_qty = int(division_zero(bin_wt, item_wt))
    vol_max_qty = int(division_zero(bin_vol, item_vol))   
    
    # Zero items fit if the item is too big or heavy for the bin
    if wht_max_qty == 0 or vol_max_qty == 0:
        max_qty = 0
    
    # If at least one item fits
    else:
        # Instantiate Packer from py3dbp library and add a "bin"
        packer = Packer()
        packer.add_bin(Bin("LB", bin_x, bin_y, bin_z, bin_wt))

        # Determine # of identical items to pack
        max_b_load = min(wht_max_qty, vol_max_qty)

        # binpacking library uses this to "queue" packable items
        for i in range(max_b_load):
            packer.add_item(Item("item_" + str(i), item_x, item_y, item_z, item_wt))
        
        # Define multi-processing threads
        def worker(q):
            packer.pack()
            q.put(packer)

        q = Queue()
        pack_process = Process(target=worker, args=(q,))

        pack_process.start()
        pack_process.join(timeout = 0.8)

        # If worker process takes more than 0.8 seconds, calculate naive max capacity instead
        # This is a safety measure to accelerate cases of tiny items in giant bins, which can take a long time to model
        if pack_process.is_alive():
            pack_process.terminate()
            pack_process.join()
            
            side_load = [int(j / i) for i, j in zip(item_attributes[:3], bin_attributes[:3])]

            load_qty = np.prod(side_load)
            max_qty = min(load_qty, wht_max_qty)
        
        # Default case, retrieve number of items packed in bin
        else:
            packer = q.get()
            max_qty = len(packer.bins[0].items)

    fill_rate = np.round((max_qty * item_vol / bin_vol) * 100, decimals=1)
    return max_qty, fill_rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Middle loop: permute_item_orientations_ray
# MAGIC `permute_item_orientations_ray()` iterates through 6 possible orientations of the starting item in the bin. The Python bin-packing framework in use is sensitive to the orientation of the first packed item, so we will treat each starting orientation as a unique trial of our optimization process.

# COMMAND ----------

@ray.remote(num_returns=2)
def permute_item_orientations_ray(args):
    """
    Calculate permutations of item orientations. 
    (x, y, z); (x, z, y); (z, x, y)... 6 for each item

    Parameters:
    args (tuple): Tuple containing item and bin dimensions

    Returns:
    qty_results_list: List of qty results from each call to pack_container_ray
    fill_results_list: List of fill results from each call to pack_container_ray
    """

    from itertools import permutations

    # Unpack args
    item_x, item_y, item_z, item_wt, item_vol, bin_x, bin_y, bin_z, bin_wt, bin_vol = args
    item_attributes = [item_x, item_y, item_z]
    bin_attributes = [bin_x, bin_y, bin_z]

    # Tracking variables
    qty_results_list = []
    fill_results_list = []
    
    # Determine permutations of item dimensions
    item_perms = permutations(item_attributes[:3])

    # 6 calls to pack_container_ray remote function for each item
    for item_perm in item_perms:
        qty_result, fill_rate = pack_container_ray.remote(item_perm + args[3:])

        qty_results_list.append(qty_result)
        fill_results_list.append(fill_rate)

    # ray.get() outside of for-loop so results retrieved async
    qty_results_list = ray.get(qty_results_list)
    fill_results_list = ray.get(fill_results_list)

    return qty_results_list, fill_results_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer-most loop: try_container_n
# MAGIC The outermost function `try_container_n()` is NOT a Ray remote function, and is called as a normal Python function. However, internal to the function, the logic uses Ray objects and async calls to optimally calculate and retrieve results. 

# COMMAND ----------

from typing import Any, Dict

def try_container_n_ray(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Determine fill for item in bin N. This function is called N times for each item

    Parameters:
    row (Dict[str, Any]): Ray row object, tuple of dimensions for both item and container. Unpacked into item and bin attributes

    Returns:
    row (Dict[str, Any]): Original row object, with new list of container fill results for a given item
    """
    # Pull out item arguments from Ray row: item_args = (item_x, item_y, item_z, item_wt, item_vol)
    item_args = (
        row['length'], 
        row['width'], 
        row['height'], 
        row['weight'], 
        row['volume']
        )

    # Get remote Ray variable for bins
    bins_args = ray.get(containers_remote)
    bin_fills = []

    # bins_args is remote object
    for bin in bins_args:
        bin_args = (
            bin["container_length_in"], 
            bin["container_width_in"], 
            bin["container_height_in"],
            bin["fill_capacity_lbs"],
            bin["bin_vol"]
            )

        # Concatenate item and bin arguments into single tuple
        args = item_args + bin_args
        
        # Pass to Ray function for bin fill permutations
        qty_results_list, fill_results_list = permute_item_orientations_ray.remote(args)
        
        single_bin_fill = {
            "container_id":     bin["container_id"],
            "qty_results":      ray.get(qty_results_list),
            "fill_pct_results": ray.get(fill_results_list)
            }
        
        bin_fills.append(single_bin_fill)

    # Convert final list to string for serialization
    row["bin_fills"] = str(bin_fills)
    return row

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Load Items inventory from Parquet
# MAGIC
# MAGIC In this example, we load in a small portion of item inventory to demonstrate the efficiency of this solution. We have 30 items to model.

# COMMAND ----------

import os
input_filepath = os.path.join(os.getcwd())+"/sample_item_input.parquet"
print("input_filepath:",input_filepath)

# Use Ray data to read in file
items = ray.data.read_parquet(input_filepath)

# Print schema to confirm data types
items.schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06. Apply Binpacking logic use Ray `.map()`
# MAGIC
# MAGIC Now that we have our item data loaded as a Ray dataset, our container data as a Ray remote object, and our three binpacking functions, we can finally apply our logic! 
# MAGIC
# MAGIC The cell below will take a bit to run. To see the parallel Ray process underway, navigate back to the cell where you initialized the Ray cluster and click the hyperlink for `Open Ray Cluster Dashboard in a new tab`. 

# COMMAND ----------

items_packed = items.map(try_container_n_ray)
items_packed.take(1)

# COMMAND ----------

# MAGIC %md 
# MAGIC Looking at the displayed results, we have the data structure we are looking for: a row representing the item, with a column `bin_fills` with a list of structs representing how this particular item will fit into each bin. As expected, we can see that for relatively small items, we can fit a huge quantity in the largest shipping-container sized bin, and that with odd-sized objects, the starting orientation of the first item matters in the bin fill percentage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07. Full pipeline: load data, apply logic, save results
# MAGIC
# MAGIC The code below is our full pipeline, and what would "run in production." We'll also run a timer to compare how fast this runs vs the theoretical serial-based process described at the beginning of this notebook.
# MAGIC
# MAGIC Finally, save our results back to a Parquet file to analyze further. For this operation, the number of files written is controlled by the number of blocks/partitions in the dataset. [See Ray docs for more info](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_parquet.html) In production, these data files should be written to the production data store, such as a [Unity Catalog Volume](https://docs.databricks.com/en/volumes/index.html)

# COMMAND ----------

import time

# Set output file destination here, UC volume recommended
output_filepath = "/Volumes/shared/scientific_computing_ray/bin_packing_optimization/"

start_time = time.perf_counter()

# Run Ray on 30 items
items_full = (
  ray.data
    .read_parquet(input_filepath)
    .map(try_container_n_ray)
    .repartition(1) # Forcing 1 file total
    .write_parquet(output_filepath)
)

end_time = time.perf_counter()
print(f"Ray took {end_time - start_time} seconds") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08. Analyzing Results
# MAGIC Now that we have results saved to a Parquet file, some quick back-of-the-napkin math: how long would this process have taken running serially? 
# MAGIC > 30 items x 4 bins x 6 starting orientations = 720 trials
# MAGIC
# MAGIC In developing the bin-packing logic, we observed each trial takes approx. 1 second. So in theory, this would have run in approximately 10x longer than our trial with Ray 
# MAGIC which took approx. 79 seconds with 4 worker nodes, which includes Ray job setup time.
# MAGIC
# MAGIC This is a huge win for business use-cases, as the ability to run complex situations 10x faster could be the difference between profitable and unprofitable decisions. This optimization pattern should also scale well to any number of combinations (e.g. millions of combinations) and with the number of Ray workers we include in the clusters (e.g. dozens or hundreds of nodes). 

# COMMAND ----------

# MAGIC %md
# MAGIC Another benefit of Ray on Databricks, is you can simultaneously use Spark. To demonstrate this, while our Ray cluster is still running, run the Pyspark code below to parse our resultant dataset, which is what might be fed into a planning and invetory tool to determine which bin to send each parts producer. 
# MAGIC
# MAGIC Note: Please review [Ray on Databricks docs](https://docs.databricks.com/en/machine-learning/ray/scale-ray.html#memory-resource-configuration-for-apache-spark-and-ray-hybrid-workloads) for working with these hybrid workloads; the below is a minimal example.

# COMMAND ----------

from pyspark.sql.functions import *

results = (
  spark.read
  .parquet(output_filepath)
  .withColumn("bin_fills_cast", from_json(col("bin_fills"), "array<struct<container_id:string,qty_results:array<int>,fill_pct_results:array<double>>>"))
  .withColumn("bin_fill_raw", explode("bin_fills_cast"))
  .withColumn("container_id", col("bin_fill_raw.container_id"))
  .withColumn("max_qty_fill", array_max(col("bin_fill_raw.qty_results")))
  .withColumn("max_pct_fill", array_max(col("bin_fill_raw.fill_pct_results")))
  .withColumn("part_dimensions", struct(col("length"), col("width"), col("height"), col("weight")))
  .select("part_number", "part_description", "part_dimensions", "container_id", "max_qty_fill", "max_pct_fill", "bin_fill_raw")
)

display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09. Concluding Thoughts
# MAGIC Scientific computing encompasses many different techniques, from simulation and optimization, to linear programming and numerical analysis. It is a broad space with decades of proven techniques, and while many of the solutions that companies are using today were built for single-node or vertically-scaled computing infrastructure, Ray Core provides a powerful horizontally-scaling framework. 
# MAGIC
# MAGIC With Ray on Databricks, we have seen how easy it is to take a computationally complex problem such as bin packing and run it 10x faster with minor code adjustments, all in the effort to improve business outcomes.
