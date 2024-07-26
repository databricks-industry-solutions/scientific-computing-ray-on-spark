# Databricks notebook source
# MAGIC %md
# MAGIC # Bin Packing Optimization with Ray on Databricks

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
# MAGIC ## 01. Concept Intro: py3dp code on single-node
# MAGIC
# MAGIC Before learning how to scale an optimization with Ray, let us first review the core business problem: how to pack items into a container, also known as "bin packing".
# MAGIC
# MAGIC The implementation below relies on the Python library `py3dbp`. More info about this library can be found [here](https://github.com/enzoruiz/3dbinpacking).
# MAGIC
# MAGIC The code below **DOES NOT** demonstrate Ray or Spark; it runs solely on the Driver, but confirms the logic for binpacking. We'll get to Ray in the next section!

# COMMAND ----------

from py3dbp import Packer, Bin, Item
import numpy as np

def pack_container(args):
    """
    Pack a container with max number of items.

    Parameters:
    args (tuple): Tuple containing item and bin dimensions

    Returns:
    max_qty: Maximum qty of items that will fit in the bin
    fill_rate: Percent of bin filled by items at max_qty
    """

    # Handle zero division errors
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

        # Determine quantity if naively pack in current arrangement ("side-load")
        side_load = [int(j / i) for i, j in zip(item_attributes[:3], bin_attributes[:3])]
        side_load_qty = np.prod(side_load)
        print("wht_max_qty: ",wht_max_qty, "; vol_max_qty: ", vol_max_qty,"; side_load_qty: ", side_load_qty)

        # Determine # of identical items to pack
        max_b_load = min(wht_max_qty, vol_max_qty, side_load_qty)
        for i in range(max_b_load):
            packer.add_item(Item("item_" + str(i), item_x, item_y, item_z, item_wt))
        packer.pack()
        max_qty = len(packer.bins[0].items)

    # Calculate Fill Rate, or % of bin that is filled by items
    fill_rate = np.round((max_qty * item_vol / bin_vol) * 100, decimals=1)
    return max_qty, fill_rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test case 1: Unit-size items, size 100 container 
# MAGIC
# MAGIC Now we can test this logic. Below, we have an item of volume 100 (`10x5x2`), with unit-size items (`1x1x1`). We should be able to fit 100 items in this container, with 100% fill rate:

# COMMAND ----------

# BIN
bin_x = 10
bin_y = 5
bin_z = 2
bin_wt = 1000
bin_vol = bin_x * bin_y * bin_z

# ITEM
item_x = 1
item_y = 1
item_z = 1
item_wt = 1
item_vol = item_x * item_y * item_z

# all args together
args = (item_x, item_y, item_z, item_wt, item_vol, bin_x, bin_y, bin_z, bin_wt, bin_vol)

max_qty, fill_rate = pack_container(args)
print("Fit items:", max_qty)
print("Fill rate:", fill_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test case 2: Odd-size items, size 100 container 
# MAGIC Now let's test with a slightly odd-size item, where the `y` dimension of the bin is not a multiple of the `y` dimension of the item (e.g. `5 / 2 != 0`)
# MAGIC
# MAGIC Note that the implementation only determines the orientation of the **first item placement**, then we rely on the py3dbp library to optimize the rest of the container fill.  

# COMMAND ----------

# BIN
bin_x = 10
bin_y = 5
bin_z = 2
bin_wt = 1000
bin_vol = bin_x * bin_y * bin_z

# ITEM
item_x = 1
item_y = 2
item_z = 1
item_wt = 1
item_vol = item_x * item_y * item_z

# all args together
args = (item_x, item_y, item_z, item_wt, item_vol, bin_x, bin_y, bin_z, bin_wt, bin_vol)

max_qty, fill_rate = pack_container(args)
print("Fit items:", max_qty)
print("Fill rate:", fill_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test case 3: Heavy items, limited by weight
# MAGIC For a third case, let's take items of unit weight `100` and fit them to a bin where the weight limit is `1000`, just to make sure our function capture weight restrictions as well. `10` items should fit in the bin, with a lot of "wasted space" (low fill rate).

# COMMAND ----------

# BIN
bin_x = 10
bin_y = 5
bin_z = 2
bin_wt = 1000
bin_vol = bin_x * bin_y * bin_z

# ITEM
item_x = 1
item_y = 2
item_z = 1
item_wt = 100
item_vol = item_x * item_y * item_z

# all args together
args = (item_x, item_y, item_z, item_wt, item_vol, bin_x, bin_y, bin_z, bin_wt, bin_vol)

max_qty, fill_rate = pack_container(args)
print("Fit items:", max_qty)
print("Fill rate:", fill_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our binpacking logic built and tested, we can move on to the most important part: how to scale this single-node Python process with Ray to handle millions of item/bin combinations!
# MAGIC
# MAGIC Proceed to the next notebook, `02_ray_for_binpacking`.
