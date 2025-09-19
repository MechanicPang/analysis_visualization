import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, max, abs, sum, min, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import pandas as pd
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.INFO, format='%(asctimes)s-%(levelname)s-%(message)s')
sc = SparkSession.builder.appName("nw").enableHiveSupport().getOrCreate()


sql = '''select '''

edges = sc.sql(sql).toPandas()

sql = '''select '''
nodeValue = sc.sql(sql).toPandas()

nw = nx.from_pandas_edgelist(edges, 'user_id1', 'user_id2')

nodeValuePd = nodeValue.set_index('neighbor_node')
nodeValuePd = nodeValuePd.reindex(nw.nodes())

nodeValuePd.sort_values(by='value', ascending=False)

plt.figure(figsize=(15,10))
nx.draw(nw, with_labels=True, node_color=np.array(nodeValuePd['value'].values, dtype='float32'),  font_size=12, font_color="black", cmap=plt.cm.Reds, pos=nx.fruchterman_reingold_layout(nw))
