from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size, lit, when

count = 0

# 初始化 SparkSession
spark = SparkSession.builder.appName("ConnectedComponents").getOrCreate()

# 示例数据（替换为实际数据）
data = [("A", "B"), ("B", "C"), ("D", "E"), ("F", "G"), ("G", "H"), ("I", "J")]
edges_df = spark.createDataFrame(data, ["node1", "node2"])

# 1. 将边双向化（无向图）
edges_df = edges_df.union(edges_df.select(col("node2").alias("node1"), col("node1").alias("node2")))

# 2. 初始化联通子图 ID（每个节点的初始 ID 为自身）
vertices_df = edges_df.select("node1").union(edges_df.select("node2")).distinct()
vertices_df = vertices_df.withColumn("component", col("node1")).withColumnRenamed("node1", "node")

# 3. 迭代更新联通子图 ID
while True:
    count += 1
    print("迭代次数：{}".format(count))
    # 更新逻辑：将每个节点的 component 更新为其邻居的最小 component
    updated = vertices_df.join(edges_df, vertices_df.node == edges_df.node1) \
        .select(edges_df.node2.alias("node"), vertices_df.component.alias("new_component")) \
        .groupBy("node").agg({"new_component": "min"}) \
        .withColumnRenamed("min(new_component)", "new_component")
    
    # 检查是否还有更新
    join_condition = vertices_df.node == updated.node
    merged = vertices_df.join(updated, join_condition, "left_outer") \
        .select(
            vertices_df.node,
            when(updated.new_component < vertices_df.component, updated.new_component).otherwise(vertices_df.component).alias("component_new"), # 与原来编号合并，取最小的为代码
            vertices_df.component
        )
    
    # 如果所有节点的 component 不再变化，则退出循环
    if merged.filter(col("component") != col("component_new")).count() == 0:
        break
    
    # 更新 component
    vertices_df = merged.select(col("node"), col("component_new").alias("component"))
    print(vertices_df.take(10))

# 4. 聚合结果
result = vertices_df.groupBy("component").agg(
    collect_list("node").alias("commu"),
    size(collect_list("node")).alias("size")
).select(
    col("component").alias("clu_id"),
    col("commu"),
    col("size")
)

# 显示结果
result.show(truncate=False)
