# quantdata

作者个人研究项目，这里只提供样本数据结构，简单的导入流程，具体数据需要自己采集和拉取。还包括数据库客户端接入API

## 数据结构

见 [schema.md](https://github.com/sric0880/quantdata/blob/main/schema.md)

## 数据库

- ~~[MinIO](https://min.io/)（[Github](https://github.com/minio/minio)）~~
- [Tiledb](https://tiledb.com/open-source/array-storage/)、
- [Mongodb](https://www.mongodb.com)
- [TDengine](https://tdengine.com/)
- [Clickhouse官网](https://clickhouse.com/)  [Python 文档](https://clickhouse.com/docs/en/integrations/python)
- [DuckDB官网](https://duckdb.org/) [Python 文档](https://duckdb.org/docs/api/python/overview)

存储：

- 按行存储的数据，使用MongoDB
- 按列存储的数据，使用其他数据库，主要用于价格数据，可以是一维（时间），也可以是二维（时间+symbols

### 启动

修改`docker-compose.yaml`中的用户名、密码以及注释，控制台输入 `docker-compose up -d`

## Benchmark

所有测试均在同一机器测试，只看相对值。绝对值不同机器配置，影响较大。

### 1. stocks_basic_info

|Tiledb(minio)|Tiledb(local)|Mongodb(带缓存)|
|--|--|--|
|165 ms ± 2.01 ms|22.1 ms ± 729 µs|13.2 µs ± 3.4 µs|

- 不带缓存差别在10倍左右，也远比TileDB性能高。

### 2. "000001.SZ" 日线数据(47个属性，7980行)

|-|47个属性|3个属性|
|--|--|--|
|TDengine(server)|402 ms ± 19.9 ms|57.1 ms ± 848 µs|
|TDengine(server+40G)|720 ms ± 13.4 ms|168 ms ± 599 µs|
|Tiledb(minio)|560 ms ± 17.7 ms|-|
|Tiledb(minio+compression)|519 ms ± 28.8 ms|35.1 ms ± 1.92 ms|
|Tiledb(local)|12.6 ms ± 298 µs|-|
|Tiledb(local+compression)|16.3 ms ± 537 µs|-|
|Clickhouse(server)|83.9 ms ± 1.64 ms|53.3 ms ± 3.21 ms|
|Duckdb(local+5G)|5.76 ms ± 118 µs|1.23 ms ± 33 µs|
|**Duckdb(local network+5G)**|7.48 ms ± 454 µs|1.64 ms ± 70.5 µs|
|pd.Dataframe pickle(local)|2.53 ms ± 556 µs|--|

- Tiledb compression 空间减少5倍，在MinIO上网络传输减少，延迟稍有下降，Tiledb本地读取IO已经不是瓶颈，反而解压更费时间。
- TDengine随着存入数据量的增大，查询性能会有下降，尤其当查询少量字段时，性能下降了3倍。

## TDengine

测试使用版本 3.0.3.0

- TDengine的主要问题在于配置不够灵活，必须要有时间维度，而是数据分块不能为每个表单独设置，而只能在数据库层面设置，K线周期不同，不能按不同周期建表，所以查询效率一般。TileDB可以建任意维度的表，并且单独定义每个存储对象的分块（tile）大小。
- TDengine对于不同的切片查询方式，需要写不同的SQL语句，按行或者按列查询效率区别非常大；TileDB只有一种切片查询方式，满足所有需求，并且效率一致。
- 它有一个优势在于超级表的概念，子表按TAGS区分，可以实现在一张大表中按TAGS查询能有查询子表的效率。

## Tiledb

适合固定不变的数据集的读取，维度大小一开始就固定下来，一次写入，多次读取。小文件适合用本地文件，大型文件的读取可以考虑Minio。

- TileDB可以直接通过访问共享文件夹实现远程获取，其性能和local几乎相等，完全满足在本地局域网内建库的需求。
- 源码还有bug，见`setup_tiledb.ipynb`
- Label Index限制比较多，比如symbols按行存储，由于symbols必须排序，当需要动态插入一行时，很难用优雅的方式实现。见`setup_tiledb.ipynb`
- 通过性能对比，发现Minio对于少量数据多个文件（每个属性一个文件）的读取性能不够理想，其本质还是通过HTTP下载，只能通过添加并行和batch一次读取多个文件的方式加速，但是通过尝试调整tiledb的相关配置，并没有任何提升，可能已经达到了性能瓶颈。对于小文件的下载，速度基本在1M~2M/s。但是对于大型文件的下载，个人电脑可以达到200M/s的速度。[MinIO Git上有关小文件下载速度慢的讨论](https://github.com/minio/mc/issues/2796)
- Minio还有一个bug，如下代码会报错在Minio下运行会报错，但是本地运行正常

```python
A.label_index(["symbol","dt"])["000001.SZ", np.datetime64('2023-04-29', 'D'):np.datetime64('2024-04-29', 'D')]
```

## Duckdb

- 通过ATTACH和DETACH可以实现共享文件夹远程访问，其性能和local相比略逊一点，但依然很快，完全满足在本地局域网内建库的需求。但是 ATTACH 操作本身非常耗时，5G大小的数据库，耗时13秒。所以Duckdb启动后，其进程需要一直在后台运行，以保证数据的快速访问。
- 为了获得最佳效率，每个symbol单独成表，否则每日数据更新后，symbol会分布在不同的block中，最后导致查询效率变低
- Duckdb可以直接加载csv和Excel、parquet数据，非常方便
- Duckdb会自动给每一列创建block range index。如果数据是symbol+dt二维，dt分散在不同行，按dt查询效率很低。测试发现，在dt上建立ART索引，查询效率反而更低了。
