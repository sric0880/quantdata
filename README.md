# quantdata

作者个人研究项目，这里只提供样本数据结构，简单的导入流程，具体数据需要自己采集和拉取。还包括数据库客户端接入API

依赖库：

- ~~[MinIO](https://min.io/)（[Github](https://github.com/minio/minio)）~~
- [Tiledb](https://tiledb.com/open-source/array-storage/)、
- [Mongodb](https://www.mongodb.com)
- [TDengine](https://tdengine.com/)
- [Clickhouse官网](https://clickhouse.com/)  [Python 文档](https://clickhouse.com/docs/en/integrations/python)

存储：

- 1维数据，按行存储的数据，使用MongoDB
- 2维数据，按列存储的数据，一个时间维度，一个Symbols维度，使用Tiledb，用于tick数据和K线数据

## Tiledb限制

- symbols按行存储，由于symbols必须排序，当需要动态插入一行时，很难用优雅的方式实现。
- Tiledb 底层有随机出现的bug，见`setup_db.ipynb`

## TDengine/MongoDB/Clickhouse启动

修改`docker-compose.yaml`中的用户名和密码，控制台输入 `docker-compose up -d`

## TDengine/MongoDB/TileDB/Clickhouse对比

通过性能对比，发现MinIO对于少量数据多个文件（每个属性一个文件）的读取性能不够理想，其本质还是通过HTTP下载，只能通过添加并行和batch一次读取多个文件的方式加速，但是通过尝试调整tiledb的相关配置，并没有任何提升，可能已经达到了性能瓶颈。对于小文件的下载，速度基本在1M~2M/s。但是对于大型文件的下载，个人电脑可以达到200M/s的速度。[MinIO Git上有关小文件下载速度慢的讨论](https://github.com/minio/mc/issues/2796)

Minio还有一个bug，如下代码会报错在MinIO下运行会报错，但是本地运行正常

```python
A.label_index(["symbol","dt"])["000001.SZ", np.datetime64('2023-04-29', 'D'):np.datetime64('2024-04-29', 'D')]
```

TDengine 测试使用版本3.0.3.0

TDengine的主要问题在于配置不够灵活，必须要有时间维度，而是数据分块不能为每个表单独设置，而只能在数据库层面设置，K线周期不同，不能按不同周期建表，所以查询效率一般。TileDB可以建任意维度的表，并且单独定义每个存储对象的分块（tile）大小。

TDengine对于不同的切片查询方式，需要写不同的SQL语句，按行或者按列查询效率区别非常大；TileDB只有一种切片查询方式，满足所有需求，并且效率一致。

TileDB可以直接通过访问共享文件夹实现远程获取，其性能和local几乎相等，完全满足在本地局域网内建库的需求。

所有测试均在同一机器测试，只看相对值。绝对值不同机器配置，影响较大。

- 拉取 stocks_basic_info

|Tiledb(minio)|Tiledb(local)|Mongodb(带缓存)|
|--|--|--|
|165 ms ± 2.01 ms|22.1 ms ± 729 µs|13.2 µs ± 3.4 µs|

1. 不带缓存差别在10倍左右，也远比TileDB性能高

- 拉取 000001.SZ 日线详情数据(47个属性，7980天)

|-|47个属性|3个属性|
|--|--|--|
|TDengine|404 ms ± 25.7 ms|57.1 ms ± 848 µs|
|TDengine(40G)|720 ms ± 13.4 ms|168 ms ± 599 µs|

1. TDengine随着存入数据量的增大，查询性能会有下降，尤其当查询少量字段时，性能下降了3倍。

|-|47个属性|3个属性|
|--|--|--|
|TDengine|402 ms ± 19.9 ms|57.1 ms ± 848 µs|
|Tiledb(minio)|560 ms ± 17.7 ms|-|
|Tiledb(minio+compression)|519 ms ± 28.8 ms|35.1 ms ± 1.92 ms|
|Tiledb(local)|12.6 ms ± 298 µs|-|
|Tiledb(local+compression)|16.3 ms ± 537 µs|-|
|Clickhouse|83.9 ms ± 1.64 ms|53.3 ms ± 3.21 ms|
|pd.Dataframe pickle(local)|2.53 ms ± 556 µs|--|

1. Tiledb compression空间减少5倍
2. Tiledb本地读取IO已经不是瓶颈，反而解压更费时间
3. Clickhouse很优秀

## TODO

1. 将Clickhouse进一步和MongoDB进行比较
2. Clickhouse在大数据量上进行测试
