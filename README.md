作者个人使用项目，这里只提供数据库结构，简单的导入流程，具体数据需要自己采集和拉取。还包括数据库客户端接入API

依赖库: ~~[MinIO](https://min.io/)（[Github](https://github.com/minio/minio)）~~、[Tiledb](https://tiledb.com/open-source/array-storage/)、
 [Mongodb](https://www.mongodb.com)

- 1维数据，按行存储的数据，使用MongoDB
- 2维数据，按列存储的数据，一个时间维度，一个Symbols维度，使用Tiledb，用于tick数据和K线数据

## 使用

修改`docker-compose.yaml`中的用户名和密码，控制台输入 `docker-compose up -d`

## TDengine、MongoDB、TileDB对比

通过性能对比，发现MinIO对于少量数据多个文件（每个属性一个文件）的读取性能不够理想，其本质还是通过HTTP下载，只能通过添加并行和batch一次读取多个文件的方式加速，但是通过尝试调整tiledb的相关配置，并没有任何提升，可能已经达到了性能瓶颈。对于小文件的下载，速度基本在1M~2M/s。但是对于大型文件的下载，个人电脑可以达到200M/s的速度。[MinIO Git上有关小文件下载速度慢的讨论](https://github.com/minio/mc/issues/2796)

Minio还有一个bug，如下代码会报错在MinIO下运行会报错，但是本地运行正常
```python
A.label_index(["symbol","dt"])["000001.SZ", np.datetime64('2023-04-29', 'D'):np.datetime64('2024-04-29', 'D')]
```

TDengine的主要问题在于配置不够灵活，必须要有时间维度，而是数据分块不能为每个表单独设置，而只能在数据库层面设置，K线周期不同，不能按不同周期建表，所以查询效率一般。TileDB可以建任意维度的表，并且单独定义每个存储对象的分块（tile）大小。

TDengine对于不同的切片查询方式，需要写不同的SQL语句，按行或者按列查询效率区别非常大；TileDB只有一种切片查询方式，满足所有需求，并且效率一致。

TileDB可以直接通过访问共享文件夹实现远程获取，其性能和local几乎相等，完全满足在本地局域网内建库的需求。

所有测试均在同一机器测试，只看相对值。

- 拉取 stocks_basic_info

|Tiledb(minio)|Tiledb(local)|Mongodb(带缓存)|
|--|--|--|
|165 ms ± 2.01 ms|22.1 ms ± 729 µs|13.2 µs ± 3.4 µs|

1. 不带缓存差别在10倍左右，也远比TileDB性能高

- 拉取 000001.SZ 日线详情数据(47个属性，7980天)

|-|47个属性|3个属性|
|--|--|--|
|TDengine|768 ms ± 28.8 ms|187 ms ± 2.15 ms|
|Tiledb(minio)|560 ms ± 17.7 ms|-||
|Tiledb(minio+compression)|519 ms ± 28.8 ms|35.1 ms ± 1.92 ms|
|Tiledb(local)|12.6 ms ± 298 µs|-|
|Tiledb(local+compression)|16.3 ms ± 537 µs|-|
|dataframe pickle(local)|2.53 ms ± 556 µs|--|

1. compression空间减少5倍
2. 当本地读取IO已经不是瓶颈，反而解压更费时间

## TODO
- 定期合并fragments