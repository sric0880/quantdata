作者个人使用项目，这里只提供数据库结构(示例，不完整)，具体数据需要自己采集和拉取，提供简单的导入示例

依赖库: ~~[MinIO](https://min.io/)（[Github](https://github.com/minio/minio)）~~、[Tiledb](https://tiledb.com/open-source/array-storage/)、
 [Mongodb](https://www.mongodb.com)

- 1维数据，按行存储的数据，使用MongoDB
- 2维数据，tick数据和K线数据，一个时间维度，一个Symbols维度，使用Tiledb

## 使用

修改`docker-compose.yaml`中的用户名和密码，控制台输入 `docker-compose up -d`

## 性能对比

通过性能对比，发现MinIO对于少量数据多个文件（每个属性一个文件）的读取性能不够理想，其本质还是通过HTTP下载，只能通过添加并行和batch一次读取多个文件的方式加速，但是通过尝试调整tiledb的相关配置，并没有任何提升，可能已经达到了性能瓶颈。对于小文件的下载，速度基本在1M~2M/s。但是对于大型文件的下载，个人电脑可以达到200M/s的速度。[MinIO Git上有关小文件下载速度慢的讨论](https://github.com/minio/mc/issues/2796)

TDengine的主要问题在于配置不够灵活，一必须要有时间维度，而是数据分块不能为每个表单独设置，而只能在数据库层面设置，K线周期不同，不能按不同周期建表，所以查询效率一般。TileDB可以建任意维度的表，并且单独定义每个存储对象的分块（tile）大小。

TileDB可以直接通过访问共享文件夹实现远程获取，其性能和local相等，完全满足在本地局域网内建库的需求。

- 拉取 stocks_basic_info

Tiledb(minio)

```sh
165 ms ± 2.01 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
```

Mongodb(带缓存)

```sh
13.2 µs ± 3.4 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)
```

- 拉取 000001.SZ 日线详情数据(47个属性，7980天)

TDengine

```sh
768 ms ± 28.8 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
```

TDengine(拉取3个属性)

```sh
187 ms ± 2.15 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
```

Tiledb(minio)

```sh
560 ms ± 17.7 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
```

Tiledb(minio+compression) 空间减少5倍

```sh
519 ms ± 28.8 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
```

Tiledb(minio+compression, 拉取3个属性)

```sh
35.1 ms ± 1.92 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
```

Tiledb(local)

```sh
12.6 ms ± 298 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
```

Tiledb(local+compression) 空间减少5倍

```sh
16.3 ms ± 537 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
```

Pandas dataframe pickle from local file(Baseline)

```sh
2.53 ms ± 556 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
```
