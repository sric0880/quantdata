作者个人使用项目，这里只提供数据库结构(示例，不完整)，具体数据需要自己采集和拉取，提供简单的导入示例

依赖库: [MinIO](https://min.io/)（[Github](https://github.com/minio/minio)）、[Tiledb](https://tiledb.com/open-source/array-storage/)、
 [Mongodb](https://www.mongodb.com)

- 1维数据，按行存储的数据，使用MongoDB
- 2维数据，tick数据和K线数据，一个时间维度，一个Symbols维度，使用Tiledb

## 使用

修改`docker-compose.yaml`中的用户名和密码，控制台输入 `docker-compose up -d`

## 性能对比

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
```

Tiledb(minio+compression, 拉取3个属性)

```sh
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
