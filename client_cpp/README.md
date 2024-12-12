# C++ Client

## Install

### MacOS/Linux

```sh
cmake -DCMAKE_BUILD_TYPE=Release
cmake --build .
sudo cmake --build . --target install
```

### Windows

```powershell
cmake.exe .. \
    -G "Visual Studio <version> <year>" -A "x64"         \
    -DCMAKE_CXX_STANDARD=17                     \
    -DCMAKE_INSTALL_PREFIX=C:\xxxx  \

cmake --build .
cmake --build . --target install
```

This command instructs CMake to install into the C:\xxxx directory. Replace the following placeholder values:

`<path>`: The path to your CMake executable

`<version>`: Your Visual Studio version number

`<year>`: The year corresponding to your Visual Studio version

## Third party

### DuckDB
[DuckDB Installation](https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=macos&download_method=direct)

MacOS下载的文件中有个libduckdb.dylib，将它拷贝到`/usr/local/lib`文件夹下

### MongoDB

- [c driver github下载](https://github.com/mongodb/mongo-c-driver/releases) 并 [安装包](https://www.mongodb.com/docs/languages/c/c-driver/current/get-started/download-and-install/) 或 [源码安装](https://www.mongodb.com/docs/languages/c/c-driver/current/install-from-source/#std-label-c-install-from-source)

    cxx依赖c driver，要先安装c driver。从源码安装：

    ```sh
    cmake -S . -B ./_build \
   -D ENABLE_EXTRA_ALIGNMENT=OFF \
   -D ENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
   -D CMAKE_BUILD_TYPE=RelWithDebInfo \
   -D ENABLE_MONGOC=OFF

    cmake --build ./_build --config RelWithDebInfo --parallel

    cmake --install "./_build" --prefix "/usr/local" --config RelWithDebInfo

    cmake -D ENABLE_MONGOC=ON ./_build

    cmake --build ./_build --config RelWithDebInfo --parallel

    cmake --install "./_build" --prefix "/usr/local" --config RelWithDebInfo
    ```

- [cxx driver github下载](https://github.com/mongodb/mongo-cxx-driver/releases)  并 [安装](https://www.mongodb.com/zh-cn/docs/languages/cpp/cpp-driver/current/get-started/download-and-install/)

    MacOS/Linux 从源码安装：

    ```sh
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17

    cmake --build .
    sudo cmake --build . --target install
    ```