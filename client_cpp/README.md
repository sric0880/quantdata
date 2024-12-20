# C++ Client

支持 MacOS、Linux、Windows

## Install third party

### DuckDB

[DuckDB Installation](https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=macos&download_method=direct)

MacOS下载的文件中有个libduckdb.dylib，将它拷贝到`/usr/local/lib`文件夹下

Linux下载的文件中有个libduckdb.so和libduckdb_static.a，将它拷贝到`/usr/local/lib`文件夹下

Windows下载的文件中有个duckdb.dll和duckdb.lib，将它们拷贝到`C:\Windows\System32`文件夹下

### MongoDB

- [c driver github下载](https://github.com/mongodb/mongo-c-driver/releases) 并 [源码安装](https://www.mongodb.com/docs/languages/c/c-driver/current/install-from-source/#std-label-c-install-from-source)

    cxx依赖c driver，要先安装c driver。从源码安装：

    ```sh
    # 安装 libbson
    cmake -S . -B ./_build -D ENABLE_EXTRA_ALIGNMENT=OFF -D ENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -D CMAKE_BUILD_TYPE=RelWithDebInfo -D ENABLE_MONGOC=OFF 
    cmake --build ./_build --config RelWithDebInfo --parallel
    # for MacOS/Linux
    cmake --install "./_build" --prefix "/usr/local" --config RelWithDebInfo
    # for Windows. 它会安装在C:\Program Files (x86)\mongo-c-driver
    cmake --install "./_build" --config RelWithDebInfo

    # 安装 libmongoc
    cmake -D ENABLE_MONGOC=ON ./_build
    cmake --build ./_build --config RelWithDebInfo --parallel
    # for MacOS/Linux
    cmake --install "./_build" --prefix "/usr/local" --config RelWithDebInfo
    # for Windows. 它会安装在C:\Program Files (x86)\mongo-c-driver
    cmake --install "./_build" --config RelWithDebInfo
    ```

- [cxx driver github下载](https://github.com/mongodb/mongo-cxx-driver/releases)  并 [安装](https://www.mongodb.com/zh-cn/docs/languages/cpp/cpp-driver/current/get-started/download-and-install/)

    - MacOS/Linux 从源码安装：

    ```sh
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DBUILD_VERSION="4.0.0"
    cmake --build .
    sudo cmake --build . --target install
    ```

    - Windows 从源码安装：

    参数 -G "Visual Studio \<version> \<year>" 填写自己的版本和年份

    `<version>`: Your Visual Studio version number

    `<year>`: The year corresponding to your Visual Studio version

    ```sh
    # CMAKE_INSTALL_PREFIX 要设置成刚才安装c driver的路径
    cmake.exe -G "Visual Studio 17 2022" -A "x64" -DCMAKE_CXX_STANDARD=17 -DCMAKE_INSTALL_PREFIX=C:\"Program Files (x86)"\mongo-c-driver -DBUILD_VERSION="4.0.0"
    cmake --build . --config RelWithDebInfo
    cmake --build . --target install --config RelWithDebInfo
    ```

    如果要安装DEBUG版本，将`RelWithDebInfo`改为`DEBUG`。如果在调试Debug模式下开发，必须安装Debug版本，否则会发生意想不到的Bug。

## Install

安装依赖vcpkg，要提前线先安装。

### MacOS/Linux

命令行安装：DCMAKE_TOOLCHAIN_FILE 改成自己的目录。

```sh
cmake -S . -B ./build -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build ./build
sudo cmake --build ./build --target install
```

### Windows

在Visual Studio中调试：修改`CMakePresets.json`中的configurePresets > windows-base > CMAKE_PREFIX_PATH。这个是前面安装mongo cxx driver的`CMAKE_INSTALL_PREFIX`，告诉CMake去哪个目录查找MongoDB的库文件。

命令行安装：DCMAKE_PREFIX_PATH 和 DCMAKE_TOOLCHAIN_FILE 都改成自己的目录。

```powershell
cmake -S . -B ./build -G "Visual Studio 17 2022" -DCMAKE_PREFIX_PATH=C:\"Program Files (x86)"\mongo-c-driver -DCMAKE_TOOLCHAIN_FILE="D:/open_source/vcpkg/scripts/buildsystems/vcpkg.cmake"
cmake --build ./build --config RelWithDebInfo --parallel
cmake --build ./build --target install --config RelWithDebInfo
```

如果要安装Debug版本，将`RelWithDebInfo`换成`Debug`即可。

## 在其他项目作为依赖库使用

在其他项目的CMakeLists.txt中添加

```
find_package(quantdata REQUIRED)
target_link_libraries(YOUR_LIB quantdata::xxxx)
```

