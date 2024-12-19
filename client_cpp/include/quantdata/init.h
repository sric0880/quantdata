#pragma once
#include <string>
#include <yaml-cpp/yaml.h>
#include "quantdata/duckdb.h"
#include "quantdata/mongodb.h"

void InitDB(const YAML::Node *config = nullptr, std::string_view stype = "");
// 调用InitDB会注册atexit回调函数释放数据库，不需要再手动调用CloseDB
// void CloseDB();