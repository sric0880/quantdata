#pragma once
#include <string>
#include <yaml-cpp/yaml.h>
#include "duckdb/quantdata.h"
#include "mongodb/quantdata.h"

void InitDB(const YAML::Node *config = nullptr, std::string_view stype = "");
// 调用InitDB会注册atexit回调函数释放数据库，不需要再手动调用CloseDB
// void CloseDB();