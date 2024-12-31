#include <iostream>
#include "quantdata/init.h"

int main(int, char **)
{
  std::cout << "init all" << std::endl;
  InitDB();
  std::cout << std::endl;

  std::cout << "live " << std::endl;
  InitDB(nullptr, "live");
  std::cout << std::endl;

  std::cout << "bt-ctp " << std::endl;
  InitDB(nullptr, "bt-ctp");
  std::cout << std::endl;

  YAML::Node config;
  YAML::Node mongodb;
  YAML::Node duckdb;
  mongodb["host"] = "127.0.0.1";
  duckdb["uri"].push_back("/Users/qiong/WorkingDoc/quantdata/datas/duckdb/finance_ticks_ctpfuture.db");
  config["mongodb"] = mongodb;
  config["duckdb"] = duckdb;
  std::cout << "custom " << std::endl;
  InitDB(&config);
  std::cout << std::endl;
}