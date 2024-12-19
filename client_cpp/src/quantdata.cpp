#include "quantdata/init.h"

void _InitDuckDB(const YAML::Node &config)
{
  std::string memory_limit = config["memory_limit"] ? config["memory_limit"].as<std::string>() : "";
  int threads_limit = config["threads_limit"] ? config["threads_limit"].as<int>() : 0;
  bool shared_memory = config["shared_memory"] ? config["shared_memory"].as<bool>() : false;
  DuckDBConnect(memory_limit, threads_limit, shared_memory);
  for (auto &uri : config["uri"])
  {
    std::string _uri = uri.as<std::string>();
    DuckDBAttach(_uri);
    std::cout << "duckdb attach " << _uri << std::endl;
  }
}

void _CloseDuckDB()
{
  DuckDBClose();
  std::cout << "duckdb closed" << std::endl;
}

void _InitMongoDB(const YAML::Node &config)
{
  std::string host = config["host"].as<std::string>();
  int port = config["port"] ? config["port"].as<int>() : 27017;
  std::string user = config["user"] ? config["user"].as<std::string>() : "root";
  std::string password = config["password"] ? config["password"].as<std::string>() : "admin";
  std::string authSource = config["authSource"] ? config["authSource"].as<std::string>() : "admin";
  std::string authMechanism = config["authMechanism"] ? config["authMechanism"].as<std::string>() : "SCRAM-SHA-1";
  MongoConnect(host, port, user, password, authSource, authMechanism);
  std::cout << "connecting mongodb" << std::endl;
}

void _CloseMongoDB()
{
  MongoClose();
  std::cout << "mongodb closed" << std::endl;
}

void _InitDB(const YAML::Node *config)
{
  //  初始化数据库连接，全局共享
  if ((*config)["duckdb"])
  {
    _InitDuckDB((*config)["duckdb"]);
    if (std::atexit(_CloseDuckDB))
    {
      std::cerr << " _CloseDuckDB registration failed!" << std::endl;
    }
  }
  if ((*config)["mongodb"])
  {
    _InitMongoDB((*config)["mongodb"]);
    if (std::atexit(_CloseMongoDB) != 0)
    {
      std::cerr << "_CloseMongoDB registration failed!" << std::endl;
    }
  }
}

void _InitDB(const YAML::Node *config, std::string_view stype)
{
  if (stype.empty())
  {
    _InitDB(config);
  }
  else
  {
    YAML::Node new_config;
    auto items = (*config)["services"][stype];
    for (YAML::const_iterator it = items.begin(); it != items.end(); ++it)
    {
      auto db_name = it->first.as<std::string>();
      auto db_cfg_name = it->second.as<std::string>();
      new_config[db_name] = (*config)[db_cfg_name];
    }
    _InitDB(&new_config);
  }
}

void InitDB(const YAML::Node *config, std::string_view stype)
{

  if (!config)
  {
    YAML::Node config = YAML::LoadFile("quantdata_config.yml");
    _InitDB(&config, stype);
  }
  else
  {
    _InitDB(config, stype);
  }
}

// void CloseDB()
// {
//   _CloseDuckDB();
//   _CloseMongoDB();
// }