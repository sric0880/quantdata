#include "quantdata/datetime.h" // defined timezone

/**
 * system_clock 精度为微妙，纳秒会有精度丢失
 */
system_clock::time_point fromisoformat(const char *time_string)
{
  std::tm t = {};
  std::istringstream ss(time_string);
  ss >> std::get_time(&t, "%F %T");
  if (ss.fail())
  {
    throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
  }
  // mktime的实现会 += timezone
  std::time_t sec = std::mktime(&t) - timezone;
  auto tp = system_clock::from_time_t(sec);
  char dot;
  if (ss >> dot && dot == '.')
  {
    std::string subseconds;
    std::getline(ss, subseconds);
    for (char a : subseconds)
    {
      if (a < '0' || a > '9')
      {
        throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
      }
    }
    auto len = subseconds.size();
    if (len == 3)
    {
      tp += milliseconds(std::stoi(subseconds));
    }
    else if (len == 6)
    {
      tp += microseconds(std::stoi(subseconds));
    }
    else if (len > 6)
    {
      tp += microseconds(std::stoi(subseconds.substr(0, 6)));
      fprintf(stderr, "fromisoformat %s will lose nanoseconds part %s\n", time_string, subseconds.substr(6).c_str());
    }
    else
    {
      throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
    }
    return tp;
  }
  return tp;
}
