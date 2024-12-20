#include "quantdata/datetime.h" // defined timezone

/**
 * system_clock 精度为微妙，纳秒会有精度丢失
 * 不支持时区，默认为UTC时区
 */
system_clock::time_point fromisoformat(const char *time_string)
{
  std::tm t = {};
  std::istringstream ss(time_string);
  ss >> std::get_time(&t, "%Y-%m-%d %T");
  if (ss.fail())
  {
    throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
  }
  // mktime的实现会 += timezone
  std::time_t sec = std::mktime(&t) - __tzoffset;
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
    else if (len == 9)
    {
      nanoseconds nanos(std::stoi(subseconds));
      auto casted = duration_cast<system_clock::duration>(nanos);
      if (casted != nanos) {
        fprintf(stderr, "fromisoformat %s will lose precision\n", time_string);
      }
      tp += casted;
    }
    else
    {
      throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
    }
    return tp;
  }
  return tp;
}
