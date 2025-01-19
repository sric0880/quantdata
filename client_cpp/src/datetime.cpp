#include "quantdata/datetime.h" // defined timezone
#include <assert.h>

/**
 * system_clock 部分系统精度为微妙，纳秒会有精度丢失
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
      return static_cast<system_clock::time_point>(Datetime<milliseconds>(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, std::stoi(subseconds)));
    }
    else if (len == 6)
    {
      return static_cast<system_clock::time_point>(Datetime<microseconds>(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, std::stoi(subseconds)));
    }
    else if (len == 9)
    {
      return static_cast<system_clock::time_point>(Datetime<nanoseconds>(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, std::stoi(subseconds)));
    }
    else
    {
      throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
    }
  }
  return static_cast<system_clock::time_point>(Datetime(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec));
}

// Helper to calculate the day number of the Monday starting week 1
// XXX This could be done more efficiently
int _isoweek1monday(int year)
{
  const int THURSDAY = 3;
  int firstday = _ymd2ord(year, 1, 1);
  int firstweekday = (firstday + 6) % 7; // See weekday() above
  int week1monday = firstday - firstweekday;
  if (firstweekday > THURSDAY)
    week1monday += 7;
  return week1monday;
}

div_t py_divmod(int x, int y)
{
  div_t r = div(x, y);
  if (r.rem && (x < 0) != (y < 0))
  {
    r.quot -= 1;
    r.rem += y;
  }
  return r;
}

IsoCalendarDate Date::isocalendar() const
{
  int _year = year;
  int week1monday = _isoweek1monday(_year);
  int today = _ymd2ord(year, mon, day);
  // Internally, week and day have origin 0
  div_t dvm = py_divmod(today - week1monday, 7);
  int week, weekday;
  week = dvm.quot;
  weekday = dvm.rem;
  if (week < 0)
  {
    --_year;
    week1monday = _isoweek1monday(_year);
    dvm = py_divmod(today - week1monday, 7);
    week = dvm.quot;
    weekday = dvm.rem;
  }
  else if (week >= 52)
  {
    if (today >= _isoweek1monday(_year + 1))
    {
      ++_year;
      week = 0;
    }
  }
  return {_year, week + 1, weekday + 1};
}
