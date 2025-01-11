#include "quantdata/datetime.h" // defined timezone
#include <assert.h>

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
      if (casted != nanos)
      {
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

constexpr const int _DAYS_IN_MONTH[] = {-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr const int _DAYS_BEFORE_MONTH[13] = {-1, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

// year -> 1 if leap year, else 0.
bool _is_leap(int year)
{
  return (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
}

// year -> number of days before January 1st of year.
int _days_before_year(int year)
{
  int y = year - 1;
  return y * 365 + y / 4 - y / 100 + y / 400;
}

// year, month -> number of days in that month in that year.
int _days_in_month(int year, int month)
{
  // assert((1 <= month) && (month <= 12));
  if (month == 2 && _is_leap(year))
    return 29;
  return _DAYS_IN_MONTH[month];
}

// year, month -> number of days in year preceding first day of month.
int _days_before_month(int year, int month)
{
  // assert((1 <= month) && (month <= 12));
  return _DAYS_BEFORE_MONTH[month] + (month > 2 && _is_leap(year));
}

// year, month, day -> ordinal, considering 01-Jan-0001 as day 1.
int _ymd2ord(int year, int month, int day)
{
  // assert((1 <= month) && (month <= 12));
  int dim = _days_in_month(year, month);
  // assert((1 <= day) && (day <= dim)); // day must be in 1..dim
  return (_days_before_year(year) + _days_before_month(year, month) + day);
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
