#pragma once
#pragma warning(disable : 4996)
#include <string>
#include <ctime>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <stdexcept>

using namespace std::chrono;
using nanoseconds100 = duration<nanoseconds::rep, std::ratio<1, 10'000'000>>;

class DatetimeInputError : public std::invalid_argument
{
  using std::invalid_argument::invalid_argument;
};

inline system_clock::time_point now()
{
  return system_clock::now();
}

template <class _Rep, class _Period>
constexpr system_clock::time_point fromtimestamp(duration<_Rep, _Period> time_since_epoch)
{
  return system_clock::time_point(duration_cast<system_clock::duration>(time_since_epoch));
}

template <typename Interger>
constexpr system_clock::time_point fromtimestamp(Interger sec)
{
  return fromtimestamp(seconds(sec));
}

template <>
constexpr system_clock::time_point fromtimestamp<double>(double ts)
{
  return system_clock::time_point(system_clock::time_point::duration(static_cast<typename system_clock::time_point::rep>(ts * system_clock::time_point::period::den)));
}

template <typename Interger>
constexpr system_clock::time_point fromtimestamp_milli(Interger milli)
{
  return fromtimestamp(milliseconds(milli));
}

template <typename Interger>
constexpr system_clock::time_point fromtimestamp_micro(Interger micro)
{
  return fromtimestamp(microseconds(micro));
}

/**
 * system_clock 部分系统精度为微妙，纳秒会有精度丢失
 */
template <typename Interger>
constexpr system_clock::time_point fromtimestamp_nano(Interger nano)
{
  printf("Integer nanoseconds to system_clock::time_point may lose precision\n");
  return fromtimestamp(nanoseconds(nano));
}

/**
 * system_clock 部分系统精度为微妙，纳秒会有精度丢失
 */
system_clock::time_point fromisoformat(const char *time_string);

template <typename Interger>
std::string isoformat(Interger sec)
{
  std::time_t t = (std::time_t)sec;
  std::ostringstream ss;
  ss << std::put_time(std::gmtime(&t), "%F %T");
  return std::string(ss.str());
}

template <class Interger1, class Interger2>
std::string isoformat_millisec(Interger1 sec, Interger2 /*long*/ milli)
{
  std::time_t t = (std::time_t)sec;
  std::ostringstream ss;
  ss << std::put_time(std::gmtime(&t), "%F %T") << "." << std::setw(3) << std::setfill('0') << milli;
  return std::string(ss.str());
}

template <class Interger1, class Interger2>
std::string isoformat_microsec(Interger1 sec, Interger2 /*long*/ micro)
{
  std::time_t t = (std::time_t)sec;
  std::ostringstream ss;
  ss << std::put_time(std::gmtime(&t), "%F %T") << "." << std::setw(6) << std::setfill('0') << micro;
  return std::string(ss.str());
}

template <class Interger1, class Interger2>
std::string isoformat_nanosec(Interger1 sec, Interger2 /*long*/ nano)
{
  std::time_t t = (std::time_t)sec;
  std::ostringstream ss;
  ss << std::put_time(std::gmtime(&t), "%F %T") << "." << std::setw(9) << std::setfill('0') << nano;
  return std::string(ss.str());
}

template <class Interger1, class Interger2>
std::string isoformat_nano100sec(Interger1 sec, Interger2 /*long*/ nano)
{
  std::time_t t = (std::time_t)sec;
  std::ostringstream ss;
  ss << std::put_time(std::gmtime(&t), "%F %T") << "." << std::setw(7) << std::setfill('0') << nano;
  return std::string(ss.str());
}

template <class Interger1>
std::string isoformat_millisec(Interger1 /*long*/ millisec)
{
  return isoformat_millisec(millisec / 1000, millisec % 1000);
}

template <class Interger1>
std::string isoformat_microsec(Interger1 /*long*/ microsec)
{
  return isoformat_microsec(microsec / 1000'000, microsec % 1000'000);
}

template <class Interger1>
std::string isoformat_nanosec(Interger1 /*long*/ nanosec)
{
  return isoformat_nanosec(nanosec / 1'000'000'000, nanosec % 1'000'000'000);
}

template <class Interger1>
std::string isoformat_nano100sec(Interger1 /*long*/ nanosec)
{
  return isoformat_nano100sec(nanosec / 10'000'000, nanosec % 10'000'000);
}

template <class _Rep, class _Period>
std::string isoformat(duration<_Rep, _Period> time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch).count();
  return isoformat(seconds_since_epoch);
}

template <>
inline std::string isoformat<nanoseconds::rep, nanoseconds::period>(nanoseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_nanosec(seconds_since_epoch.count(), subseconds.count());
}

template <>
inline std::string isoformat<nanoseconds100::rep, nanoseconds100::period>(nanoseconds100 time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_nano100sec(seconds_since_epoch.count(), subseconds.count());
}

template <>
inline std::string isoformat<microseconds::rep, microseconds::period>(microseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_microsec(seconds_since_epoch.count(), subseconds.count());
}

template <>
inline std::string isoformat<milliseconds::rep, milliseconds::period>(milliseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_millisec(seconds_since_epoch.count(), subseconds.count());
}

template <>
inline std::string isoformat<system_clock::time_point>(system_clock::time_point tp)
{
  return isoformat(tp.time_since_epoch());
}

constexpr int _DAYS_IN_MONTH[] = {-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int _DAYS_BEFORE_MONTH[13] = {-1, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

// year -> 1 if leap year, else 0.
constexpr bool _is_leap(int year)
{
  return (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
}

// year -> number of days before January 1st of year.
constexpr int _days_before_year(int year)
{
  int y = year - 1;
  return y * 365 + y / 4 - y / 100 + y / 400;
}

// year, month -> number of days in that month in that year.
constexpr int _days_in_month(int year, int month)
{
  // assert((1 <= month) && (month <= 12));
  if (month == 2 && _is_leap(year))
    return 29;
  return _DAYS_IN_MONTH[month];
}

// year, month -> number of days in year preceding first day of month.
constexpr int _days_before_month(int year, int month)
{
  // assert((1 <= month) && (month <= 12));
  return _DAYS_BEFORE_MONTH[month] + (month > 2 && _is_leap(year));
}

// year, month, day -> ordinal, considering 01-Jan-0001 as day 1.
constexpr int _ymd2ord(int year, int month, int day)
{
  // assert((1 <= month) && (month <= 12));
  int dim = _days_in_month(year, month);
  // assert((1 <= day) && (day <= dim)); // day must be in 1..dim
  return (_days_before_year(year) + _days_before_month(year, month) + day);
}

struct IsoCalendarDate
{
  int year;
  int week;    /* [1, 52/53] */
  int weekday; /* from monday to sunday [1, 7] */
};
struct Date
{
  int year;
  int mon; /* months since January [1-12] */
  int day; /* day of the month [1-31] */
  /*
  Return a named tuple containing ISO year, week number, and weekday.

  The first ISO week of the year is the (Mon-Sun) week
  containing the year's first Thursday; everything else derives
  from that.

  The first week is 1; Monday is 1 ... Sunday is 7.

  ISO calendar algorithm taken from python::datetime
  */
  IsoCalendarDate isocalendar() const;
  /*
  Return proleptic Gregorian ordinal for the year, month and day.

  January 1 of year 1 is day 1.  Only the year, month and day values
  contribute to the result.
  */
  constexpr int toordinal() const
  {
    return _ymd2ord(year, mon, day);
  }
};

template <class Precision = seconds>
struct Time
{
  static_assert(std::is_same_v<Precision, seconds> || std::is_same_v<Precision, milliseconds> || std::is_same_v<Precision, microseconds> || std::is_same_v<Precision, nanoseconds>);
  int hour;       /* hours since midnight [0-23] */
  int min;        /* minutes after the hour [0-59] */
  int sec;        /* seconds after the minute [0-60] */
  int subseconds; /* milliseconds or microseconds or nanoseconds depends on precision*/

  constexpr Precision to_duration() const
  {
    return Precision(subseconds) + seconds(sec + min * 60 + hour * 3600);
  }

  constexpr operator Precision() const
  {
    return to_duration();
  }
};

template <>
constexpr seconds Time<seconds>::to_duration() const
{
  return seconds(sec + min * 60 + hour * 3600);
}

template <class Precision = seconds>
struct Datetime
{
  typedef Precision precision_type;
  Date date;
  Time<Precision> time;

  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp
  constexpr Datetime(Date d, Time<Precision> t) : date(d), time(t) {}
  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp，默认本地时间
  constexpr Datetime(int year, int mon, int day, int hour = 0, int min = 0, int sec = 0) : Datetime({year, mon, day}, {hour, min, sec, 0}) {};
  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp，默认本地时间
  constexpr Datetime(int year, int mon, int day, int hour, int min, int sec, int subseconds) : Datetime({year, mon, day}, {hour, min, sec, subseconds})
  {
    static_assert(!std::is_same_v<Precision, seconds>);
  };

  template <class _Precision>
  explicit Datetime(_Precision time_since_epoch)
  {
    seconds seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
    Precision subseconds = duration_cast<Precision>(time_since_epoch) - seconds_since_epoch;
    std::time_t t = (std::time_t)seconds_since_epoch.count();
    std::tm *_tm = std::gmtime(&t);
    date.year = _tm->tm_year + 1900;
    date.mon = _tm->tm_mon + 1;
    date.day = _tm->tm_mday;
    time.hour = _tm->tm_hour;
    time.min = _tm->tm_min;
    time.sec = _tm->tm_sec;
    time.subseconds = (int)subseconds.count();
  }

  explicit Datetime(typename Precision::rep time_since_epoch) : Datetime(Precision(time_since_epoch)) {}

  Precision to_duration() const;

  std::string isoformat() const
  {
    return ::isoformat(to_duration());
  }

  // return time since epoch
  typename Precision::rep to_timestamp() const
  {
    return to_duration().count();
  }

  operator Precision() const
  {
    return to_duration();
  }

  /** 对于纳秒 部分系统会有精度丢失 所以这里不能做隐式转换 */
  explicit operator system_clock::time_point() const
  {
    return ::fromtimestamp(to_duration());
  }
};

template <class _Precision_x, class _Precision_y>
bool operator==(const Datetime<_Precision_x> &x, const Datetime<_Precision_y> &y)
{
  return x.to_duration() == y.to_duration();
}

template <class _Precision_x, class _Precision_y>
auto operator-(const Datetime<_Precision_x> &x, const Datetime<_Precision_y> &y)
{
  return x.time.to_duration() - y.time.to_duration() + seconds((x.date.toordinal() - y.date.toordinal()) * 86400);
}

constexpr const seconds::rep seconds_till_epoch = (seconds::rep)Date{1970, 1, 1}.toordinal() * 86400;

template <class Precision>
Precision Datetime<Precision>::to_duration() const
{
  // 效率比较低
  // std::tm _tm{};
  // _tm.tm_year = date.year - 1900;
  // _tm.tm_mon = date.mon - 1;
  // _tm.tm_mday = date.day;
  // _tm.tm_hour = time.hour;
  // _tm.tm_min = time.min;
  // _tm.tm_sec = time.sec;
  // std::time_t sec = std::mktime(&_tm) - __tzoffset;
  // return precision(time.subseconds) + seconds(sec);
  return time.to_duration() + seconds((seconds::rep)date.toordinal() * 86400 - seconds_till_epoch);
}

namespace datetime
{
  // 按精度来转换
  template <class Precision = seconds, class _Duration_or_Integer>
  inline Datetime<Precision> fromtimestamp(_Duration_or_Integer t)
  {
    return Datetime<Precision>(t);
  }
}

template <>
inline Datetime<nanoseconds>::operator system_clock::time_point() const
{
  printf("Datetime<nanoseconds> to system_clock::time_point may lose precision\n");
  return ::fromtimestamp(duration_cast<system_clock::duration>(to_duration()));
}
