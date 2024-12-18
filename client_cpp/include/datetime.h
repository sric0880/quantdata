#include <string>
#include <ctime>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <stdexcept>

#include "time.h" // defined timezone

using namespace std::chrono;

class DatetimeInputError : public std::invalid_argument
{
  using std::invalid_argument::invalid_argument;
};

system_clock::time_point now()
{
  return system_clock::now();
}

template <class _Rep, class _Period>
constexpr system_clock::time_point fromtimestamp(duration<_Rep, _Period> time_since_epoch)
{
  return system_clock::time_point(time_since_epoch);
}

template <typename Interger>
constexpr system_clock::time_point fromtimestamp(Interger sec)
{
  return fromtimestamp(seconds(sec));
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
 * system_clock 精度为微妙，纳秒会有精度丢失
 */
template <typename Interger>
constexpr system_clock::time_point fromtimestamp_nano(Interger nano)
{
  return fromtimestamp(duration_cast<microseconds>(nanoseconds(nano)));
}

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

template <class Interger1>
std::string isoformat_millisec(Interger1 /*long*/ millisec)
{
  return isoformat_millisec(millisec / 1000, millisec % 1000);
}

template <class Interger1>
std::string isoformat_microsec(Interger1 /*long*/ microsec)
{
  return isoformat_microsec(microsec / 1000000, microsec % 1000000);
}

template <class Interger1>
std::string isoformat_nanosec(Interger1 /*long*/ nanosec)
{
  return isoformat_nanosec(nanosec / 1000000000, nanosec % 1000000000);
}

template <class _Rep, class _Period>
std::string isoformat(duration<_Rep, _Period> time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch).count();
  return isoformat(seconds_since_epoch);
}

template <>
std::string isoformat<nanoseconds::rep, nanoseconds::period>(nanoseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_nanosec(seconds_since_epoch.count(), subseconds.count());
}

template <>
std::string isoformat<microseconds::rep, microseconds::period>(microseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_microsec(seconds_since_epoch.count(), subseconds.count());
}

template <>
std::string isoformat<milliseconds::rep, milliseconds::period>(milliseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto subseconds = time_since_epoch - seconds_since_epoch;
  return isoformat_millisec(seconds_since_epoch.count(), subseconds.count());
}

template <>
std::string isoformat<system_clock::time_point>(system_clock::time_point tp)
{
  return isoformat(tp.time_since_epoch());
}

struct Date
{
  int year;
  int mon; /* months since January [1-12] */
  int day; /* day of the month [1-31] */
};

using ratio_one = std::ratio<1>;

template <class _Period = ratio_one>
struct Time
{
  static_assert((std::is_same_v<_Period, std::nano> || std::is_same_v<_Period, std::micro> || std::is_same_v<_Period, std::milli> || std::is_same_v<_Period, std::ratio<1>>), "");
  typedef duration<long long, _Period> precision;
  int hour;       /* hours since midnight [0-23] */
  int min;        /* minutes after the hour [0-59] */
  int sec;        /* seconds after the minute [0-60] */
  int subseconds; /* milliseconds or microseconds or nanoseconds depends on precision*/

  constexpr precision to_duration()
  {
    return precision(subseconds) + seconds(sec) + minutes(min) + hours(hour);
  }

  constexpr operator precision()
  {
    return to_duration();
  }
};

template <class _Period = ratio_one>
struct Datetime
{
  typedef typename Time<_Period>::precision precision;
  Date date;
  Time<_Period> time;

private:
  precision ts_; /* timestamp since epoch */

public:
  Datetime() = default;
  Datetime(Date d, Time<_Period> t) : date(d), time(t)
  {
    to_duration();
  }
  Datetime(int year, int mon, int day, int hour, int min, int sec) : Datetime({year, mon, day}, {hour, min, sec, 0}) {};
  Datetime(int year, int mon, int day, int hour, int min, int sec, int subseconds) : Datetime({year, mon, day}, {hour, min, sec, subseconds})
  {
    static_assert(!std::is_same_v<_Period, ratio_one>);
  };

  template <class _Rep, class __Period>
  explicit Datetime(duration<_Rep, __Period> time_since_epoch) : ts_(duration_cast<precision>(time_since_epoch))
  {
    seconds seconds_since_epoch = duration_cast<seconds>(ts_);
    precision subseconds = ts_ - seconds_since_epoch;
    std::time_t t = (std::time_t)seconds_since_epoch.count();
    std::tm *_tm = std::gmtime(&t);
    date.year = _tm->tm_year + 1900;
    date.mon = _tm->tm_mon + 1;
    date.day = _tm->tm_mday;
    time.hour = _tm->tm_hour;
    time.min = _tm->tm_min;
    time.sec = _tm->tm_sec;
    time.subseconds = subseconds.count();
  }

  std::string isoformat()
  {
    return ::isoformat(ts_);
  }

  // return time since epoch
  constexpr uint64_t timestamp()
  {
    return (uint64_t)ts_.count();
  }

  constexpr const precision &to_duration()
  {
    std::tm _tm{};
    _tm.tm_year = date.year - 1900;
    _tm.tm_mon = date.mon - 1;
    _tm.tm_mday = date.day;
    _tm.tm_hour = time.hour;
    _tm.tm_min = time.min;
    _tm.tm_sec = time.sec;
    std::time_t sec = std::mktime(&_tm) - timezone;
    ts_ = precision(time.subseconds) + seconds(sec);
    return ts_;
  }

  constexpr operator precision()
  {
    return ts_;
  }

  /** 对于纳秒 会有精度丢失 所以这里不能做隐式转换 */
  constexpr explicit operator system_clock::time_point()
  {
    return ::fromtimestamp(ts_);
  }
};

namespace datetime
{
  template <class _Period = ratio_one, class _Rep, class __Period>
  Datetime<_Period> fromtimestamp(duration<_Rep, __Period> time_since_epoch)
  {
    return Datetime<_Period>(time_since_epoch);
  }

  // 按精度来转换
  template <class _Period = ratio_one, typename Interger>
  Datetime<_Period> fromtimestamp(Interger t)
  {
    return Datetime<_Period>(typename Datetime<_Period>::precision(t));
  }
}

template <>
constexpr Datetime<std::nano>::operator system_clock::time_point()
{
  return ::fromtimestamp(duration_cast<system_clock::duration>(to_duration()));
}
