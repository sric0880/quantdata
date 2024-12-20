#pragma once
#pragma warning(disable : 4996)
#include <string>
#include <ctime>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <stdexcept>

#include "time.h" // defined timezone

#ifdef _WIN32
#define __tzoffset _timezone
#else
#define __tzoffset timezone
#endif

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

struct Date
{
  int year;
  int mon; /* months since January [1-12] */
  int day; /* day of the month [1-31] */
};

using ratio_one = std::ratio<1>;

template <class _Period>
struct Precision;
template <>
struct Precision<std::nano>
{
  typedef nanoseconds type;
};
template <>
struct Precision<std::micro>
{
  typedef microseconds type;
};
template <>
struct Precision<std::milli>
{
  typedef milliseconds type;
};
template <>
struct Precision<ratio_one>
{
  typedef seconds type;
};
template <class _Period = ratio_one>
struct Time
{
  typedef typename Precision<_Period>::type precision;
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

  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp
  constexpr Datetime(Date d, Time<_Period> t) : date(d), time(t)
  {
    std::tm _tm{};
    _tm.tm_year = date.year - 1900;
    _tm.tm_mon = date.mon - 1;
    _tm.tm_mday = date.day;
    _tm.tm_hour = time.hour;
    _tm.tm_min = time.min;
    _tm.tm_sec = time.sec;
    std::time_t sec = std::mktime(&_tm) - __tzoffset;
    ts_ = precision(time.subseconds) + seconds(sec);
  }
  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp，默认本地时间
  constexpr Datetime(int year, int mon, int day, int hour = 0, int min = 0, int sec = 0) : Datetime({year, mon, day}, {hour, min, sec, 0}) {};
  // 一定要是UTC时间，不同时区，直接影响到底层的timestamp，默认本地时间
  constexpr Datetime(int year, int mon, int day, int hour, int min, int sec, int subseconds) : Datetime({year, mon, day}, {hour, min, sec, subseconds})
  {
    static_assert(!std::is_same_v<_Period, ratio_one>);
  };

  template <class _Rep, class __Period>
  constexpr explicit Datetime(duration<_Rep, __Period> time_since_epoch) : ts_(duration_cast<precision>(time_since_epoch))
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
    time.subseconds = (int)subseconds.count();
  }

  std::string isoformat()
  {
    return ::isoformat(ts_);
  }

  // return time since epoch
  constexpr typename precision::rep to_timestamp() const
  {
    return ts_.count();
  }

  constexpr const precision &to_duration() const
  {
    return ts_;
  }

  constexpr operator precision() const
  {
    return ts_;
  }

  /** 对于纳秒 部分系统会有精度丢失 所以这里不能做隐式转换 */
  constexpr explicit operator system_clock::time_point() const
  {
    return ::fromtimestamp(ts_);
  }

private:
  precision ts_; /* timestamp since epoch */
};

template <class _Period>
bool operator==(const Datetime<_Period> &x, const Datetime<_Period> &y)
{
  return x.timestamp() == y.timestamp();
}

namespace datetime
{
  template <class _Period = ratio_one, class _Rep, class __Period>
  inline Datetime<_Period> fromtimestamp(duration<_Rep, __Period> time_since_epoch)
  {
    return Datetime<_Period>(time_since_epoch);
  }

  // 按精度来转换
  template <class _Period = ratio_one, typename Interger>
  inline Datetime<_Period> fromtimestamp(Interger t)
  {
    return Datetime<_Period>(typename Datetime<_Period>::precision(t));
  }
}

template <>
inline constexpr Datetime<std::nano>::operator system_clock::time_point() const
{
  return ::fromtimestamp(duration_cast<system_clock::duration>(ts_));
}
