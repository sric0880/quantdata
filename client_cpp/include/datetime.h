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

time_point<system_clock> now()
{
  return system_clock::now();
}

template <class _Rep, class _Period>
time_point<system_clock> fromtimestamp(duration<_Rep, _Period> time_since_epoch)
{
  return time_point<system_clock>(time_since_epoch);
}

template <typename Interger>
time_point<system_clock> fromtimestamp_milli(Interger milli)
{
  return fromtimestamp(milliseconds(milli));
}

template <typename Interger>
time_point<system_clock> fromtimestamp_micro(Interger micro)
{
  return fromtimestamp(microseconds(micro));
}

/**
 * system_clock 精度为微妙，纳秒会有精度丢失
 */
template <typename Interger>
time_point<system_clock> fromtimestamp_nano(Interger nano)
{
  return fromtimestamp(duration_cast<microseconds>(nanoseconds(nano)));
}

/**
 * system_clock 精度为微妙，纳秒会有精度丢失
 */
time_point<system_clock> fromisoformat(const char *time_string)
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
    std::string after_sec;
    std::getline(ss, after_sec);
    for (char a : after_sec)
    {
      if (a < '0' || a > '9')
      {
        throw DatetimeInputError(std::string(time_string) + " is invalid iso time format");
      }
    }
    auto len = after_sec.size();
    if (len == 3)
    {
      tp += milliseconds(std::stoi(after_sec));
    }
    else if (len == 6)
    {
      tp += microseconds(std::stoi(after_sec));
    }
    else if (len > 6)
    {
      tp += microseconds(std::stoi(after_sec.substr(0, 6)));
      fprintf(stderr, "fromisoformat %s will lose nanoseconds part %s\n", time_string, after_sec.substr(6).c_str());
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
  auto after_seconds = time_since_epoch - seconds_since_epoch;
  return isoformat_nanosec(seconds_since_epoch.count(), after_seconds.count());
}

template <>
std::string isoformat<microseconds::rep, microseconds::period>(microseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto after_seconds = time_since_epoch - seconds_since_epoch;
  return isoformat_microsec(seconds_since_epoch.count(), after_seconds.count());
}

template <>
std::string isoformat<milliseconds::rep, milliseconds::period>(milliseconds time_since_epoch)
{
  auto seconds_since_epoch = duration_cast<seconds>(time_since_epoch);
  auto after_seconds = time_since_epoch - seconds_since_epoch;
  return isoformat_millisec(seconds_since_epoch.count(), after_seconds.count());
}

template <>
std::string isoformat<time_point<system_clock>>(time_point<system_clock> tp)
{
  return isoformat(tp.time_since_epoch());
}