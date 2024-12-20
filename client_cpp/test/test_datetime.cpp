#include <iostream>
#include "quantdata/macros.h"
#include "quantdata/datetime.h"

int main(int, char **)
{
  auto nowtime = now();
  auto micro_count = nowtime.time_since_epoch().count();
  auto nano = nanoseconds(1734412701444555666);
  auto micro = duration_cast<microseconds>(nano);
  auto milli = duration_cast<milliseconds>(micro);
  auto sec = duration_cast<seconds>(milli);
  auto mins = duration_cast<minutes>(sec);
  auto hour = duration_cast<hours>(mins);
  std::cout << isoformat(nowtime) << std::endl;
  QD_ASSERT(isoformat(sec) == "2024-12-17 05:18:21", "");
  QD_ASSERT(isoformat(mins) == "2024-12-17 05:18:00", "");
  QD_ASSERT(isoformat(hour) == "2024-12-17 05:00:00", "");
  QD_ASSERT(isoformat(milli) == "2024-12-17 05:18:21.444", "");
  QD_ASSERT(isoformat(micro) == "2024-12-17 05:18:21.444555", "");
  QD_ASSERT(isoformat(nano) == "2024-12-17 05:18:21.444555666", "");
  QD_ASSERT(isoformat_millisec(milli.count()) == isoformat(milli), "");
  QD_ASSERT(isoformat_microsec(micro.count()) == isoformat(micro), "");
  QD_ASSERT(isoformat_nanosec(nano.count()) == isoformat(nano), "");
  // fromisoformat("2024-07-29 00:00:00.");      // error
  try
  {
    fromisoformat("2024-07-29 00:00:00.01g999"); // error
    QD_ASSERT(false, "no err: xxx is invalid iso time format");
  }
  catch (std::exception &)
  {
  }
  try
  {
    fromisoformat("2024-07-29 00:00:00.01"); // error
    QD_ASSERT(false, "no err: xxx is invalid iso time format");
  }
  catch (std::exception &)
  {
  }
  // system_clock 啥精度都有哈，真是千奇百怪
  if constexpr (std::is_same_v<system_clock::duration, nanoseconds>)
  {
    QD_ASSERT("2024-07-29 00:00:00.999666777" == isoformat(fromisoformat("2024-07-29 00:00:00.999666777")), "");
    QD_ASSERT("2024-07-29 00:00:00.000000000" == isoformat(fromisoformat("2024-07-29 00:00:00")), "");
    QD_ASSERT("2024-07-29 00:00:00.000000000" == isoformat(fromisoformat("2024-07-29 00:00:00.000000")), "");
    QD_ASSERT("2024-07-29 00:00:00.000001000" == isoformat(fromisoformat("2024-07-29 00:00:00.000001")), "");
    QD_ASSERT("2024-07-29 00:00:00.999999000" == isoformat(fromisoformat("2024-07-29 00:00:00.999999")), "");
    QD_ASSERT("2024-07-29 00:00:00.999000000" == isoformat(fromisoformat("2024-07-29 00:00:00.999")), "");
  }
  else if constexpr (std::is_same_v<system_clock::duration::period, std::ratio<1, 10'000'000>>)
  {
    QD_ASSERT("2024-07-29 00:00:00.9996667" == isoformat(fromisoformat("2024-07-29 00:00:00.999666777")), "");
    QD_ASSERT("2024-07-29 00:00:00.0000000" == isoformat(fromisoformat("2024-07-29 00:00:00")), "");
    QD_ASSERT("2024-07-29 00:00:00.0000000" == isoformat(fromisoformat("2024-07-29 00:00:00.000000")), "");
    QD_ASSERT("2024-07-29 00:00:00.0000010" == isoformat(fromisoformat("2024-07-29 00:00:00.000001")), "");
    QD_ASSERT("2024-07-29 00:00:00.9999990" == isoformat(fromisoformat("2024-07-29 00:00:00.999999")), "");
    QD_ASSERT("2024-07-29 00:00:00.9990000" == isoformat(fromisoformat("2024-07-29 00:00:00.999")), "");
  }
  else
  {
    QD_ASSERT("2024-07-29 00:00:00.999666" == isoformat(fromisoformat("2024-07-29 00:00:00.999666777")), "");
    QD_ASSERT("2024-07-29 00:00:00.000000" == isoformat(fromisoformat("2024-07-29 00:00:00")), "");
    QD_ASSERT("2024-07-29 00:00:00.000000" == isoformat(fromisoformat("2024-07-29 00:00:00.000000")), "");
    QD_ASSERT("2024-07-29 00:00:00.000001" == isoformat(fromisoformat("2024-07-29 00:00:00.000001")), "");
    QD_ASSERT("2024-07-29 00:00:00.999999" == isoformat(fromisoformat("2024-07-29 00:00:00.999999")), "");
    QD_ASSERT("2024-07-29 00:00:00.999000" == isoformat(fromisoformat("2024-07-29 00:00:00.999")), "");
  }

  QD_ASSERT(fromtimestamp(sec.count()) == time_point<system_clock>(sec), "");
  QD_ASSERT(fromtimestamp_milli(milli.count()) == time_point<system_clock>(milli), "");
  QD_ASSERT(fromtimestamp_micro(micro.count()) == time_point<system_clock>(micro), "");
  QD_ASSERT(fromtimestamp_nano(nano.count()) == system_clock::time_point(duration_cast<system_clock::duration>(nano)), "");

  QD_ASSERT(datetime::fromtimestamp<std::micro>(micro.count()).isoformat() == "2024-12-17 05:18:21.444555", "");
  QD_ASSERT(datetime::fromtimestamp<std::milli>(micro).isoformat() == "2024-12-17 05:18:21.444", "");
  QD_ASSERT(datetime::fromtimestamp(sec.count()).isoformat() == "2024-12-17 05:18:21", "");
  QD_ASSERT(datetime::fromtimestamp(sec).isoformat() == "2024-12-17 05:18:21", "");
  QD_ASSERT(Datetime<std::milli>({2024, 12, 17}, {5, 18, 21, 444}).isoformat() == "2024-12-17 05:18:21.444", "");
  QD_ASSERT(Datetime<std::milli>(2024, 12, 17, 5, 18, 21, 444).isoformat() == "2024-12-17 05:18:21.444", "");
  QD_ASSERT(Datetime(2024, 12, 17, 5, 18, 21).isoformat() == "2024-12-17 05:18:21", "");
  // QD_ASSERT(Datetime<std::milli>({2024, 12, 17}, {5, 18, 21, 444}).isoformat() == "2024-12-17 05:18:21.444", "");
}