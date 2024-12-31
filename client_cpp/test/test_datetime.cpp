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
  // system_clock ɶ���ȶ��й�������ǧ��ٹ�
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

  // test isocalendar
  auto d0 = Date({2024, 12, 31}).isocalendar();
  QD_ASSERT((d0.year == 2025 && d0.week == 1 && d0.weekday == 2), "");
  auto d1 = Date({2025, 1, 1}).isocalendar();
  QD_ASSERT((d1.year == 2025 && d1.week == 1 && d1.weekday == 3), "");
  auto d2 = Date({2025, 1, 3}).isocalendar();
  QD_ASSERT((d2.year == 2025 && d2.week == 1 && d2.weekday == 5), "");
  auto d3 = Date({2025, 1, 6}).isocalendar();
  QD_ASSERT((d3.year == 2025 && d3.week == 2 && d3.weekday == 1), "");
  auto d4 = Date({2025, 1, 10}).isocalendar();
  QD_ASSERT((d4.year == 2025 && d4.week == 2 && d4.weekday == 5), "");
  auto d5 = Date({2025, 1, 15}).isocalendar();
  QD_ASSERT((d5.year == 2025 && d5.week == 3 && d5.weekday == 3), "");
  auto d6 = Date({2025, 1, 21}).isocalendar();
  QD_ASSERT((d6.year == 2025 && d6.week == 4 && d6.weekday == 2), "");
  auto d7 = Date({2025, 1, 28}).isocalendar();
  QD_ASSERT((d7.year == 2025 && d7.week == 5 && d7.weekday == 2), "");
  auto d8 = Date({2025, 2, 5}).isocalendar();
  QD_ASSERT((d8.year == 2025 && d8.week == 6 && d8.weekday == 3), "");
  auto d9 = Date({2025, 2, 14}).isocalendar();
  QD_ASSERT((d9.year == 2025 && d9.week == 7 && d9.weekday == 5), "");
  auto d10 = Date({2025, 2, 24}).isocalendar();
  QD_ASSERT((d10.year == 2025 && d10.week == 9 && d10.weekday == 1), "");
  auto d11 = Date({2025, 3, 7}).isocalendar();
  QD_ASSERT((d11.year == 2025 && d11.week == 10 && d11.weekday == 5), "");
  auto d12 = Date({2025, 3, 19}).isocalendar();
  QD_ASSERT((d12.year == 2025 && d12.week == 12 && d12.weekday == 3), "");
  auto d13 = Date({2025, 4, 1}).isocalendar();
  QD_ASSERT((d13.year == 2025 && d13.week == 14 && d13.weekday == 2), "");
  auto d14 = Date({2025, 4, 15}).isocalendar();
  QD_ASSERT((d14.year == 2025 && d14.week == 16 && d14.weekday == 2), "");
  auto d15 = Date({2025, 4, 30}).isocalendar();
  QD_ASSERT((d15.year == 2025 && d15.week == 18 && d15.weekday == 3), "");
  auto d16 = Date({2025, 5, 16}).isocalendar();
  QD_ASSERT((d16.year == 2025 && d16.week == 20 && d16.weekday == 5), "");
  auto d17 = Date({2025, 6, 2}).isocalendar();
  QD_ASSERT((d17.year == 2025 && d17.week == 23 && d17.weekday == 1), "");
  auto d18 = Date({2025, 6, 20}).isocalendar();
  QD_ASSERT((d18.year == 2025 && d18.week == 25 && d18.weekday == 5), "");
  auto d19 = Date({2025, 7, 9}).isocalendar();
  QD_ASSERT((d19.year == 2025 && d19.week == 28 && d19.weekday == 3), "");
}