package online.mwang.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * @author mwangli
 * @date 2022/3/28 10:01
 */
public class DateUtils {

    public static String format(Long timestamp, String pattern) {
        return FastDateFormat.getInstance(pattern).format(timestamp);
    }

    public static Date parse(String dateStr, String pattern) {
        try {
            return FastDateFormat.getInstance(pattern).parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取当前日期加目标天数的日期
     * Example: 昨天(-1)，明天(+1)，上周(-7)，30天内(-30)
     */
    public static Date getNextDate(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, days);
        return calendar.getTime();
    }

    public static void main(String[] args) {
        Date nextDate = getNextDate(new Date(), -3);
        System.out.println(nextDate);
    }
}
