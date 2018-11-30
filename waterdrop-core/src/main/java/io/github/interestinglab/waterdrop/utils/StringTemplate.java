package io.github.interestinglab.waterdrop.utils;

import org.apache.commons.lang.text.StrSubstitutor;

import java.text.SimpleDateFormat;
import java.util.*;

public class StringTemplate {

    /**
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * */
    public static String substitute(String str, String timeFormat) {

        final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        final String formatteddDate = sdf.format(new Date());

        final Map valuesMap = new HashMap();
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formatteddDate);
        valuesMap.put(timeFormat, formatteddDate);
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(str);
    }

    public static String substitute(String str, String timeFormat,int offset) {

        final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, offset);
        String formatteddDate = sdf.format(calendar.getTime());

        final Map valuesMap = new HashMap();
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formatteddDate);
        valuesMap.put(timeFormat, formatteddDate);
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(str);
    }
}
