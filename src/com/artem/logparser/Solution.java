package com.artem.logparser;

import java.nio.file.Paths;
import java.util.Date;
import java.util.Set;

public class Solution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("d:\\Java\\com.artem.logparser.logs\\"));
     /*   System.out.println(logParser.getNumberOfUniqueIPs(null, new Date()));
        System.out.println(logParser.getUniqueIPs(null, new Date()));
        System.out.println(logParser.getAllUsers());
        System.out.println(logParser.getNumberOfUserEvents("Eduard Petrovich Morozko", null, null));
       */  Set<Object> set = logParser.execute("get event for date = \"30.01.2014 12:56:22\"");
        for (Object o : set) {
            Event event = (Event)o;
            System.out.println(event);
        }
        Set<Object> set2 = logParser.execute("get ip for user = \"Eduard Petrovich Morozko\" and date between \"11.12.2013 0:00:00\" and \"03.01.2014 23:59:59\"");
        for (Object o : set2) {
            String ip = (String)o;
            System.out.println(ip);
        }
    }
}