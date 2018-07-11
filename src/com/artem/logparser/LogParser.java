package com.artem.logparser;

import com.artem.logparser.query.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {
    private List<Record> records;
    private SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");


    @Override
    public Set<String> getAllUsers() {
        Set<String> allUsers = new HashSet<>();
        for (Record record : records) {
            allUsers.add(record.user);
        }
        return allUsers;
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        Set<String> uniqueUsers = new HashSet<>();
        for (Record record : records) {
            if (isBetween(record.date, after, before)) {
                uniqueUsers.add(record.user);
            }
        }
        return uniqueUsers.size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (record.user.equals(user) && isBetween(record.date, after, before)) {
                events.add(record.event);
            }
        }
        return events.size();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.ip.equals(ip) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.LOGIN) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.DOWNLOAD_PLUGIN) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.WRITE_MESSAGE) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.SOLVE_TASK) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.SOLVE_TASK) &&
                    record.taskNumber == task && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.DONE_TASK) && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (Record record : records) {
            if (record.event.equals(Event.DONE_TASK) &&
                    record.taskNumber == task && isBetween(record.date, after, before)) {
                users.add(record.user);
            }
        }
        return users;
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(event) && isBetween(record.date, after, before)) {
                dates.add(record.date);
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Record record : records) {
            if (record.status.equals(Status.FAILED) && isBetween(record.date, after, before)) {
                dates.add(record.date);
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Record record : records) {
            if (record.status.equals(Status.ERROR) && isBetween(record.date, after, before)) {
                dates.add(record.date);
            }
        }
        return dates;
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        Date first = null;
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(Event.LOGIN) && isBetween(record.date, after, before)) {
                if (first == null) {
                    first = record.date;
                } else if (record.date.getTime() < first.getTime()) {
                    first = record.date;
                }
            }
        }
        return first;
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        Date first = null;
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(Event.SOLVE_TASK) && Integer.valueOf(task).equals(record.taskNumber) && isBetween(record.date, after, before)) {
                if (first == null) {
                    first = record.date;
                } else if (record.date.getTime() < first.getTime()) {
                    first = record.date;
                }
            }
        }
        return first;
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        Date first = null;
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(Event.DONE_TASK) && Integer.valueOf(task).equals(record.taskNumber) && isBetween(record.date, after, before)) {
                if (first == null) {
                    first = record.date;
                } else if (record.date.getTime() < first.getTime()) {
                    first = record.date;
                }
            }
        }
        return first;
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(Event.WRITE_MESSAGE) && isBetween(record.date, after, before)) {
                dates.add(record.date);
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Record record : records) {
            if (record.user.equals(user) && record.event.equals(Event.DOWNLOAD_PLUGIN) && isBetween(record.date, after, before)) {
                dates.add(record.date);
            }
        }
        return dates;
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        Set<Event> events = getAllEvents(after, before);
        return events.size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (isBetween(record.date, after, before))
                events.add(record.event);
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (record.ip.equals(ip) && isBetween(record.date, after, before))
                events.add(record.event);
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (record.user.equals(user) && isBetween(record.date, after, before))
                events.add(record.event);
        }
        return events;
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (record.status.equals(Status.FAILED) && isBetween(record.date, after, before))
                events.add(record.event);
        }
        return events;
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Record record : records) {
            if (record.status.equals(Status.ERROR) && isBetween(record.date, after, before))
                events.add(record.event);
        }
        return events;
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        ArrayList<Event> attempts = new ArrayList<>();
        for (Record record : records) {
            if (record.event.equals(Event.SOLVE_TASK) && Integer.valueOf(task).equals(record.taskNumber) && isBetween(record.date, after, before))
                attempts.add(record.event);
        }
        return attempts.size();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        ArrayList<Event> attempts = new ArrayList<>();
        for (Record record : records) {
            if (record.event.equals(Event.DONE_TASK) && Integer.valueOf(task).equals(record.taskNumber) && isBetween(record.date, after, before))
                attempts.add(record.event);
        }
        return attempts.size();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> map = new HashMap<>();
        for (Record record : records) {
            if (record.event.equals(Event.SOLVE_TASK) && record.taskNumber != null && isBetween(record.date, after, before)) {
                if (map.containsKey(record.taskNumber)) {
                    map.put(record.taskNumber, map.get(record.taskNumber) + 1);
                } else {
                    map.put(record.taskNumber, 1);
                }
            }

        }
        return map;
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> map = new HashMap<>();
        for (Record record : records) {
            if (record.event.equals(Event.DONE_TASK) && record.taskNumber != null && isBetween(record.date, after, before)) {
                if (map.containsKey(record.taskNumber)) {
                    map.put(record.taskNumber, map.get(record.taskNumber) + 1);
                } else {
                    map.put(record.taskNumber, 1);
                }
            }

        }
        return map;
    }

    @Override
    public Set<Object> execute(String query) {
        Set<Object> set = new HashSet<>();
        if (query == null || query.isEmpty()) return set;

        Pattern pattern = Pattern.compile("get (ip|user|date|event|status)"
                + "( for (ip|user|date|event|status) = \"(.*?)\")?"
                + "( and date between \"(.*?)\"? and \"(.*?)\")?");
        Matcher matcher = pattern.matcher(query);
        String field1 = null;
        String field2 = null;
        String value = null;
        String after = null;
        String before = null;
        if (matcher.find()) {
            field1 = matcher.group(1);
            field2 = matcher.group(3);
            value = matcher.group(4);
            after = matcher.group(6);
            before = matcher.group(7);
        }
        if (field1 == null) return null;
        if (field2 == null) field2 = field1;
        Date dateAfter = null;
        Date dateBefore = null;
        try {
            if (after != null) {
                dateAfter = formatter.parse(after);
            }
            if (before != null) {
                dateBefore = formatter.parse(before);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


        if (query.split(" ").length == 2) {
            for (Record record : records) {
                switch (query) {
                    case "get ip":
                        set.add(record.ip);
                        break;
                    case "get user":
                        set.add(record.user);
                        break;
                    case "get date":
                        set.add(record.date);
                        break;
                    case "get event":
                        set.add(record.event);
                        break;
                    case "get status":
                        set.add(record.status);
                        break;
                }
            }
            return set;
        } else {
            try {
                for (Record record : records) {
                    Field[] fields = record.getClass().getDeclaredFields();
                    if (field2.equals("ip") && record.ip.equals(value)
                            && isBetween(record.date, dateAfter, dateBefore)) {
                        set.add(record.getClass().getDeclaredField(field1).get(record));
                    } else if (field2.equals("user") && record.user.equals(value)
                            && isBetween(record.date, dateAfter, dateBefore)) {
                        set.add(record.getClass().getDeclaredField(field1).get(record));
                    } else if (field2.equals("date") && record.date.equals(formatter.parse(value))
                            && isBetween(record.date, dateAfter, dateBefore)) {
                        set.add(record.getClass().getDeclaredField(field1).get(record));
                    } else if (field2.equals("event") && record.event.equals(Event.valueOf(value))
                            && isBetween(record.date, dateAfter, dateBefore)) {
                        set.add(record.getClass().getDeclaredField(field1).get(record));
                    } else if (field2.equals("status") && record.status.equals(Status.valueOf(value))
                            && isBetween(record.date, dateAfter, dateBefore)) {
                        set.add(record.getClass().getDeclaredField(field1).get(record));
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return set;
        }
    }

    private class Record {
        String ip;
        String user;
        Date date;
        Event event;
        Integer taskNumber;
        Status status;

        @Override
        public String toString() {
            return "Record{" +
                    "ip='" + ip + '\'' +
                    ", user='" + user + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", taskNumber=" + taskNumber +
                    ", status=" + status +
                    '}';
        }
    }

    public LogParser(Path logDir) {
        records = new ArrayList<>();

        readRecords(logDir);
    }

    private void readRecords(Path logDir) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(logDir)) {
            for (Path log : directoryStream) {
                if (Files.isRegularFile(log) && log.toString().endsWith(".log")) {
                    BufferedReader reader = Files.newBufferedReader(log, StandardCharsets.UTF_8);
                    while (reader.ready()) {
                        Record record = new Record();
                        String[] entry = reader.readLine().split("\t");
                        record.ip = entry[0];
                        record.user = entry[1];
                        record.date = formatter.parse(entry[2]);

                        if (entry[3].indexOf(' ') == -1) {
                            record.event = Event.valueOf(entry[3]);
                            record.taskNumber = null;
                        } else {
                            String[] event = entry[3].split(" ");
                            record.event = Event.valueOf(event[0]);
                            record.taskNumber = Integer.parseInt(event[1]);
                        }

                        record.status = Status.valueOf(entry[4]);

                        records.add(record);
                    }
                    reader.close();
                } else {
                    if (Files.isDirectory(log)) {
                        readRecords(log);
                    }
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    private Set<Record> getFilteredEntries(Date after, Date before) {

        Set<Record> filteredRecords = new HashSet<>();
        for (Record record : records) {
            if (isBetween(record.date, after, before)) {
                filteredRecords.add(record);
            }
        }

        return filteredRecords;
    }

    private boolean isBetween(Date date, Date after, Date before) {
        return (after == null || date.after(after)) &&
                (before == null || date.before(before));
    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        Set<String> ips = getUniqueIPs(after, before);

        return ips.size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        Set<Record> filteredRecords = getFilteredEntries(after, before);
        Set<String> ips = new HashSet<>();

        for (Record record : filteredRecords) {
            ips.add(record.ip);
        }

        return ips;
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        Set<Record> filteredRecords = getFilteredEntries(after, before);
        Set<String> ips = new HashSet<>();

        for (Record record : filteredRecords) {
            if (record.user.equals(user))
                ips.add(record.ip);
        }

        return ips;
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        Set<Record> filteredRecords = getFilteredEntries(after, before);
        Set<String> ips = new HashSet<>();

        for (Record record : filteredRecords) {
            if (record.event.equals(event))
                ips.add(record.ip);
        }

        return ips;
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        Set<Record> filteredRecords = getFilteredEntries(after, before);
        Set<String> ips = new HashSet<>();

        for (Record record : filteredRecords) {
            if (record.status.equals(status))
                ips.add(record.ip);
        }

        return ips;
    }
}