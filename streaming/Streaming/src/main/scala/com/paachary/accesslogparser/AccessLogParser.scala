package com.paachary.accesslogparser

import java.util.regex.{Matcher, Pattern}


class AccessLogParser extends  Serializable {

    private val ddd = "\\d{1,3}"
    private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    private val client = "(\\S+)"
    private val user = "(\\S+)"
    private val dateTime = "(\\[.+?\\])"
    private val request = "\"(.*?)\""
    private val status = "(\\d{3})"
    private val bytes = "(\\S+)"
    private val referer = "\"(.*?)\""
    private val agent = "\"(.*?)\""
    private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    private val p = Pattern.compile(regex)

    def parseRecord(record: String): Option[AccessLogRecord] = {
        val matcher = p.matcher(record)
        if (matcher.find) {
            Some(buildAccessLogRecord(matcher))
        } else {
            println("record in parse record else ="+ record)
            None
        }
    }

    private def buildAccessLogRecord(matcher: Matcher) = {
        AccessLogRecord(
            matcher.group(1),
            matcher.group(2),
            matcher.group(3),
            matcher.group(4),
            matcher.group(5),
            matcher.group(6),
            matcher.group(7),
            matcher.group(8),
            matcher.group(9))
    }
}

object AccessLogParser {

    def parseRequestField(request: String): Option[Tuple3[String, String, String]] = {
        val arr = request.split(" ")
        if (arr.size == 3) Some((arr(0), arr(1), arr(2))) else None
    }
}
