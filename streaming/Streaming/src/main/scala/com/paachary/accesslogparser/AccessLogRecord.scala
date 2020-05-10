package com.paachary.accesslogparser

case class AccessLogRecord(
                          clientIPAddress : String,
                          rfc1413ClientIdentify : String,
                          remoteUser : String,
                          dateTime: String,
                          request: String,
                          httpStatusCode: String,
                          bytesSent: String,
                          referer: String,
                          userAgent: String
                          )
