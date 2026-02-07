package com.tomshley.boilerplate.jvm.cloudevents

import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import java.util.Base64

// Based on https://github.com/cloudevents/sdk-java/blob/master/core/src/main/java/io/cloudevents/core/v1/CloudEventV1.java
// and https://github.com/cloudevents/sdk-java/blob/master/core/src/main/java/io/cloudevents/core/impl/BaseCloudEvent.java
// Json definition is here https://github.com/cloudevents/spec/blob/master/spec.json

/*
 *    "id": "description": "Identifies the event.", "examples": ["A234-1234-1234"]
 *    "source": "description": "Identifies the context in which an event happened.","examples" : [
 *       "https://github.com/cloudevents",
 *       "mailto:cncf-wg-serverless@lists.cncf.io",
 *       "urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66",
 *       "cloudevents/spec/pull/123",
 *       "/sensors/tn-1234567/alerts",
 *       "1-555-123-4567"]
 *    "specversion": "description": "The version of the CloudEvents specification which the event uses.","examples": ["1.x-wip"]
 *    "type": "description": "Describes the type of event related to the originating occurrence.","examples" : [
 *        "com.github.pull_request.opened",
 *        "com.example.object.deleted.v2"]
 *    "datacontenttype": "description": "Content type of the data value. Must adhere to RFC 2046 format.", "examples": [
 *        "text/xml",
 *        "application/json",
 *        "image/png",
 *           },
 *    "dataschema": "description": "Identifies the kafka that data adheres to.",
 *    "subject": "description": "Describes the subject of the event in the context of the event producer (identified by source).","examples": ["mynewfile.jpg"]
 *    "time": "description": "Timestamp of when the occurrence happened. Must adhere to RFC 3339.", "examples": ["2018-04-05T17:31:00Z"]
 *    "data": "description": "The event payload.", "examples": ["<much wow=\"xml\"/>"]}
 *    "data_base64": "description": "Base64 encoded event payload. Must adhere to RFC4648.","examples": ["Zm9vYg=="]
 *
 *    "required": ["id", "source", "specversion", "type"]
 *
 *    To allow some of the variables to come from the header we make all of the variables here optional
 */

case class CloudEvent(id: String,
                      source: String,
                      specversion: String,
                      `type`: String,
                      datacontenttype: Option[String] = None,
                      dataschema: Option[String] = None,
                      subject: Option[String] = None,
                      time: Option[String] = None,
                      data: Option[String] = None,
                      data_base64: Option[Array[Byte]] = None,
                      extensions: Option[Map[String, String]] = None)
    extends MarshallModel[CloudEvent] {

  def dataBase64String: Option[String] =
    data_base64.map(bytes => Base64.getEncoder.encodeToString(bytes))
}
