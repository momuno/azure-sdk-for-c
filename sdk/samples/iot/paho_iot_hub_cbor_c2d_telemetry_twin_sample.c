// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef _MSC_VER
#pragma warning(push)
// warning C4201: nonstandard extension used: nameless struct/union
#pragma warning(disable : 4201)
#endif
#include <paho-mqtt/MQTTClient.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include <azure/az_core.h>
#include <azure/az_iot.h>

#include "iot_sample_common.h"

#ifdef __GNUC__
#pragma GCC diagnostic push
// warning within TinyCBOR library: conversion from 'int' to uint8_t'
#pragma GCC diagnostic ignored "-Wconversion"
#endif
#include "cbor.h"
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#define SAMPLE_TYPE PAHO_IOT_HUB
#define SAMPLE_NAME PAHO_IOT_HUB_CBOR_C2D_TELEMETRY_TWIN_SAMPLE

#define RID_BUFFER_SIZE 64
#define STANDARD_BUFFER_SIZE 128
#define USERNAME_BUFFER_SIZE 256

#define MAX_MESSAGE_COUNT 10
#define MQTT_TIMEOUT_RECEIVE_MS (30 * 1000)
#define MQTT_TIMEOUT_DISCONNECT_MS (10 * 1000)

/**
 * The content type system property for C2D and/or telemetry messages will appear as a key-value
 * pair appended to the topic. They key is SDK-defined as `$.ct`, and URL-encoded as `%24.ct`. The
 * value is application-defined and must be agreed upon between the device and service side
 * applications. Examples for this value include `text%2Fplain` and `application%2Fjson`. To
 * demonstrate setting the content type system property, this sample uses `application%2Fcbor`.
 * See az_iot_common.h for more system properties available to set for C2D and telemetry messaging.
 */
#define CONTENT_TYPE_C2D "application%2Fcbor" // application-defined
#define CONTENT_TYPE_TELEMETRY "application%2Fcbor" // application-defined

static az_span const twin_get_rid_base = AZ_SPAN_LITERAL_FROM_STR("tg-");
static uint64_t twin_get_rid_num = 0;
static az_span const reported_property_rid_base = AZ_SPAN_LITERAL_FROM_STR("rp-");
static uint64_t reported_property_rid_num = 0;

static char* const twin_desired_name = "desired";
static char* const twin_property_device_count_name = "device_count";
static int64_t twin_property_device_count_value = 0;
static char* const telemetry_property_message_iteration_name = "telemetry_message_iteration";
static int64_t telemetry_property_message_iteration_value = 0;

static iot_sample_environment_variables env_vars;
static az_iot_hub_client hub_client;
static MQTTClient mqtt_client;
static char mqtt_client_username_buffer[USERNAME_BUFFER_SIZE];

// Functions
static void create_and_configure_mqtt_client(void);
static void connect_mqtt_client_to_iot_hub(void);
static void subscribe_mqtt_client_to_iot_hub_topics(void);
static void send_and_receive_messages(void);
static void disconnect_mqtt_client_from_iot_hub(void);

static void request_twin_document(void);
static void send_reported_property(void);
static void send_telemetry(void);
static void receive_message(void);
static void handle_message(char* topic, int topic_len, MQTTClient_message const* message);
static void handle_c2d_message(az_span message_span, az_iot_hub_client_c2d_request* c2d_request);
static void handle_device_twin_message(
    az_span message_span,
    az_iot_hub_client_twin_response* twin_response);
static bool parse_cbor_desired_property(az_span message_span, int64_t* out_parsed_device_count);
static bool update_property_device_count(int64_t new_device_count);
static void build_cbor_reported_property(
    uint8_t* reported_property_payload,
    size_t reported_property_payload_size,
    size_t* out_reported_property_payload_length);
static void build_cbor_telemetry(
    uint8_t* telemetry_payload,
    size_t telemetry_payload_size,
    size_t* out_telemetry_payload_length);
static void generate_rid_span(az_span base_span, uint64_t unique_id, az_span* out_rid_span);

/*
 * This sample utilizes the Azure IoT Hub to get the device twin document, send a reported
 * property message, and receive desired property messages all in CBOR data format. It also shows
 * how to set the content type system property for C2D and telemetry messaging. After 10 attempts to
 * receive a C2D or desired property message, the sample will exit.
 *
 * To run this sample, Intel's MIT licensed TinyCBOR library must be installed. The Embedded C SDK
 * is not dependent on any particular CBOR library. X509 self-certification is used.
 *
 * Device Twin:
 * A property named `device_count` is used. To send a device twin desired property message, select
 * your device's Device Twin tab in the Azure Portal of your IoT Hub. Add the property
 * `device_count` along with a corresponding value to the `desired` section of the twin JSON. Select
 * Save to update the twin document and send the twin message to the device. The IoT Hub will
 * translate the twin JSON into CBOR for the device to consume and decode.
 *
 * {
 *   "properties": {
 *     "desired": {
 *       "device_count": 42,
 *     }
 *   }
 * }
 *
 * No other property names sent in a desired property message are supported. If any are sent, the
 * log will report the `device_count` property was not found.
 *
 * C2D Messaging:
 * To send a C2D message, select your device's Message to Device tab in the Azure Portal for your
 * IoT Hub. Under Properties, enter the SDK-defined content type system property name `$.ct` for
 * Key, and the application-defined value `application/cbor` for Value. This value must be agreed
 * upon between the device and service side applications to use the content type system property for
 * C2D messaging. Enter a message in the Message Body and select Send Message. The Key and Value
 * will appear as a URL-encoded key-value pair appended to the topic: `%24.ct=application%2Fcbor`.
 *
 * NOTE: The Azure Portal only recognizes printable character input and will NOT translate a JSON
 * formatted message into CBOR. Therefore, this sample only demonstrates how to parse the topic for
 * the content type system property. It is up to the service application to encode correctly
 * formatted CBOR (or other specified content type) and the device application to correctly decode
 * it.
 *
 * Telemetry:
 * The sample will automatically send CBOR formatted messages after each attempt to receive a C2D or
 * desired property message. The SDK-defined content type system property name `$.ct` and the
 * application-defined value `application/cbor` will appear as a URL-encoded key-value pair appended
 * to the topic: `%24.ct=application%2Fcbor`. This value must be agreed upon between the device and
 * service side applications to use the content type system property for Telemetry messaging.
 */
int main(void)
{
  create_and_configure_mqtt_client();
  IOT_SAMPLE_LOG_SUCCESS("Client created and configured.");

  connect_mqtt_client_to_iot_hub();
  IOT_SAMPLE_LOG_SUCCESS("Client connected to IoT Hub.");

  subscribe_mqtt_client_to_iot_hub_topics();
  IOT_SAMPLE_LOG_SUCCESS("Client subscribed to IoT Hub topics.");

  send_and_receive_messages();

  disconnect_mqtt_client_from_iot_hub();
  IOT_SAMPLE_LOG_SUCCESS("Client disconnected from IoT Hub.");

  return 0;
}

static void create_and_configure_mqtt_client(void)
{
  int rc;

  // Reads in environment variables set by user for purposes of running sample.
  iot_sample_read_environment_variables(SAMPLE_TYPE, SAMPLE_NAME, &env_vars);

  // Build an MQTT endpoint c-string.
  char mqtt_endpoint_buffer[STANDARD_BUFFER_SIZE];
  iot_sample_create_mqtt_endpoint(
      SAMPLE_TYPE, &env_vars, mqtt_endpoint_buffer, sizeof(mqtt_endpoint_buffer));

  // Initialize the hub client with the default connection options.
  az_iot_hub_client_options options = az_iot_hub_client_options_default();

  // Set the twin document content type to CBOR.
  options.twin_content_type = AZ_IOT_HUB_CLIENT_OPTION_TWIN_CONTENT_TYPE_CBOR;

  rc = az_iot_hub_client_init(&hub_client, env_vars.hub_hostname, env_vars.hub_device_id, &options);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to initialize hub client: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  // Get the MQTT client id used for the MQTT connection.
  char mqtt_client_id_buffer[STANDARD_BUFFER_SIZE];
  rc = az_iot_hub_client_get_client_id(
      &hub_client, mqtt_client_id_buffer, sizeof(mqtt_client_id_buffer), NULL);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to get MQTT client id: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  // Create the Paho MQTT client.
  rc = MQTTClient_create(
      &mqtt_client, mqtt_endpoint_buffer, mqtt_client_id_buffer, MQTTCLIENT_PERSISTENCE_NONE, NULL);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to create MQTT client: MQTTClient return code %d.", rc);
    exit(rc);
  }
}

static void connect_mqtt_client_to_iot_hub(void)
{
  int rc;

  // Get the MQTT client username.
  rc = az_iot_hub_client_get_user_name(
      &hub_client, mqtt_client_username_buffer, sizeof(mqtt_client_username_buffer), NULL);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to get MQTT client username: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  IOT_SAMPLE_LOG(" "); // Formatting
  IOT_SAMPLE_LOG("MQTT client username: %s\n", mqtt_client_username_buffer);

  // Set MQTT connection options.
  MQTTClient_connectOptions mqtt_connect_options = MQTTClient_connectOptions_initializer;
  mqtt_connect_options.username = mqtt_client_username_buffer;
  mqtt_connect_options.password = NULL; // This sample uses x509 authentication.
  mqtt_connect_options.cleansession = false; // Set to false so can receive any pending messages.
  mqtt_connect_options.keepAliveInterval = AZ_IOT_DEFAULT_MQTT_CONNECT_KEEPALIVE_SECONDS;

  MQTTClient_SSLOptions mqtt_ssl_options = MQTTClient_SSLOptions_initializer;
  mqtt_ssl_options.verify = 1;
  mqtt_ssl_options.enableServerCertAuth = 1;
  mqtt_ssl_options.keyStore = (char*)az_span_ptr(env_vars.x509_cert_pem_file_path);
  if (az_span_size(env_vars.x509_trust_pem_file_path) != 0) // Is only set if required by OS.
  {
    mqtt_ssl_options.trustStore = (char*)az_span_ptr(env_vars.x509_trust_pem_file_path);
  }
  mqtt_connect_options.ssl = &mqtt_ssl_options;

  // Connect MQTT client to the Azure IoT Hub.
  rc = MQTTClient_connect(mqtt_client, &mqtt_connect_options);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to connect: MQTTClient return code %d.\n"
        "If on Windows, confirm the AZ_IOT_DEVICE_X509_TRUST_PEM_FILE_PATH environment variable is "
        "set correctly.",
        rc);
    exit(rc);
  }
}

static void subscribe_mqtt_client_to_iot_hub_topics(void)
{
  int rc;

  // Messages received on the twin PATCH topic will be updates to the desired properties.
  rc = MQTTClient_subscribe(mqtt_client, AZ_IOT_HUB_CLIENT_TWIN_PATCH_SUBSCRIBE_TOPIC, 1);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to subscribe to the Twin Patch topic: MQTTClient return code %d.", rc);
    exit(rc);
  }

  // Messages received on the twin response topic will be response statuses from the server.
  rc = MQTTClient_subscribe(mqtt_client, AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_SUBSCRIBE_TOPIC, 1);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to subscribe to the Twin Response topic: MQTTClient return code %d.", rc);
    exit(rc);
  }

  // Messages received on the C2D topic will be cloud-to-device messages.
  rc = MQTTClient_subscribe(mqtt_client, AZ_IOT_HUB_CLIENT_C2D_SUBSCRIBE_TOPIC, 1);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to subscribe to the C2D topic: MQTTClient return code %d.", rc);
    exit(rc);
  }
}

static void send_and_receive_messages(void)
{
  // Get the latest twin document from the IoT Hub.
  request_twin_document();
  receive_message();

  // Update the IoT Hub with device's current properties.
  send_reported_property();
  receive_message();

  send_telemetry();

  // Wait for desired property PATCH messages or C2D messages. Send Telemetry.
  for (uint8_t message_count = 0; message_count < MAX_MESSAGE_COUNT; message_count++)
  {
    receive_message();
    send_telemetry();
  }
}

static void disconnect_mqtt_client_from_iot_hub(void)
{
  int rc = MQTTClient_disconnect(mqtt_client, MQTT_TIMEOUT_DISCONNECT_MS);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to disconnect MQTT client: MQTTClient return code %d.", rc);
    exit(rc);
  }

  MQTTClient_destroy(&mqtt_client);
}

static void request_twin_document(void)
{
  int rc;

  IOT_SAMPLE_LOG(" "); // Formatting
  IOT_SAMPLE_LOG("Client requesting twin document from service.");

  // Generate the unique rid for request.
  uint8_t twin_get_rid_buffer[RID_BUFFER_SIZE];
  az_span twin_get_rid_span
      = az_span_create(twin_get_rid_buffer, (int32_t)sizeof(twin_get_rid_buffer));
  generate_rid_span(twin_get_rid_base, twin_get_rid_num, &twin_get_rid_span);
  ++twin_get_rid_num; // Increment to keep uniqueness.

  // Get the twin document GET topic to publish the twin document request.
  char twin_get_topic_buffer[STANDARD_BUFFER_SIZE];
  size_t twin_get_topic_length;
  rc = az_iot_hub_client_twin_document_get_publish_topic(
      &hub_client,
      twin_get_rid_span,
      twin_get_topic_buffer,
      sizeof(twin_get_topic_buffer),
      &twin_get_topic_length);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to get the twin document GET topic: az_result return code 0x%08x.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG("Topic: %.*s", (int)twin_get_topic_length, twin_get_topic_buffer);

  // Publish the twin document GET request.
  rc = MQTTClient_publish(
      mqtt_client, twin_get_topic_buffer, 0, NULL, IOT_SAMPLE_MQTT_PUBLISH_QOS, 0, NULL);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to publish the twin document GET request: MQTTClient return code %d.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG_SUCCESS("Client published the twin document GET request.");
}

static void send_reported_property(void)
{
  int rc;

  IOT_SAMPLE_LOG(" "); // Formatting
  IOT_SAMPLE_LOG("Client sending reported property to service.");

  // Generate the unique rid for request.
  uint8_t reported_property_rid_buffer[RID_BUFFER_SIZE];
  az_span reported_property_rid_span
      = az_span_create(reported_property_rid_buffer, (int32_t)sizeof(reported_property_rid_buffer));
  generate_rid_span(
      reported_property_rid_base, reported_property_rid_num, &reported_property_rid_span);
  ++reported_property_rid_num; // Increment to keep uniqueness.

  // Get the twin reported property PATCH topic to publish a reported property message.
  char reported_property_patch_topic_buffer[STANDARD_BUFFER_SIZE];
  size_t reported_property_patch_topic_length;
  rc = az_iot_hub_client_twin_patch_get_publish_topic(
      &hub_client,
      reported_property_rid_span,
      reported_property_patch_topic_buffer,
      sizeof(reported_property_patch_topic_buffer),
      &reported_property_patch_topic_length);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to get the twin reported property PATCH topic: az_result return code 0x%08x.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG(
      "Topic: %.*s",
      (int)reported_property_patch_topic_length,
      reported_property_patch_topic_buffer);

  // Build the reported property message in CBOR.
  uint8_t reported_property_payload_buffer[STANDARD_BUFFER_SIZE];
  size_t reported_property_payload_length;
  build_cbor_reported_property(
      reported_property_payload_buffer,
      sizeof(reported_property_payload_buffer),
      &reported_property_payload_length);
  IOT_SAMPLE_LOG_HEX(
      "Payload:", (int)reported_property_payload_length, reported_property_payload_buffer);

  // Publish the reported property PATCH message.
  rc = MQTTClient_publish(
      mqtt_client,
      reported_property_patch_topic_buffer,
      (int)reported_property_payload_length,
      reported_property_payload_buffer,
      IOT_SAMPLE_MQTT_PUBLISH_QOS,
      0,
      NULL);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to publish the twin reported property PATCH message: MQTTClient return code %d.",
        rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG_SUCCESS("Client published the twin reported property PATCH message.");
}

static void send_telemetry(void)
{
  int rc;

  IOT_SAMPLE_LOG(" "); // Formatting
  IOT_SAMPLE_LOG("Client sending telemetry to service.");

  // Set the content type system property value for telemetry messages. This value is
  // application-defined and must reflect what the service application expects.
  az_span telemetry_properties_span
      = AZ_SPAN_FROM_STR(AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE "=" CONTENT_TYPE_TELEMETRY);
  az_iot_message_properties telemetry_message_properties;
  rc = az_iot_message_properties_init(
      &telemetry_message_properties,
      telemetry_properties_span,
      az_span_size(telemetry_properties_span));
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to set message properties content type for telemetry: az_result return code "
        "0x%08x.",
        rc);
    exit(rc);
  }

  // Get the telemetry topic to publish the telemetry message.
  char telemetry_topic_buffer[STANDARD_BUFFER_SIZE];
  size_t telemetry_topic_length;
  rc = az_iot_hub_client_telemetry_get_publish_topic(
      &hub_client,
      &telemetry_message_properties,
      telemetry_topic_buffer,
      sizeof(telemetry_topic_buffer),
      &telemetry_topic_length);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to get the telemetry topic: az_result return code 0x%08x.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG("Topic: %.*s", (int)telemetry_topic_length, telemetry_topic_buffer);

  // Build the telemetry message in CBOR.
  uint8_t telemetry_payload_buffer[STANDARD_BUFFER_SIZE];
  size_t telemetry_payload_length;
  build_cbor_telemetry(
      telemetry_payload_buffer, sizeof(telemetry_payload_buffer), &telemetry_payload_length);
  IOT_SAMPLE_LOG_HEX("Payload:", (int)telemetry_payload_length, telemetry_payload_buffer);

  // Publish the telemetry message.
  rc = MQTTClient_publish(
      mqtt_client,
      telemetry_topic_buffer,
      (int)telemetry_payload_length,
      telemetry_payload_buffer,
      IOT_SAMPLE_MQTT_PUBLISH_QOS,
      0,
      NULL);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to publish the telemetry message: MQTTClient return code %d.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG_SUCCESS("Client published the telemetry message.");
}

static void receive_message(void)
{
  char* topic = NULL;
  int topic_len = 0;
  MQTTClient_message* message = NULL;

  // MQTTCLIENT_SUCCESS or MQTTCLIENT_TOPICNAME_TRUNCATED if a message is received.
  // MQTTCLIENT_SUCCESS can also indicate that the timeout expired, in which case message is NULL.
  // MQTTCLIENT_TOPICNAME_TRUNCATED if the topic contains embedded NULL characters.
  // An error code is returned if there was a problem trying to receive a message.
  int rc = MQTTClient_receive(mqtt_client, &topic, &topic_len, &message, MQTT_TIMEOUT_RECEIVE_MS);
  if ((rc != MQTTCLIENT_SUCCESS) && (rc != MQTTCLIENT_TOPICNAME_TRUNCATED))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to receive message: MQTTClient return code %d.", rc);
    exit(rc);
  }
  else if (message == NULL)
  {
    IOT_SAMPLE_LOG(" "); // Formatting
    IOT_SAMPLE_LOG("Receive message timeout expired.");
  }
  else
  {
    IOT_SAMPLE_LOG(" "); // Formatting
    IOT_SAMPLE_LOG_SUCCESS("Client received a message from the service.");

    if (rc == MQTTCLIENT_TOPICNAME_TRUNCATED)
    {
      topic_len = (int)strlen(topic); // Don't include any part of topic after first embedded NULL.
    }
    handle_message(topic, topic_len, message);

    MQTTClient_freeMessage(&message);
  }
  MQTTClient_free(topic);
}

static void handle_message(char* topic, int topic_len, MQTTClient_message const* message)
{
  az_result rc;
  az_iot_hub_client_c2d_request c2d_request;
  az_iot_hub_client_twin_response twin_response;

  az_span const topic_span = az_span_create((uint8_t*)topic, topic_len);
  az_span const message_span = az_span_create((uint8_t*)message->payload, message->payloadlen);

  rc = az_iot_hub_client_c2d_parse_received_topic(&hub_client, topic_span, &c2d_request);
  if (az_result_succeeded(rc))
  {
    IOT_SAMPLE_LOG_SUCCESS("Client received a valid C2D message topic.");
    IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);

    handle_c2d_message(message_span, &c2d_request);
  }
  else
  {
    rc = az_iot_hub_client_twin_parse_received_topic(&hub_client, topic_span, &twin_response);
    if (az_result_succeeded(rc))
    {
      IOT_SAMPLE_LOG_SUCCESS("Client received a valid twin response topic or twin message topic.");
      IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);

      handle_device_twin_message(message_span, &twin_response);
    }
    else
    {
      IOT_SAMPLE_LOG_ERROR("Message from unknown topic: az_result return code 0x%08x.", rc);
      IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);
      exit(rc);
    }
  }
}

static void handle_c2d_message(az_span message_span, az_iot_hub_client_c2d_request* c2d_request)
{
  (void)message_span;
  az_span content_type_span;

  az_result rc = az_iot_message_properties_find(
      &c2d_request->properties,
      AZ_SPAN_FROM_STR(AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE),
      &content_type_span);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR(
        "Sytem property name `%s` could not be found in topic.",
        AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE);
  }
  else
  {
    // The content type system property value is application-defined and must reflect how the
    // service application has chosen to define it.
    if (az_span_is_content_equal(content_type_span, AZ_SPAN_FROM_STR(CONTENT_TYPE_C2D)))
    {
      IOT_SAMPLE_LOG_SUCCESS(
          "Client received expected system property content type value: %s.", CONTENT_TYPE_C2D);
      // The application should parse the message_span as the expected content type.
    }
    else
    {
      IOT_SAMPLE_LOG_ERROR(
          "Client did not receive expected system property content type value: %s.",
          CONTENT_TYPE_C2D);
    }
  }
}

static void handle_device_twin_message(
    az_span message_span,
    az_iot_hub_client_twin_response* twin_response)
{
  int64_t desired_property_device_count;

  // Invoke appropriate action per response type (3 types only).
  switch (twin_response->response_type)
  {
    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_GET:
      IOT_SAMPLE_LOG("Message Type: GET response");
      IOT_SAMPLE_LOG("Status: %d", twin_response->status);
      IOT_SAMPLE_LOG_HEX("Payload:", az_span_size(message_span), az_span_ptr(message_span));

      if (twin_response->status == AZ_IOT_STATUS_OK)
      {
        // Parse for the `device_count` property and if parsed, update locally.
        if (parse_cbor_desired_property(message_span, &desired_property_device_count))
        {
          int64_t temp_value = twin_property_device_count_value;
          twin_property_device_count_value = desired_property_device_count;
          IOT_SAMPLE_LOG_SUCCESS(
              "Client twin updated `%s` locally from %ld to %ld.",
              twin_property_device_count_name,
              temp_value,
              twin_property_device_count_value);
        }
      }
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_REPORTED_PROPERTIES:
      IOT_SAMPLE_LOG("Message Type: Reported Properties PATCH response");
      IOT_SAMPLE_LOG("Status: %d", twin_response->status);
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_DESIRED_PROPERTIES:
      // This is actually a PATCH request from the service. We do not send a response.
      IOT_SAMPLE_LOG("Message Type: Desired Properties PATCH request");
      IOT_SAMPLE_LOG_HEX("Payload:", az_span_size(message_span), az_span_ptr(message_span));

      // Parse for the `device_count` property.
      if (parse_cbor_desired_property(message_span, &desired_property_device_count))
      {
        // If the `device_count` property differs from device, update locally and send a reported
        // property message to the IoT Hub.
        if (update_property_device_count(desired_property_device_count))
        {
          send_reported_property();
          receive_message();
        }
      }
      break;
  }
}

static bool parse_cbor_desired_property(az_span message_span, int64_t* out_parsed_device_count)
{
  *out_parsed_device_count = 0;

  CborError rc; // CborNoError == 0
  bool result;

  CborParser parser;
  CborValue root;
  CborValue desired_root;
  CborValue device_count;

  rc = cbor_parser_init(
      az_span_ptr(message_span), (size_t)az_span_size(message_span), 0, &parser, &root);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to initiate parser: CborError %d.", rc);
    exit(rc);
  }

  // If full twin document received, update root to point to "desired" section.
  rc = cbor_value_map_find_value(&root, twin_desired_name, &desired_root);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Error when searching for %s: CborError %d.", twin_desired_name, rc);
    exit(rc);
  }
  if (cbor_value_is_valid(&desired_root))
  {
    root = desired_root;
  }

  rc = cbor_value_map_find_value(&root, twin_property_device_count_name, &device_count);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Error when searching for %s: CborError %d.", twin_property_device_count_name, rc);
    exit(rc);
  }

  if (cbor_value_is_valid(&device_count))
  {
    if (cbor_value_is_integer(&device_count))
    {
      rc = cbor_value_get_int64(&device_count, out_parsed_device_count);
      if (rc)
      {
        IOT_SAMPLE_LOG_ERROR(
            "Failed to get int64 value of property %s: CborError %d.",
            twin_property_device_count_name,
            rc);
        exit(rc);
      }
      else
      {
        IOT_SAMPLE_LOG(
            "Parsed desired property `%s`: %ld",
            twin_property_device_count_name,
            *out_parsed_device_count);
        result = true;
      }
    }
    else
    {
      IOT_SAMPLE_LOG("`%s` property value was not an integer.", twin_property_device_count_name);
      result = false;
    }
  }
  else
  {
    IOT_SAMPLE_LOG(
        "`%s` property name was not found in desired property message.",
        twin_property_device_count_name);
    result = false;
  }

  return result;
}

static bool update_property_device_count(int64_t new_device_count)
{
  bool result;

  if (twin_property_device_count_value != new_device_count)
  {
    int64_t temp_value = twin_property_device_count_value;
    twin_property_device_count_value = new_device_count;
    IOT_SAMPLE_LOG_SUCCESS(
        "Client twin updated `%s` locally from %ld to %ld.",
        twin_property_device_count_name,
        temp_value,
        twin_property_device_count_value);
    result = true;
  }
  else
  {
    IOT_SAMPLE_LOG_SUCCESS(
        "Client twin update not required. `%s` locally remains %ld.",
        twin_property_device_count_name,
        twin_property_device_count_value);
    result = false;
  }

  return result;
}

static void build_cbor_reported_property(
    uint8_t* reported_property_payload,
    size_t reported_property_payload_size,
    size_t* out_reported_property_payload_length)
{
  CborError rc; // CborNoError == 0

  CborEncoder encoder;
  CborEncoder encoder_map;

  cbor_encoder_init(&encoder, reported_property_payload, reported_property_payload_size, 0);

  rc = cbor_encoder_create_map(&encoder, &encoder_map, 1);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to create map: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encode_text_string(
      &encoder_map, twin_property_device_count_name, strlen(twin_property_device_count_name));
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to encode text string '%s': CborError %d.", twin_property_device_count_name, rc);
    exit(rc);
  }

  rc = cbor_encode_int(&encoder_map, twin_property_device_count_value);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to encode int '%ld': CborError %d.", twin_property_device_count_value, rc);
    exit(rc);
  }

  rc = cbor_encoder_close_container(&encoder, &encoder_map);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to close container: CborError %d.", rc);
    exit(rc);
  }

  *out_reported_property_payload_length
      = cbor_encoder_get_buffer_size(&encoder, reported_property_payload);
}

static void build_cbor_telemetry(
    uint8_t* telemetry_payload,
    size_t telemetry_payload_size,
    size_t* out_telemetry_payload_length)
{
  CborError rc; // CborNoError == 0

  CborEncoder encoder;
  CborEncoder encoder_map;

  cbor_encoder_init(&encoder, telemetry_payload, telemetry_payload_size, 0);

  rc = cbor_encoder_create_map(&encoder, &encoder_map, 1);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to create map: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encode_text_string(
      &encoder_map,
      telemetry_property_message_iteration_name,
      strlen(telemetry_property_message_iteration_name));
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to encode text string '%s': CborError %d.",
        telemetry_property_message_iteration_name,
        rc);
    exit(rc);
  }

  rc = cbor_encode_int(&encoder_map, telemetry_property_message_iteration_value);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to encode int '%ld': CborError %d.",
        telemetry_property_message_iteration_value,
        rc);
    exit(rc);
  }

  rc = cbor_encoder_close_container(&encoder, &encoder_map);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to close container: CborError %d.", rc);
    exit(rc);
  }

  *out_telemetry_payload_length = cbor_encoder_get_buffer_size(&encoder, telemetry_payload);

  ++telemetry_property_message_iteration_value; // Increase for next telemetry message.
}

static void generate_rid_span(az_span base_span, uint64_t unique_id, az_span* out_rid_span)
{
  az_result rc;
  az_span remainder;

  if (az_span_size(*out_rid_span) < az_span_size(base_span) || az_span_size(base_span) < 0)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to generate_rid_span: az_result return code 0x%08x.", AZ_ERROR_NOT_ENOUGH_SPACE);
    exit(AZ_ERROR_NOT_ENOUGH_SPACE);
  }

  remainder = az_span_copy(*out_rid_span, base_span);

  rc = az_span_u64toa(remainder, unique_id, &remainder); // Performs size check internally.
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to convert uint64_t to ASCII: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  *out_rid_span = az_span_create(
      az_span_ptr(*out_rid_span), az_span_size(*out_rid_span) - az_span_size(remainder));
}
