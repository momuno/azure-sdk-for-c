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
// warning within intel/tinycbor: conversion from 'int' to uint8_t'
#pragma GCC diagnostic ignored "-Wconversion"
#endif
#include "cbor.h"
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#define SAMPLE_TYPE PAHO_IOT_HUB
#define SAMPLE_NAME PAHO_IOT_HUB_CBOR_C2D_TELEMETRY_TWIN_SAMPLE

#define MAX_MESSAGE_COUNT 10
#define MQTT_TIMEOUT_RECEIVE_MS (60 * 1000)
#define MQTT_TIMEOUT_DISCONNECT_MS (10 * 1000)

#define CONTENT_TYPE_C2D "cbor" // application-defined
#define CONTENT_TYPE_TELEMETRY "cbor" // application-defined

static az_span const twin_get_rid_base = AZ_SPAN_LITERAL_FROM_STR("tg-");
static uint64_t twin_get_rid_num = 0;
static az_span const reported_property_rid_base = AZ_SPAN_LITERAL_FROM_STR("rp-");
static uint64_t reported_property_rid_num = 0;

static char* const property_device_count_name = "device_count";
static int64_t property_device_count_value = 0;
static char* const telemetry_message_name = "telemetry_message";
static int64_t telemetry_message_value = 1;

static iot_sample_environment_variables env_vars;
static az_iot_hub_client hub_client;
static MQTTClient mqtt_client;
static char mqtt_client_username_buffer[256];

// Functions
static void create_and_configure_mqtt_client(void);
static void connect_mqtt_client_to_iot_hub(void);
static void subscribe_mqtt_client_to_iot_hub_topics(void);
static void send_and_receive_messages(void);
static void disconnect_mqtt_client_from_iot_hub(void);

static void generate_rid_span(az_span base_span, uint64_t unique_id, az_span* out_rid_span);
static void request_twin_document(void);
static void send_reported_property(void);
static void send_telemetry(void);
static void receive_message(void);
static void handle_message(char* topic, int topic_len, MQTTClient_message const* message);
static void handle_c2d_message(az_span message_span, az_iot_hub_client_c2d_request* c2d_request);
static void handle_device_twin_message(az_span message_span, az_iot_hub_client_twin_response* twin_response);
static bool parse_desired_property(az_span message_span, int64_t* out_parsed_device_count);
static void update_property_device_count(int64_t device_count);
static void build_reported_property(
    uint8_t* reported_property_payload,
    size_t reported_property_payload_size,
    size_t* out_reported_property_payload_length);
static void build_telemetry(
    uint8_t* telemetry_payload,
    size_t telemetry_payload_size,
    size_t* out_telemetry_payload_length);


/*
 * CBOR for Device Twin:
 *
 * This sample utilizes the Azure IoT Hub to get the device twin document, send a reported
 * property message, and receive desired property messages all in CBOR. Upon receiving a
 * desired property message, the sample will update the twin property locally and send a reported
 * property message back to the IoT Hub.
 *
 * A property named `device_count` is used. No other property names sent in a desired property
 * message are supported. If any are sent, the IOT_SAMPLE_LOG will report the `device_count`
 * property was not found. To send a device twin desired property message, select your device's
 * Device twin tab in the Azure Portal of your IoT Hub. Add the property `device_count` along with a
 * corresponding value to the `desired` section of the twin JSON. Select Save to update the twin
 * document and send the twin message to the device. The IoT Hub will translate the twin JSON into
 * CBOR for the device to consume.
 *
 * {
 *   "properties": {
 *     "desired": {
 *       "device_count": 42,
 *     }
 *   }
 * }
 *
 * CBOR for C2D and Telemetry:
 *
 * This sample also shows how to set an application-defined content type (such as CBOR) for either
 * C2D or telemetry messaging, to be used with a coordinated service-side application. After
 * receiving a message (C2D or twin desired property) or upon a message timeout, the sample will
 * send a single telemetry message in CBOR. After 10 attempts to receive a message, the sample will
 * exit. IMPORTANT: Only device-side implementation is shown; the required corresponding
 * service-side implementation is not part of this sample.
 *
 * This sample demonstrates use of the intel/tinycbor library for encoding and decoding of CBOR. The
 * Embedded C SDK is not dependent on this CBOR library. X509 self-certification is used.
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
  char mqtt_endpoint_buffer[128];
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
  char mqtt_client_id_buffer[128];
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
}

static void send_and_receive_messages(void)
{
  request_twin_document();
  (void)receive_message();

  send_reported_property();
  (void)receive_message();

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

static void generate_rid_span(az_span base_span, uint64_t unique_id, az_span* out_rid_span)
{
  az_result rc;
  az_span remainder;

  if (az_span_size(*out_rid_span) < az_span_size(base_span) || az_span_size(base_span) < 0)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to generate_rid_span: az_result return code 0x%08x.", AZ_ERROR_NOT_ENOUGH_SPACE);
    exit(AZ_ERROR_NOT_ENOUGH_SPACE);
  }

  remainder = az_span_copy(*out_rid_span, base_span);

  rc = az_span_u64toa(remainder, unique_id, &remainder); // Performs size check iternally.
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to convert uint64_t to ASCII: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  *out_rid_span = az_span_create(az_span_ptr(*out_rid_span), az_span_size(*out_rid_span) - az_span_size(remainder));
}

static void request_twin_document(void)
{
  int rc;

  IOT_SAMPLE_LOG(" "); // Formatting
  IOT_SAMPLE_LOG("Client requesting twin document from service.");

  // Generate the unique rid for request.
  uint8_t twin_get_rid_buffer[64];
  az_span twin_get_rid_span = az_span_create(twin_get_rid_buffer, (int32_t)sizeof(twin_get_rid_buffer));
  generate_rid_span(twin_get_rid_base, twin_get_rid_num, &twin_get_rid_span);
  ++twin_get_rid_num; // Increment to keep uniqueness.

  // Get the twin document GET topic to publish the twin document request.
  char twin_get_topic_buffer[128];
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
  uint8_t reported_property_rid_buffer[64];
  az_span reported_property_rid_span = az_span_create(reported_property_rid_buffer, (int32_t)sizeof(reported_property_rid_buffer));
  generate_rid_span(reported_property_rid_base, reported_property_rid_num, &reported_property_rid_span);
  ++reported_property_rid_num; // Increment to keep uniqueness.

  // Get the twin reported property PATCH topic to publish a reported property message.
  char reported_property_patch_topic_buffer[128];
  size_t reported_property_patch_topic_length;
  rc = az_iot_hub_client_twin_patch_get_publish_topic(
      &hub_client,
      reported_property_rid_span,
      reported_property_patch_topic_buffer,
      sizeof(reported_property_patch_topic_buffer),
      &reported_property_patch_topic_length);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to get the twin reported property PATCH topic: az_result return code 0x%08x.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG("Topic: %.*s", (int)reported_property_patch_topic_length, reported_property_patch_topic_buffer);

  // Build the reported property message in CBOR.
  uint8_t reported_property_payload_buffer[128];
  size_t reported_property_payload_length;
  build_reported_property(
      reported_property_payload_buffer,
      sizeof(reported_property_payload_buffer),
      &reported_property_payload_length);
  IOT_SAMPLE_LOG_HEX("Payload:", (int)reported_property_payload_length, reported_property_payload_buffer);

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

  // Set the telemetry content type to CBOR.
  // The content type value is application-defined and should reflect what the service client
  // expects. In this sample, the content type value is arbitrary.
  az_span telemetry_properties_span = AZ_SPAN_FROM_STR(AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE "=" CONTENT_TYPE_TELEMETRY);
  az_iot_message_properties telemetry_message_properties;
  rc = az_iot_message_properties_init(&telemetry_message_properties, telemetry_properties_span, az_span_size(telemetry_properties_span));
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to set message properties content type for telemetry: az_result return code 0x%08x.", rc);
    exit(rc);
  }

  // Get the telemetry topic to publish the telemetry message.
  char telemetry_topic_buffer[128];
  size_t telemetry_topic_length;
  rc = az_iot_hub_client_telemetry_get_publish_topic(
      &hub_client, &telemetry_message_properties, telemetry_topic_buffer, sizeof(telemetry_topic_buffer), &telemetry_topic_length);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Failed to get the telemetry topic: az_result return code 0x%08x.", rc);
    exit(rc);
  }
  IOT_SAMPLE_LOG("Topic: %.*s", (int)telemetry_topic_length, telemetry_topic_buffer);

  // Build the telemetry message in CBOR.
  uint8_t telemetry_payload_buffer[64];
  size_t telemetry_payload_length;
  build_telemetry(telemetry_payload_buffer, sizeof(telemetry_payload_buffer), &telemetry_payload_length);
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
    IOT_SAMPLE_LOG_ERROR(
        "Failed to publish the telemetry message: MQTTClient return code %d.",
        rc);
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
      topic_len = (int)strlen(topic); //Don't include any part of topic after first embedded NULL.
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
    IOT_SAMPLE_LOG_SUCCESS("Client received a valid topic response.");
    IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);

    handle_c2d_message(message_span, &c2d_request);
  }
  else
  {
    rc = az_iot_hub_client_twin_parse_received_topic(&hub_client, topic_span, &twin_response);
    if (az_result_succeeded(rc))
    {
      IOT_SAMPLE_LOG_SUCCESS("Client received a valid topic response.");
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
  az_span content_type_span;

  az_result rc = az_iot_message_properties_find(&c2d_request->properties, AZ_SPAN_FROM_STR(AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE), &content_type_span);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Property name %s could not be found in topic: az_result_return code 0x%08x.", AZ_IOT_MESSAGE_PROPERTIES_CONTENT_TYPE, rc);
    exit(rc);
  }

  // Check if content type from service client matches application-defined expected value CONTENT_TYPE_C2D.
  char content_type_buffer[32];
  az_span_to_str(content_type_buffer, (int32_t)sizeof(content_type_buffer), content_type_span);
  if (strcmp(content_type_buffer, CONTENT_TYPE_C2D) == 0)
  {
    IOT_SAMPLE_LOG_SUCCESS("Client received expected CBOR content type: %s.", CONTENT_TYPE_C2D);
    IOT_SAMPLE_LOG_HEX("Payload:", az_span_size(message_span), az_span_ptr(message_span));
  }
  else
  {
    IOT_SAMPLE_LOG_ERROR("Client did not receive expected content type value: %s.", CONTENT_TYPE_C2D);
  }
}

static void handle_device_twin_message(az_span message_span, az_iot_hub_client_twin_response* twin_response)
{
  // Invoke appropriate action per response type (3 types only).
  switch (twin_response->response_type)
  {
    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_GET:
      IOT_SAMPLE_LOG("Message Type: GET response");
      IOT_SAMPLE_LOG("Status: %d", twin_response->status);
      IOT_SAMPLE_LOG_HEX("Payload:", az_span_size(message_span), az_span_ptr(message_span));
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_REPORTED_PROPERTIES:
      IOT_SAMPLE_LOG("Message Type: Reported Properties PATCH response");
      IOT_SAMPLE_LOG("Status: %d", twin_response->status);
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_DESIRED_PROPERTIES:
      // This is acutally a request from the service, and we do not send back a response.
      IOT_SAMPLE_LOG("Message Type: Desired Properties PATCH request");
      IOT_SAMPLE_LOG_HEX("Payload:", az_span_size(message_span), az_span_ptr(message_span));

      // Parse for the device count property.
      int64_t desired_property_device_count;
      if (parse_desired_property(message_span, &desired_property_device_count))
      {
        // Update device count locally and report update to server.
        update_property_device_count(desired_property_device_count);
        send_reported_property();
        (void)receive_message();
      }
      break;
  }
}

static bool parse_desired_property(az_span message_span, int64_t* out_parsed_device_count)
{
  *out_parsed_device_count = 0;

  CborError rc; // CborNoError == 0
  bool result;

  // Parse message_span.
  CborParser parser;
  CborValue root;
  CborValue device_count;

  rc = cbor_parser_init(az_span_ptr(message_span), (size_t)az_span_size(message_span), 0, &parser, &root);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to initiate parser: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_value_map_find_value(&root, property_device_count_name, &device_count);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Error when searching for %s: CborError %d.", property_device_count_name, rc);
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
            "Failed to get int64 value for %s: CborError %d.",
            property_device_count_name,
            rc);
        exit(rc);
      }
      else
      {
        IOT_SAMPLE_LOG(
            "Parsed desired `%s`: %" PRIi64,
            property_device_count_name,
            *out_parsed_device_count);
        result = true;
      }
    }
    else
    {
      IOT_SAMPLE_LOG("`%s` property was not an integer.", property_device_count_name);
      result = false;
    }
  }
  else
  {
    IOT_SAMPLE_LOG(
        "`%s` property was not found in desired property message.",
        property_device_count_name);
    result = false;
  }

  return result;
}

static void update_property_device_count(int64_t device_count)
{
  property_device_count_value = device_count;
  IOT_SAMPLE_LOG_SUCCESS(
      "Client updated `%s` locally to %." PRIi64,
      property_device_count_name,
      property_device_count_value);
}

static void build_reported_property(
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
      &encoder_map, property_device_count_name, strlen(property_device_count_name));
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to encode text string: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encode_int(&encoder_map, property_device_count_value);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to encode int: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encoder_close_container(&encoder, &encoder_map);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to close container: CborError %d.", rc);
    exit(rc);
  }

  *out_reported_property_payload_length = cbor_encoder_get_buffer_size(&encoder, reported_property_payload);
}


static void build_telemetry(uint8_t* telemetry_payload, size_t telemetry_payload_size, size_t* out_telemetry_payload_length)
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

  rc = cbor_encode_text_string(&encoder_map, telemetry_message_name, strlen(telemetry_message_name));
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to encode text string: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encode_int(&encoder_map, telemetry_message_value);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to encode int: CborError %d.", rc);
    exit(rc);
  }

  rc = cbor_encoder_close_container(&encoder, &encoder_map);
  if (rc)
  {
    IOT_SAMPLE_LOG_ERROR("Failed to close container: CborError %d.", rc);
    exit(rc);
  }

  *out_telemetry_payload_length = cbor_encoder_get_buffer_size(&encoder, telemetry_payload);

  ++telemetry_message_value; // Increase for next telemetry message.
}