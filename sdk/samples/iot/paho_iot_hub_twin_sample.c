// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

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

#define SAMPLE_TYPE PAHO_IOT_HUB
#define SAMPLE_NAME PAHO_IOT_HUB_TWIN_SAMPLE

#define RID_BUFFER_SIZE 64
#define STANDARD_BUFFER_SIZE 128

#define MAX_TWIN_MESSAGE_COUNT 10
#define MQTT_TIMEOUT_RECEIVE_MS (60 * 1000)
#define MQTT_TIMEOUT_DISCONNECT_MS (10 * 1000)

static az_span const twin_get_rid_base = AZ_SPAN_LITERAL_FROM_STR("tg-");
static uint64_t twin_get_rid_num = 0;
static az_span const reported_property_rid_base = AZ_SPAN_LITERAL_FROM_STR("rp-");
static uint64_t reported_property_rid_num = 0;

static az_span const twin_version_name = AZ_SPAN_LITERAL_FROM_STR("$version");
static az_span const twin_desired_name = AZ_SPAN_LITERAL_FROM_STR("desired");
static az_span const twin_property_device_count_name = AZ_SPAN_LITERAL_FROM_STR("device_count");
static int32_t twin_property_device_count_value = 0;

static iot_sample_environment_variables env_vars;
static az_iot_hub_client hub_client;
static MQTTClient mqtt_client;
static char mqtt_client_username_buffer[STANDARD_BUFFER_SIZE];

// Functions
static void create_and_configure_mqtt_client(void);
static void connect_mqtt_client_to_iot_hub(void);
static void subscribe_mqtt_client_to_iot_hub_topics(void);
static void send_and_receive_device_twin_messages(void);
static void disconnect_mqtt_client_from_iot_hub(void);

static void request_twin_document(void);
static void send_reported_property(void);
static void receive_device_twin_message(void);
static void handle_device_twin_message(char* topic, int topic_len, MQTTClient_message const* message);
static bool parse_desired_property(
    az_span message_span,
    int32_t* out_parsed_device_count);
static void update_property_device_count(int32_t new_device_count);
static void build_reported_property(
    az_span reported_property_payload,
    az_span* out_reported_property_payload);
static void generate_rid_span(az_span base_span, uint64_t unique_id, az_span* out_rid_span);

/*
 * This sample utilizes the Azure IoT Hub to get the device twin document, send a reported property
 * message, and receive up to 5 desired property messages. If a timeout occurs while waiting for a
 * message from the Azure IoT Hub, the sample will exit. Upon receiving a desired property message,
 * the sample will update the twin property locally and send a reported property message back to the
 * service. X509 self-certification is used.
 *
 * A property named `device_count` is supported for this sample. To send a device twin desired
 * property message, select your device's Device Twin tab in the Azure Portal of your IoT Hub. Add
 * the property `device_count` along with a corresponding value to the `desired` section of the
 * JSON. Select Save to update the twin document and send the twin message to the device.
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
 * IOT_SAMPLE_LOG will report there is nothing to update.
 */
int main(void)
{
  create_and_configure_mqtt_client();
  IOT_SAMPLE_LOG_SUCCESS("Client created and configured.");

  connect_mqtt_client_to_iot_hub();
  IOT_SAMPLE_LOG_SUCCESS("Client connected to IoT Hub.");

  subscribe_mqtt_client_to_iot_hub_topics();
  IOT_SAMPLE_LOG_SUCCESS("Client subscribed to IoT Hub topics.");

  send_and_receive_device_twin_messages();

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
  rc = az_iot_hub_client_init(&hub_client, env_vars.hub_hostname, env_vars.hub_device_id, NULL);
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

  // Messages received on the Twin Patch topic will be updates to the desired properties.
  rc = MQTTClient_subscribe(mqtt_client, AZ_IOT_HUB_CLIENT_TWIN_PATCH_SUBSCRIBE_TOPIC, 1);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to subscribe to the Twin Patch topic: MQTTClient return code %d.", rc);
    exit(rc);
  }

  // Messages received on Twin Response topic will be response statuses from the server.
  rc = MQTTClient_subscribe(mqtt_client, AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_SUBSCRIBE_TOPIC, 1);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    IOT_SAMPLE_LOG_ERROR(
        "Failed to subscribe to the Twin Response topic: MQTTClient return code %d.", rc);
    exit(rc);
  }
}

static void send_and_receive_device_twin_messages(void)
{
  // Get the latest twin document from the IoT Hub.
  request_twin_document();

  // Wait for a GET request response, a reported property PATCH response, or a desired property
  // PATCH message.
  for (uint8_t message_count = 0; message_count < MAX_TWIN_MESSAGE_COUNT; message_count++)
  {
    receive_device_twin_message();
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
  IOT_SAMPLE_LOG("Client requesting device twin document from service.");

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

  // Build the reported property message.
  char reported_property_payload_buffer[STANDARD_BUFFER_SIZE];
  az_span reported_property_payload = AZ_SPAN_FROM_BUFFER(reported_property_payload_buffer);
  build_reported_property(reported_property_payload, &reported_property_payload);
  IOT_SAMPLE_LOG_AZ_SPAN("Payload:", reported_property_payload);

  // Publish the reported property PATCH message.
  rc = MQTTClient_publish(
      mqtt_client,
      reported_property_patch_topic_buffer,
      az_span_size(reported_property_payload),
      az_span_ptr(reported_property_payload),
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

static void receive_device_twin_message(void)
{
  char* topic = NULL;
  int topic_len = 0;
  MQTTClient_message* message = NULL;

  IOT_SAMPLE_LOG(" "); // Formatting

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
    IOT_SAMPLE_LOG("Receive message timeout expired.");
  }
  else
  {
    IOT_SAMPLE_LOG_SUCCESS("Client received a device twin message from the service.");

    if (rc == MQTTCLIENT_TOPICNAME_TRUNCATED)
    {
      topic_len = (int)strlen(topic); // Don't include any part of topic after first embedded NULL.
    }
    handle_device_twin_message(topic, topic_len, message);

    MQTTClient_freeMessage(&message);
  }
  MQTTClient_free(topic);
}

static void handle_device_twin_message(char* topic, int topic_len, MQTTClient_message const* message)
{
  az_result rc;
  az_iot_hub_client_twin_response twin_response;
  int32_t desired_property_device_count;

  az_span const topic_span = az_span_create((uint8_t*)topic, topic_len);
  az_span const message_span = az_span_create((uint8_t*)message->payload, message->payloadlen);

  // Parse the topic.
  rc = az_iot_hub_client_twin_parse_received_topic(&hub_client, topic_span, &twin_response);
  if (az_result_failed(rc))
  {
    IOT_SAMPLE_LOG_ERROR("Message from unknown topic: az_result return code 0x%08x.", rc);
    IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);
    exit(rc);
  }
  IOT_SAMPLE_LOG_SUCCESS("Client received a valid topic response.");
  IOT_SAMPLE_LOG_AZ_SPAN("Topic:", topic_span);

  // Invoke appropriate action per response type (3 types only).
  switch (twin_response.response_type)
  {
    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_GET:
      IOT_SAMPLE_LOG("Message Type: GET response");
      IOT_SAMPLE_LOG("Status: %d", twin_response.status);
      IOT_SAMPLE_LOG_AZ_SPAN("Payload:", message_span);

      if (twin_response.status == AZ_IOT_STATUS_OK)
      {
        if (parse_desired_property(message_span, &desired_property_device_count))
        {
          update_property_device_count(desired_property_device_count);
        }
        send_reported_property();
      }
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_REPORTED_PROPERTIES:
      IOT_SAMPLE_LOG("Message Type: Reported Properties PATCH response");
      IOT_SAMPLE_LOG("Status: %d", twin_response.status);
      break;

    case AZ_IOT_HUB_CLIENT_TWIN_RESPONSE_TYPE_DESIRED_PROPERTIES:
      // This is acutally a request from the service, and we do not send back a response.
      IOT_SAMPLE_LOG("Message Type: Desired Properties PATCH request");
      IOT_SAMPLE_LOG_AZ_SPAN("Payload:", message_span);

      if (parse_desired_property(message_span, &desired_property_device_count))
      {
        update_property_device_count(desired_property_device_count);
        send_reported_property();
      }
      break;
  }
}

static bool parse_desired_property(az_span message_span, int32_t* out_parsed_device_count)
{
  char const* const log = "Failed to parse for desired `%.*s` property";
  az_span property = twin_property_device_count_name;

  bool property_found = false;
  *out_parsed_device_count = 0;

  // Parse message_span.
  az_json_reader jr;
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_init(&jr, message_span, NULL), log, property);
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
  if (jr.token.kind == AZ_JSON_TOKEN_BEGIN_OBJECT)
  {
    // If full twin document, move to desired section.
    IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
    if (az_json_token_is_text_equal(&jr.token, twin_desired_name))
    {
      IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
      if (jr.token.kind == AZ_JSON_TOKEN_BEGIN_OBJECT)
      {
        IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
      }
      else
      {
        IOT_SAMPLE_LOG_AZ_SPAN(log, property);
        return property_found;
      }
    }

    while (!property_found && (jr.token.kind != AZ_JSON_TOKEN_END_OBJECT))
    {
      if (az_json_token_is_text_equal(&jr.token, twin_property_device_count_name))
      {
        // Move to the value token.
        IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
        IOT_SAMPLE_EXIT_IF_AZ_FAILED(
            az_json_token_get_int32(&jr.token, out_parsed_device_count), log, property);
        property_found = true;
      }
      else if (az_json_token_is_text_equal(&jr.token, twin_version_name))
      {
        break;
      }
      else
      {
        IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_skip_children(&jr), log, property);
      }
      IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_reader_next_token(&jr), log, property);
    }
  }

  if (property_found)
  {
    IOT_SAMPLE_LOG(
        "Parsed desired `%.*s`: %d",
        az_span_size(property),
        az_span_ptr(property),
        *out_parsed_device_count);
  }
  else
  {
    IOT_SAMPLE_LOG(
        "`%.*s` property name was not found in desired property message.",
        az_span_size(property),
        az_span_ptr(property));
  }

  return property_found;
}

static void update_property_device_count(int32_t new_device_count)
{
  int32_t temp_value = twin_property_device_count_value;
  twin_property_device_count_value = new_device_count;
  IOT_SAMPLE_LOG_SUCCESS(
      "Client updated `%.*s` locally from %d to %d.",
      az_span_size(twin_property_device_count_name),
      az_span_ptr(twin_property_device_count_name),
      temp_value,
      twin_property_device_count_value);
}

static void build_reported_property(
    az_span reported_property_payload,
    az_span* out_reported_property_payload)
{
  char const* const log = "Failed to build reported property payload";

  az_json_writer jw;
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_writer_init(&jw, reported_property_payload, NULL), log);
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_writer_append_begin_object(&jw), log);
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(
      az_json_writer_append_property_name(&jw, twin_property_device_count_name), log);
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_writer_append_int32(&jw, twin_property_device_count_value), log);
  IOT_SAMPLE_EXIT_IF_AZ_FAILED(az_json_writer_append_end_object(&jw), log);

  *out_reported_property_payload = az_json_writer_get_bytes_used_in_destination(&jw);
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
