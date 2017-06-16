// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef IOTHUBTRANSPORT_AMQP_MESSENGER
#define IOTHUBTRANSPORT_AMQP_MESSENGER

#include "azure_c_shared_utility/umock_c_prod.h"
#include "azure_c_shared_utility/optionhandler.h"
#include "azure_c_shared_utility/map.h"
#include "azure_uamqp_c/session.h"

#ifdef __cplusplus
extern "C"
{
#endif


static const char* MESSENGER_OPTION_EVENT_SEND_TIMEOUT_SECS = "amqp_event_send_timeout_secs";

typedef struct AMQP_MESSENGER_INSTANCE* AMQP_MESSENGER_HANDLE;

#define AMQP_MESSENGER_SEND_STATUS_STRINGS \
	AMQP_MESSENGER_SEND_STATUS_IDLE, \
	AMQP_MESSENGER_SEND_STATUS_BUSY

DEFINE_ENUM(AMQP_MESSENGER_SEND_STATUS, AMQP_MESSENGER_SEND_STATUS_STRINGS);

#define AMQP_MESSENGER_SEND_RESULT_STRINGS \
	AMQP_MESSENGER_SEND_RESULT_OK, \
	AMQP_MESSENGER_SEND_RESULT_ERROR_CANNOT_PARSE, \
	AMQP_MESSENGER_SEND_RESULT_ERROR_FAIL_SENDING, \
	AMQP_MESSENGER_SEND_RESULT_ERROR_TIMEOUT, \
	AMQP_MESSENGER_SEND_RESULT_MESSENGER_DESTROYED

DEFINE_ENUM(AMQP_MESSENGER_SEND_RESULT, AMQP_MESSENGER_SEND_RESULT_STRINGS)

#define AMQP_MESSENGER_DISPOSITION_RESULT_STRINGS \
	AMQP_MESSENGER_DISPOSITION_RESULT_NONE, \
	AMQP_MESSENGER_DISPOSITION_RESULT_ACCEPTED, \
	AMQP_MESSENGER_DISPOSITION_RESULT_REJECTED, \
	AMQP_MESSENGER_DISPOSITION_RESULT_RELEASED 

DEFINE_ENUM(AMQP_MESSENGER_DISPOSITION_RESULT, AMQP_MESSENGER_DISPOSITION_RESULT_STRINGS);

#define AMQP_MESSENGER_STATE_STRINGS \
	AMQP_MESSENGER_STATE_STARTING, \
	AMQP_MESSENGER_STATE_STARTED, \
	AMQP_MESSENGER_STATE_STOPPING, \
	AMQP_MESSENGER_STATE_STOPPED, \
	AMQP_MESSENGER_STATE_ERROR

DEFINE_ENUM(AMQP_MESSENGER_STATE, AMQP_MESSENGER_STATE_STRINGS);

typedef struct AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO_TAG
{
	delivery_number message_id;
	char* source;
} AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO;

typedef void(*AMQP_MESSENGER_SEND_COMPLETE_CALLBACK)(AMQP_MESSENGER_SEND_RESULT result, void* context);
typedef void(*AMQP_MESSENGER_STATE_CHANGED_CALLBACK)(void* context, AMQP_MESSENGER_STATE previous_state, AMQP_MESSENGER_STATE new_state);
typedef AMQP_MESSENGER_DISPOSITION_RESULT(*ON_AMQP_MESSENGER_MESSAGE_RECEIVED)(MESSAGE_HANDLE message, AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* disposition_info, void* context);

typedef struct AMQP_MESSENGER_CONFIG_TAG
{
	const char* device_id;
	char* iothub_host_fqdn;

	/**
	* @brief Sample format: "%s/devices/%s"
	*/
	const char* devices_path_format;
	/**
	* @brief Sample format: "/messages/events"
	*/
	const char* send_link_target_suffix;
	/**
	* @brief Sample format: "/messages/devicebound"
	*/
	const char* receive_link_source_suffix;

	/**
	* @brief Properties to be set on the send link upon attachment. It can be NULL.
	*/
	MAP_HANDLE send_link_attach_properties;

	/**
	* @brief Properties to be set on the receive link upon attachment. It can be NULL.
	*/
	MAP_HANDLE receive_link_attach_properties;

	AMQP_MESSENGER_STATE_CHANGED_CALLBACK on_state_changed_callback;
	void* on_state_changed_context;
} AMQP_MESSENGER_CONFIG;

MOCKABLE_FUNCTION(, AMQP_MESSENGER_HANDLE, amqp_messenger_create, const AMQP_MESSENGER_CONFIG*, messenger_config);
MOCKABLE_FUNCTION(, int, amqp_messenger_send_async, AMQP_MESSENGER_HANDLE, messenger_handle, MESSAGE_HANDLE, message, AMQP_MESSENGER_SEND_COMPLETE_CALLBACK, on_messenger_event_send_complete_callback, void*, context);
MOCKABLE_FUNCTION(, int, amqp_messenger_subscribe_for_messages, AMQP_MESSENGER_HANDLE, messenger_handle, ON_AMQP_MESSENGER_MESSAGE_RECEIVED, on_message_received_callback, void*, context);
MOCKABLE_FUNCTION(, int, amqp_messenger_unsubscribe_for_messages, AMQP_MESSENGER_HANDLE, messenger_handle);
MOCKABLE_FUNCTION(, int, amqp_messenger_send_message_disposition, AMQP_MESSENGER_HANDLE, messenger_handle, AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO*, disposition_info, AMQP_MESSENGER_DISPOSITION_RESULT, disposition_result);
MOCKABLE_FUNCTION(, int, amqp_messenger_get_send_status, AMQP_MESSENGER_HANDLE, messenger_handle, AMQP_MESSENGER_SEND_STATUS*, send_status);
MOCKABLE_FUNCTION(, int, amqp_messenger_start, AMQP_MESSENGER_HANDLE, messenger_handle, SESSION_HANDLE, session_handle);
MOCKABLE_FUNCTION(, int, amqp_messenger_stop, AMQP_MESSENGER_HANDLE, messenger_handle);
MOCKABLE_FUNCTION(, void, amqp_messenger_do_work, AMQP_MESSENGER_HANDLE, messenger_handle);
MOCKABLE_FUNCTION(, void, amqp_messenger_destroy, AMQP_MESSENGER_HANDLE, messenger_handle);
MOCKABLE_FUNCTION(, int, amqp_messenger_set_option, AMQP_MESSENGER_HANDLE, messenger_handle, const char*, name, void*, value);
MOCKABLE_FUNCTION(, OPTIONHANDLER_HANDLE, amqp_messenger_retrieve_options, AMQP_MESSENGER_HANDLE, messenger_handle);
MOCKABLE_FUNCTION(, void, amqp_messenger_destroy_disposition_info, AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO*, disposition_info);


#ifdef __cplusplus
}
#endif

#endif /*IOTHUBTRANSPORT_AMQP_AMQP_MESSENGER*/
