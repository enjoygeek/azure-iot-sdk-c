// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include <stdbool.h>
#include "azure_c_shared_utility/optimize_size.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/agenttime.h" 
#include "azure_c_shared_utility/xlogging.h"
#include "azure_c_shared_utility/uniqueid.h"
#include "azure_uamqp_c/link.h"
#include "azure_uamqp_c/messaging.h"
#include "azure_uamqp_c/message_sender.h"
#include "azure_uamqp_c/message_receiver.h"
#include "message_queue.h"
#include "iothub_client_retry_control.h"
#include "iothubtransport_amqp_messenger.h"

#define RESULT_OK 0
#define INDEFINITE_TIME ((time_t)(-1))

#define LINK_BASE_ADDRESS                               "amqps://%s/%s"
#define MESSAGE_SENDER_LINK_NAME_PREFIX                 "link-snd"
#define MESSAGE_SENDER_MAX_LINK_SIZE                    UINT64_MAX
#define MESSAGE_RECEIVER_LINK_NAME_PREFIX               "link-rcv"
#define MESSAGE_RECEIVER_MAX_LINK_SIZE                  65536
#define DEFAULT_EVENT_SEND_RETRY_LIMIT                  0
#define DEFAULT_EVENT_SEND_TIMEOUT_SECS                 600
#define DEFAULT_MAX_SEND_ERROR_COUNT                    10
#define MAX_MESSAGE_SENDER_STATE_CHANGE_TIMEOUT_SECS    300
#define MAX_MESSAGE_RECEIVER_STATE_CHANGE_TIMEOUT_SECS  300
#define UNIQUE_ID_BUFFER_SIZE                           37

static const char* MESSENGER_SAVED_MQ_OPTIONS = "amqp_message_queue_options";

typedef struct AMQP_MESSENGER_INSTANCE_TAG
{
	char* device_id;
	char* iothub_host_fqdn;
	char* devices_path_format;
	char* send_link_target_suffix;
	char* receive_link_source_suffix;
	
	AMQP_MESSENGER_STATE_CHANGED_CALLBACK on_state_changed_callback;
	void* on_state_changed_context;

	bool receive_messages;
	ON_AMQP_MESSENGER_MESSAGE_RECEIVED on_message_received_callback;
	void* on_message_received_context;

	MESSAGE_QUEUE_HANDLE send_queue;
	AMQP_MESSENGER_STATE state;

	SESSION_HANDLE session_handle;
	LINK_HANDLE sender_link;
	MESSAGE_SENDER_HANDLE message_sender;
	MESSAGE_SENDER_STATE message_sender_current_state;
	MESSAGE_SENDER_STATE message_sender_previous_state;
	LINK_HANDLE receiver_link;
	MESSAGE_RECEIVER_HANDLE message_receiver;
	MESSAGE_RECEIVER_STATE message_receiver_current_state;
	MESSAGE_RECEIVER_STATE message_receiver_previous_state;

	MAP_HANDLE send_link_attach_properties;
	MAP_HANDLE receive_link_attach_properties;

	size_t send_error_count;
	size_t max_send_error_count;
	time_t last_message_sender_state_change_time;
	time_t last_message_receiver_state_change_time;
} AMQP_MESSENGER_INSTANCE;

typedef struct MESSAGE_SEND_CONTEXT_TAG
{
	MESSAGE_HANDLE message;
	bool is_destroyed;

	AMQP_MESSENGER_INSTANCE* messenger;

	AMQP_MESSENGER_SEND_COMPLETE_CALLBACK on_send_complete_callback;
	void* user_context;

	PROCESS_MESSAGE_COMPLETED_CALLBACK on_process_message_completed_callback;
} MESSAGE_SEND_CONTEXT;


static MESSAGE_SEND_CONTEXT* create_message_send_context()
{
	MESSAGE_SEND_CONTEXT* result;

	if ((result = (MESSAGE_SEND_CONTEXT*)malloc(sizeof(MESSAGE_SEND_CONTEXT))) == NULL)
	{
		LogError("Failed creating the message send context");
	}
	else
	{
		memset(result, 0, sizeof(MESSAGE_SEND_CONTEXT));
	}

	return result;
}

static void destroy_message_send_context(MESSAGE_SEND_CONTEXT* context)
{
	free(context);
}

static STRING_HANDLE create_devices_path(const char* devices_path_format, const char* iothub_host_fqdn, const char* device_id)
{
	STRING_HANDLE devices_path;

	if ((devices_path = STRING_new()) == NULL)
	{
		LogError("Failed creating devices_path (STRING_new failed)");
	}
	else if (STRING_sprintf(devices_path, devices_path_format, iothub_host_fqdn, device_id) != 0)
		{
			LogError("Failed creating devices_path (STRING_sprintf failed)");
			STRING_delete(devices_path);
			devices_path = NULL;
		}
	
	return devices_path;
}

static STRING_HANDLE create_link_address(STRING_HANDLE devices_path, const char* address_suffix)
{
	STRING_HANDLE link_address;

	if ((link_address = STRING_new()) == NULL)
	{
		LogError("failed creating link_address (STRING_new failed)");
	}
	else
	{
		const char* devices_path_char_ptr = STRING_c_str(devices_path);
		if (STRING_sprintf(link_address, LINK_BASE_ADDRESS, devices_path_char_ptr, address_suffix) != RESULT_OK)
		{
			LogError("Failed creating the link_address (STRING_sprintf failed)");
			STRING_delete(link_address);
			link_address = NULL;
		}
	}

	return link_address;
}

static STRING_HANDLE create_link_source_name(STRING_HANDLE link_name)
{
	STRING_HANDLE source_name;
	
	if ((source_name = STRING_new()) == NULL)
	{
		LogError("Failed creating the source_name (STRING_new failed)");
	}
	else
	{
		const char* link_name_char_ptr = STRING_c_str(link_name);
		if (STRING_sprintf(source_name, "%s-source", link_name_char_ptr) != RESULT_OK)
		{
			STRING_delete(source_name);
			source_name = NULL;
			LogError("Failed creating the source_name (STRING_sprintf failed)");
		}
	}

	return source_name;
}

static STRING_HANDLE create_link_target_name(STRING_HANDLE link_name)
{
	STRING_HANDLE target_name;

	if ((target_name = STRING_new()) == NULL)
	{
		LogError("Failed creating the target_name (STRING_new failed)");
	}
	else
	{
		const char* link_name_char_ptr = STRING_c_str(link_name);
		if (STRING_sprintf(target_name, "%s-target", link_name_char_ptr) != RESULT_OK)
		{
			STRING_delete(target_name);
			target_name = NULL;
			LogError("Failed creating the target_name (STRING_sprintf failed)");
		}
	}

	return target_name;
}

static STRING_HANDLE create_link_name(const char* prefix, const char* infix)
{
	char* unique_id;
	STRING_HANDLE tag = NULL;

	if ((unique_id = (char*)malloc(sizeof(char) * UNIQUE_ID_BUFFER_SIZE + 1)) == NULL)
	{
		LogError("Failed generating an unique tag (malloc failed)");
	}
	else
	{
        memset(unique_id, 0, sizeof(char) * UNIQUE_ID_BUFFER_SIZE + 1);

		if (UniqueId_Generate(unique_id, UNIQUE_ID_BUFFER_SIZE) != UNIQUEID_OK)
		{
			LogError("Failed generating an unique tag (UniqueId_Generate failed)");
		}
		else if ((tag = STRING_new()) == NULL)
		{
			LogError("Failed generating an unique tag (STRING_new failed)");
		}
		else if (STRING_sprintf(tag, "%s-%s-%s", prefix, infix, unique_id) != RESULT_OK)
		{
			STRING_delete(tag);
			tag = NULL;
			LogError("Failed generating an unique tag (STRING_sprintf failed)");
		}

		free(unique_id);
	}

	return tag;
}

static void update_messenger_state(AMQP_MESSENGER_INSTANCE* instance, AMQP_MESSENGER_STATE new_state)
{
	if (new_state != instance->state)
	{
		AMQP_MESSENGER_STATE previous_state = instance->state;
		instance->state = new_state;

		if (instance->on_state_changed_callback != NULL)
		{
			instance->on_state_changed_callback(instance->on_state_changed_context, previous_state, new_state);
		}
	}
}

static int add_link_attach_properties(LINK_HANDLE link, MAP_HANDLE user_defined_properties)
{
	int result;
	fields attach_properties;

	if ((attach_properties = amqpvalue_create_map()) == NULL)
	{
		LogError("Failed to create the map for attach properties.");
		result = __FAILURE__;
	}
	else
	{
		const char* const* keys;
		const char* const* values;
		size_t count;

		if (Map_GetInternals(user_defined_properties, &keys, &values, &count) != MAP_OK)
		{
			LogError("failed getting user defined properties details.");
			result = __FAILURE__;
		}
		else
		{
			size_t i;
			result = RESULT_OK;

			for (i = 0; i < count && result == RESULT_OK; i++)
			{
				AMQP_VALUE key;
				AMQP_VALUE value;

				if ((key = amqpvalue_create_symbol(keys[i])) == NULL)
				{
					LogError("Failed creating AMQP_VALUE For key %s.", keys[i]);
					result = __FAILURE__;
				}
				else
				{
					if ((value = amqpvalue_create_string(values[i])) == NULL)
					{
						LogError("Failed creating AMQP_VALUE For key %s value", keys[i]);
						result = __FAILURE__;
					}
					else
					{
						if ((result = amqpvalue_set_map_value(attach_properties, key, value)) != 0)
						{
							LogError("Failed adding property %s to map (%d)", keys[i], result);
							result = __FAILURE__;
						}

						amqpvalue_destroy(value);
					}

					amqpvalue_destroy(key);
				}
			}

			if (result == RESULT_OK)
			{
				if ((result = link_set_attach_properties(link, attach_properties)) != 0)
				{
					LogError("Failed attaching properties to link (%d)", result);
					result = __FAILURE__;
				}
				else
				{
					result = RESULT_OK;
				}
			}
		}

		amqpvalue_destroy(attach_properties);
	}

	return result;
}

static void destroy_message_sender(AMQP_MESSENGER_INSTANCE* instance)
{
	if (instance->message_sender != NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_060: [`instance->message_sender` shall be destroyed using messagesender_destroy()]
		messagesender_destroy(instance->message_sender);
		instance->message_sender = NULL;
		instance->message_sender_current_state = MESSAGE_SENDER_STATE_IDLE;
		instance->message_sender_previous_state = MESSAGE_SENDER_STATE_IDLE;
		instance->last_message_sender_state_change_time = INDEFINITE_TIME;
	}

	if (instance->sender_link != NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_063: [`instance->sender_link` shall be destroyed using link_destroy()]
		link_destroy(instance->sender_link);
		instance->sender_link = NULL;
	}
}

static void on_message_sender_state_changed_callback(void* context, MESSAGE_SENDER_STATE new_state, MESSAGE_SENDER_STATE previous_state)
{
	if (context == NULL)
	{
		LogError("on_message_sender_state_changed_callback was invoked with a NULL context; although unexpected, this failure will be ignored");
	}
	else if (new_state != previous_state)
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)context;
		instance->message_sender_current_state = new_state;
		instance->message_sender_previous_state = previous_state;
		instance->last_message_sender_state_change_time = get_time(NULL);
	}
}

static int create_message_sender(AMQP_MESSENGER_INSTANCE* instance)
{
	int result;

	STRING_HANDLE link_name = NULL;
	STRING_HANDLE source_name = NULL;
	AMQP_VALUE source = NULL;
	AMQP_VALUE target = NULL;
	STRING_HANDLE devices_path = NULL;
	STRING_HANDLE send_link_address = NULL;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_033: [A variable, named `devices_path`, shall be created concatenating `instance->iothub_host_fqdn`, "/devices/" and `instance->device_id`]
	if ((devices_path = create_devices_path(instance->devices_path_format, instance->iothub_host_fqdn, instance->device_id)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_034: [If `devices_path` fails to be created, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (failed creating the 'devices_path')");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_035: [A variable, named `send_link_address`, shall be created concatenating "amqps://", `devices_path` and "/messages/events"]
	else if ((send_link_address = create_link_address(devices_path, instance->send_link_target_suffix)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_036: [If `send_link_address` fails to be created, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (failed creating the 'send_link_address')");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_037: [A `link_name` variable shall be created using an unique string label per AMQP session]
	else if ((link_name = create_link_name(MESSAGE_SENDER_LINK_NAME_PREFIX, instance->device_id)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_038: [If `link_name` fails to be created, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (failed creating an unique link name)");
	}
	else if ((source_name = create_link_source_name(link_name)) == NULL)
	{
		result = __FAILURE__;
		LogError("Failed creating the message sender (failed creating an unique source name)");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_039: [A `source` variable shall be created with messaging_create_source() using an unique string label per AMQP session]
	else if ((source = messaging_create_source(STRING_c_str(source_name))) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_040: [If `source` fails to be created, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (messaging_create_source failed)");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_041: [A `target` variable shall be created with messaging_create_target() using `send_link_address`]
	else if ((target = messaging_create_target(STRING_c_str(send_link_address))) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_042: [If `target` fails to be created, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (messaging_create_target failed)");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_043: [`instance->sender_link` shall be set using link_create(), passing `instance->session_handle`, `link_name`, "role_sender", `source` and `target` as parameters]
	else if ((instance->sender_link = link_create(instance->session_handle, STRING_c_str(link_name), role_sender, source, target)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_044: [If link_create() fails, amqp_messenger_do_work() shall fail and return]
		result = __FAILURE__;
		LogError("Failed creating the message sender (link_create failed)");
	}
	else if (instance->send_link_attach_properties != NULL &&
		add_link_attach_properties(instance->sender_link, instance->send_link_attach_properties) != RESULT_OK)
	{
		LogError("Failed setting message sender link max message size.");
		result = __FAILURE__;
		link_destroy(instance->sender_link);
		instance->sender_link = NULL;
	}
	else
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_047: [`instance->sender_link` maximum message size shall be set to UINT64_MAX using link_set_max_message_size()]
		if (link_set_max_message_size(instance->sender_link, MESSAGE_SENDER_MAX_LINK_SIZE) != RESULT_OK)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_048: [If link_set_max_message_size() fails, it shall be logged and ignored.]
			LogError("Failed setting message sender link max message size.");
		}

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_051: [`instance->message_sender` shall be created using messagesender_create(), passing the `instance->sender_link` and `on_message_sender_state_changed_callback`]
		if ((instance->message_sender = messagesender_create(instance->sender_link, on_message_sender_state_changed_callback, (void*)instance)) == NULL)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_052: [If messagesender_create() fails, amqp_messenger_do_work() shall fail and return]
			result = __FAILURE__;
			link_destroy(instance->sender_link);
			instance->sender_link = NULL;
			LogError("Failed creating the message sender (messagesender_create failed)");
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_053: [`instance->message_sender` shall be opened using messagesender_open()]
			if (messagesender_open(instance->message_sender) != RESULT_OK)
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_054: [If messagesender_open() fails, amqp_messenger_do_work() shall fail and return]
				result = __FAILURE__;
				messagesender_destroy(instance->message_sender);
				instance->message_sender = NULL;
				link_destroy(instance->sender_link);
				instance->sender_link = NULL;
				LogError("Failed opening the AMQP message sender.");
			}
			else
			{
				result = RESULT_OK;
			}
		}
	}

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_055: [Before returning, amqp_messenger_do_work() shall release all the temporary memory it has allocated]
	if (link_name != NULL)
		STRING_delete(link_name);
	if (source_name != NULL)
		STRING_delete(source_name);
	if (source != NULL)
		amqpvalue_destroy(source);
	if (target != NULL)
		amqpvalue_destroy(target);
	if (devices_path != NULL)
		STRING_delete(devices_path);
	if (send_link_address != NULL)
		STRING_delete(send_link_address);

	return result;
}

static void destroy_message_receiver(AMQP_MESSENGER_INSTANCE* instance)
{
	if (instance->message_receiver != NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_061: [`instance->message_receiver` shall be closed using messagereceiver_close()]
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_093: [`instance->message_receiver` shall be closed using messagereceiver_close()]
		if (messagereceiver_close(instance->message_receiver) != RESULT_OK)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_094: [If messagereceiver_close() fails, it shall be logged and ignored]
			LogError("Failed closing the AMQP message receiver (this failure will be ignored).");
		}

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_062: [`instance->message_receiver` shall be destroyed using messagereceiver_destroy()]
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_095: [`instance->message_receiver` shall be destroyed using messagereceiver_destroy()]
		messagereceiver_destroy(instance->message_receiver);

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_096: [`instance->message_receiver` shall be set to NULL]
		instance->message_receiver = NULL;
		instance->message_receiver_current_state = MESSAGE_RECEIVER_STATE_IDLE;
		instance->message_receiver_previous_state = MESSAGE_RECEIVER_STATE_IDLE;
		instance->last_message_receiver_state_change_time = INDEFINITE_TIME;
	}

	if (instance->receiver_link != NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_064: [`instance->receiver_link` shall be destroyed using link_destroy()]
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_097: [`instance->receiver_link` shall be destroyed using link_destroy()]
		link_destroy(instance->receiver_link);
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_098: [`instance->receiver_link` shall be set to NULL]
		instance->receiver_link = NULL;
	}
}

static void on_message_receiver_state_changed_callback(const void* context, MESSAGE_RECEIVER_STATE new_state, MESSAGE_RECEIVER_STATE previous_state)
{
	if (context == NULL)
	{
		LogError("on_message_receiver_state_changed_callback was invoked with a NULL context; although unexpected, this failure will be ignored");
	}
	else
	{
		if (new_state != previous_state)
		{
			AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)context;
			instance->message_receiver_current_state = new_state;
			instance->message_receiver_previous_state = previous_state;
			instance->last_message_receiver_state_change_time = get_time(NULL);
		}
	}
}

static AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* create_message_disposition_info(AMQP_MESSENGER_INSTANCE* messenger)
{
	AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* result;

	if ((result = (AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO*)malloc(sizeof(AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO))) == NULL)
	{
		LogError("Failed creating AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO container (malloc failed)");
		result = NULL;
	}
	else
	{
		delivery_number message_id;

		if (messagereceiver_get_received_message_id(messenger->message_receiver, &message_id) != RESULT_OK)
		{
			LogError("Failed creating AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO container (messagereceiver_get_received_message_id failed)");
			free(result);
			result = NULL;
		}
		else
		{
			const char* link_name;

			if (messagereceiver_get_link_name(messenger->message_receiver, &link_name) != RESULT_OK)
			{
				LogError("Failed creating AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO container (messagereceiver_get_link_name failed)");
				free(result);
				result = NULL;
			}
			else if (mallocAndStrcpy_s(&result->source, link_name) != RESULT_OK)
			{
				LogError("Failed creating AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO container (failed copying link name)");
				free(result);
				result = NULL;
			}
			else
			{
				result->message_id = message_id;
			}
		}
	}

	return result;
}

static void destroy_message_disposition_info(AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* disposition_info)
{
	free(disposition_info->source);
	free(disposition_info);
}

static AMQP_VALUE create_uamqp_disposition_result_from(AMQP_MESSENGER_DISPOSITION_RESULT disposition_result)
{
	AMQP_VALUE uamqp_disposition_result;

	if (disposition_result == AMQP_MESSENGER_DISPOSITION_RESULT_NONE)
	{
		uamqp_disposition_result = NULL; // intentionally not sending an answer.
	}
	else if (disposition_result == AMQP_MESSENGER_DISPOSITION_RESULT_ACCEPTED)
	{
		uamqp_disposition_result = messaging_delivery_accepted();
	}
	else if (disposition_result == AMQP_MESSENGER_DISPOSITION_RESULT_RELEASED)
	{
		uamqp_disposition_result = messaging_delivery_released();
	}
	else if (disposition_result == AMQP_MESSENGER_DISPOSITION_RESULT_REJECTED)
	{
		uamqp_disposition_result = messaging_delivery_rejected("Rejected by application", "Rejected by application");
	}
	else
	{
		LogError("Failed creating a disposition result for messagereceiver (result %d is not supported)", disposition_result);
		uamqp_disposition_result = NULL;
	}

	return uamqp_disposition_result;
}

static AMQP_VALUE on_message_received_internal_callback(const void* context, MESSAGE_HANDLE message)
{
	AMQP_VALUE result;
	AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)context;
	AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* message_disposition_info;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_186: [A AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO instance shall be created containing the source link name and message delivery ID]
	if ((message_disposition_info = create_message_disposition_info(instance)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_187: [**If the AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO instance fails to be created, on_message_received_internal_callback shall return messaging_delivery_released()]
		LogError("on_message_received_internal_callback failed (failed creating AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO).");
		result = messaging_delivery_released();
	}
	else
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_123: [`instance->on_message_received_callback` shall be invoked passing the IOTHUB_MESSAGE_HANDLE and AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO instance]
		AMQP_MESSENGER_DISPOSITION_RESULT disposition_result = instance->on_message_received_callback(message, message_disposition_info, instance->on_message_received_context);

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_125: [If `instance->on_message_received_callback` returns AMQP_MESSENGER_DISPOSITION_RESULT_ACCEPTED, on_message_received_internal_callback shall return the result of messaging_delivery_accepted()]
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_126: [If `instance->on_message_received_callback` returns AMQP_MESSENGER_DISPOSITION_RESULT_RELEASED, on_message_received_internal_callback shall return the result of messaging_delivery_released()]
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_127: [If `instance->on_message_received_callback` returns AMQP_MESSENGER_DISPOSITION_RESULT_REJECTED, on_message_received_internal_callback shall return the result of messaging_delivery_rejected()]
		result = create_uamqp_disposition_result_from(disposition_result);
	}

	return result;
}

static int create_message_receiver(AMQP_MESSENGER_INSTANCE* instance)
{
	int result;

	STRING_HANDLE devices_path = NULL;
	STRING_HANDLE receive_link_address = NULL;
	STRING_HANDLE link_name = NULL;
	STRING_HANDLE target_name = NULL;
	AMQP_VALUE source = NULL;
	AMQP_VALUE target = NULL;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_068: [A variable, named `devices_path`, shall be created concatenating `instance->iothub_host_fqdn`, "/devices/" and `instance->device_id`]
	if ((devices_path = create_devices_path(instance->devices_path_format, instance->iothub_host_fqdn, instance->device_id)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_069: [If `devices_path` fails to be created, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (failed creating the 'devices_path')");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_070: [A variable, named `receive_link_address`, shall be created concatenating "amqps://", `devices_path` and "/messages/devicebound"]
	else if ((receive_link_address = create_link_address(devices_path, instance->receive_link_source_suffix)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_071: [If `receive_link_address` fails to be created, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (failed creating the 'receive_link_address')");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_072: [A `link_name` variable shall be created using an unique string label per AMQP session]
	else if ((link_name = create_link_name(MESSAGE_RECEIVER_LINK_NAME_PREFIX, instance->device_id)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_073: [If `link_name` fails to be created, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (failed creating an unique link name)");
		result = __FAILURE__;
	}
	else if ((target_name = create_link_target_name(link_name)) == NULL)
	{
		LogError("Failed creating the message receiver (failed creating an unique target name)");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_074: [A `target` variable shall be created with messaging_create_target() using an unique string label per AMQP session]
	else if ((target = messaging_create_target(STRING_c_str(target_name))) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_075: [If `target` fails to be created, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (messaging_create_target failed)");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_076: [A `source` variable shall be created with messaging_create_source() using `receive_link_address`]
	else if ((source = messaging_create_source(STRING_c_str(receive_link_address))) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_077: [If `source` fails to be created, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (messaging_create_source failed)");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_078: [`instance->receiver_link` shall be set using link_create(), passing `instance->session_handle`, `link_name`, "role_receiver", `source` and `target` as parameters]
	else if ((instance->receiver_link = link_create(instance->session_handle, STRING_c_str(link_name), role_receiver, source, target)) == NULL)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_079: [If link_create() fails, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (link_create failed)");
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_080: [`instance->receiver_link` settle mode shall be set to "receiver_settle_mode_first" using link_set_rcv_settle_mode(), ]
	else if (link_set_rcv_settle_mode(instance->receiver_link, receiver_settle_mode_first) != RESULT_OK)
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_081: [If link_set_rcv_settle_mode() fails, amqp_messenger_do_work() shall fail and return]
		LogError("Failed creating the message receiver (link_set_rcv_settle_mode failed)");
		link_destroy(instance->receiver_link);
		instance->receiver_link = NULL;
		result = __FAILURE__;
	}
	else if (instance->receive_link_attach_properties != NULL &&
		add_link_attach_properties(instance->receiver_link, instance->receive_link_attach_properties) != RESULT_OK)
	{
		LogError("Failed setting message receiver link max message size.");
		link_destroy(instance->receiver_link);
		instance->receiver_link = NULL;
		result = __FAILURE__;
	}
	else
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_082: [`instance->receiver_link` maximum message size shall be set to 65536 using link_set_max_message_size()]
		if (link_set_max_message_size(instance->receiver_link, MESSAGE_RECEIVER_MAX_LINK_SIZE) != RESULT_OK)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_083: [If link_set_max_message_size() fails, it shall be logged and ignored.]
			LogError("Failed setting message receiver link max message size.");
		}

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_086: [`instance->message_receiver` shall be created using messagereceiver_create(), passing the `instance->receiver_link` and `on_messagereceiver_state_changed_callback`]
		if ((instance->message_receiver = messagereceiver_create(instance->receiver_link, on_message_receiver_state_changed_callback, (void*)instance)) == NULL)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_087: [If messagereceiver_create() fails, amqp_messenger_do_work() shall fail and return]
			LogError("Failed creating the message receiver (messagereceiver_create failed)");
			link_destroy(instance->receiver_link);
			instance->receiver_link = NULL;
			result = __FAILURE__;
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_088: [`instance->message_receiver` shall be opened using messagereceiver_open(), passing `on_message_received_internal_callback`]
			if (messagereceiver_open(instance->message_receiver, on_message_received_internal_callback, (void*)instance) != RESULT_OK)
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_089: [If messagereceiver_open() fails, amqp_messenger_do_work() shall fail and return]
				LogError("Failed opening the AMQP message receiver.");
				messagereceiver_destroy(instance->message_receiver);
				link_destroy(instance->receiver_link);
				instance->message_receiver = NULL;
				instance->receiver_link = NULL;
				result = __FAILURE__;
			}
			else
			{
				result = RESULT_OK;
			}
		}
	}

	if (devices_path != NULL)
		STRING_delete(devices_path);
	if (receive_link_address != NULL)
		STRING_delete(receive_link_address);
	if (link_name != NULL)
		STRING_delete(link_name);
	if (target_name != NULL)
		STRING_delete(target_name);
	if (source != NULL)
		amqpvalue_destroy(source);
	if (target != NULL)
		amqpvalue_destroy(target);

	return result;
}

static void on_send_complete_callback(void* context, MESSAGE_SEND_RESULT send_result)
{ 
	if (context != NULL)
	{
		MESSAGE_QUEUE_RESULT mq_result;
		MESSAGE_SEND_CONTEXT* msg_ctx = (MESSAGE_SEND_CONTEXT*)context;

		if (send_result == MESSAGE_SEND_OK)
		{
			mq_result = MESSAGE_QUEUE_SUCCESS;
		}
		else
		{
			mq_result = MESSAGE_QUEUE_ERROR;
		}

		msg_ctx->on_process_message_completed_callback(msg_ctx->messenger->send_queue, (MQ_MESSAGE_HANDLE)msg_ctx->message, mq_result, NULL);
	}
}

static void on_process_message_callback(MESSAGE_QUEUE_HANDLE message_queue, MQ_MESSAGE_HANDLE message, PROCESS_MESSAGE_COMPLETED_CALLBACK on_process_message_completed_callback, void* context)
{
	if (message_queue == NULL || message == NULL || on_process_message_completed_callback == NULL || context == NULL)
	{
		LogError("invalid argument (message_queue=%p, message=%p, on_process_message_completed_callback=%p, context=%p)", message_queue, message, on_process_message_completed_callback, context);
	}
	else
	{
		MESSAGE_SEND_CONTEXT* message_context = (MESSAGE_SEND_CONTEXT*)context;
		message_context->on_process_message_completed_callback = on_process_message_completed_callback;

		if (messagesender_send(message_context->messenger->message_sender, (MESSAGE_HANDLE)message, on_send_complete_callback, context) != 0)
		{
			LogError("Failed sending AMQP message");
			on_process_message_completed_callback(message_queue, message, MESSAGE_QUEUE_ERROR, NULL);
		}

		message_destroy((MESSAGE_HANDLE)message);
		message_context->is_destroyed = true;
	}
}



// ---------- Set/Retrieve Options Helpers ----------//

static void* amqp_messenger_clone_option(const char* name, const void* value)
{
	void* result;

	if (name == NULL || value == NULL)
	{
		LogError("invalid argument (name=%p, value=%p)", name, value);
		result = NULL;
	}
	else
	{
		if (strcmp(MESSENGER_SAVED_MQ_OPTIONS, name) == 0)
		{
			if ((result = (void*)OptionHandler_Clone((OPTIONHANDLER_HANDLE)value)) == NULL)
			{
				LogError("failed cloning option '%s'", name);
			}
		}
		else
		{
			LogError("Failed to clone messenger option (option with name '%s' is not suppported)", name);
			result = NULL;
		}
	}

	return result;
}

static void amqp_messenger_destroy_option(const char* name, const void* value)
{
	if (name == NULL || value == NULL)
	{
		LogError("invalid argument (name=%p, value=%p)", name, value);
	}
	else if (strcmp(MESSENGER_SAVED_MQ_OPTIONS, name) == 0)
	{
		OptionHandler_Destroy((OPTIONHANDLER_HANDLE)value);
	}
	else
	{
		LogError("invalid argument (option '%s' is not suppported)", name);
	}
}


// Public API:

int amqp_messenger_subscribe_for_messages(AMQP_MESSENGER_HANDLE messenger_handle, ON_AMQP_MESSENGER_MESSAGE_RECEIVED on_message_received_callback, void* context)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_016: [If `messenger_handle` is NULL, amqp_messenger_subscribe_for_messages() shall fail and return __FAILURE__]
	if (messenger_handle == NULL)
	{
		result = __FAILURE__;
		LogError("amqp_messenger_subscribe_for_messages failed (messenger_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_017: [If `instance->receive_messages` is already true, amqp_messenger_subscribe_for_messages() shall fail and return __FAILURE__]
		if (instance->receive_messages)
		{
			result = __FAILURE__;
			LogError("amqp_messenger_subscribe_for_messages failed (messenger already subscribed)");
		}
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_018: [If `on_message_received_callback` is NULL, amqp_messenger_subscribe_for_messages() shall fail and return __FAILURE__]
		else if (on_message_received_callback == NULL)
		{
			result = __FAILURE__;
			LogError("amqp_messenger_subscribe_for_messages failed (on_message_received_callback is NULL)");
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_019: [`on_message_received_callback` shall be saved on `instance->on_message_received_callback`]
			instance->on_message_received_callback = on_message_received_callback;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_020: [`context` shall be saved on `instance->on_message_received_context`]
			instance->on_message_received_context = context;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_021: [amqp_messenger_subscribe_for_messages() shall set `instance->receive_messages` to true]
			instance->receive_messages = true;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_022: [If no failures occurr, amqp_messenger_subscribe_for_messages() shall return 0]
			result = RESULT_OK;
		}
	}

	return result;
}

int amqp_messenger_unsubscribe_for_messages(AMQP_MESSENGER_HANDLE messenger_handle)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_023: [If `messenger_handle` is NULL, amqp_messenger_unsubscribe_for_messages() shall fail and return __FAILURE__]
	if (messenger_handle == NULL)
	{
		result = __FAILURE__;
		LogError("amqp_messenger_unsubscribe_for_messages failed (messenger_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_024: [If `instance->receive_messages` is already false, amqp_messenger_unsubscribe_for_messages() shall fail and return __FAILURE__]
		if (instance->receive_messages == false)
		{
			result = __FAILURE__;
			LogError("amqp_messenger_unsubscribe_for_messages failed (messenger is not subscribed)");
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_025: [amqp_messenger_unsubscribe_for_messages() shall set `instance->receive_messages` to false]
			instance->receive_messages = false;
			
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_026: [amqp_messenger_unsubscribe_for_messages() shall set `instance->on_message_received_callback` to NULL]
			instance->on_message_received_callback = NULL;
			
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_027: [amqp_messenger_unsubscribe_for_messages() shall set `instance->on_message_received_context` to NULL]
			instance->on_message_received_context = NULL;
			
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_028: [If no failures occurr, amqp_messenger_unsubscribe_for_messages() shall return 0]
			result = RESULT_OK;
		}
	}

	return result;
}

int amqp_messenger_send_message_disposition(AMQP_MESSENGER_HANDLE messenger_handle, AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* disposition_info, AMQP_MESSENGER_DISPOSITION_RESULT disposition_result)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_179: [If `messenger_handle` or `disposition_info` are NULL, amqp_messenger_send_message_disposition() shall fail and return __FAILURE__]  
	if (messenger_handle == NULL || disposition_info == NULL)
	{
		LogError("Failed sending message disposition (either messenger_handle (%p) or disposition_info (%p) are NULL)", messenger_handle, disposition_info);
		result = __FAILURE__;
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_180: [If `disposition_info->source` is NULL, amqp_messenger_send_message_disposition() shall fail and return __FAILURE__]  
	else if (disposition_info->source == NULL)
	{
		LogError("Failed sending message disposition (disposition_info->source is NULL)");
		result = __FAILURE__;
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* messenger = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_189: [If `messenger_handle->message_receiver` is NULL, amqp_messenger_send_message_disposition() shall fail and return __FAILURE__]
		if (messenger->message_receiver == NULL)
		{
			LogError("Failed sending message disposition (message_receiver is not created; check if it is subscribed)");
			result = __FAILURE__;
		}
		else
		{
			AMQP_VALUE uamqp_disposition_result;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_181: [An AMQP_VALUE disposition result shall be created corresponding to the `disposition_result` provided]
			if ((uamqp_disposition_result = create_uamqp_disposition_result_from(disposition_result)) == NULL)
			{
				LogError("Failed sending message disposition (disposition result %d is not supported)", disposition_result);
				result = __FAILURE__;
			}
			else
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_182: [`messagereceiver_send_message_disposition()` shall be invoked passing `disposition_info->source`, `disposition_info->message_id` and the corresponding AMQP_VALUE disposition result]  
				if (messagereceiver_send_message_disposition(messenger->message_receiver, disposition_info->source, disposition_info->message_id, uamqp_disposition_result) != RESULT_OK)
				{
					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_183: [If `messagereceiver_send_message_disposition()` fails, amqp_messenger_send_message_disposition() shall fail and return __FAILURE__]  
					LogError("Failed sending message disposition (messagereceiver_send_message_disposition failed)");
					result = __FAILURE__;
				}
				else
				{
					destroy_message_disposition_info(disposition_info);

					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_185: [If no failures occurr, amqp_messenger_send_message_disposition() shall return 0]  
					result = RESULT_OK;
				}

				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_184: [amqp_messenger_send_message_disposition() shall destroy the AMQP_VALUE disposition result]
				amqpvalue_destroy(uamqp_disposition_result);
			}
		}
	}

	return result;
}

static void on_message_processing_completed_callback(MQ_MESSAGE_HANDLE message, MESSAGE_QUEUE_RESULT result, USER_DEFINED_REASON reason, void* message_context)
{
	(void)reason;

	if (message_context == NULL)
	{
		LogError("on_message_processing_completed_callback invoked with NULL context");
	}
	else
	{
		MESSAGE_SEND_CONTEXT* msg_ctx = (MESSAGE_SEND_CONTEXT*)message_context;
		AMQP_MESSENGER_SEND_RESULT messenger_send_result;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_107: [If no failure occurs, `task->on_send_complete_callback` shall be invoked with result EVENT_SEND_COMPLETE_RESULT_OK]  
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_108: [If a failure occurred, `task->on_send_complete_callback` shall be invoked with result EVENT_SEND_COMPLETE_RESULT_ERROR_FAIL_SENDING] 
		if (result == MESSAGE_QUEUE_SUCCESS)
		{
			messenger_send_result = AMQP_MESSENGER_SEND_RESULT_OK;
		}
		else if (result == MESSAGE_QUEUE_TIMEOUT)
		{
			messenger_send_result = AMQP_MESSENGER_SEND_RESULT_ERROR_TIMEOUT;
		}
		else if (result == MESSAGE_QUEUE_CANCELLED && msg_ctx->messenger->state == AMQP_MESSENGER_STATE_STOPPED)
		{
			messenger_send_result = AMQP_MESSENGER_SEND_RESULT_MESSENGER_DESTROYED;
		}
		else
		{
			msg_ctx->messenger->send_error_count++;

			messenger_send_result = AMQP_MESSENGER_SEND_RESULT_ERROR_FAIL_SENDING;
		}

		if (msg_ctx->on_send_complete_callback != NULL)
		{
			msg_ctx->on_send_complete_callback(messenger_send_result, msg_ctx->user_context);
		}

		if (!msg_ctx->is_destroyed)
		{
			message_destroy((MESSAGE_HANDLE)message);
		}

		destroy_message_send_context(msg_ctx);
	}
}

int amqp_messenger_send_async(AMQP_MESSENGER_HANDLE messenger_handle, MESSAGE_HANDLE message, AMQP_MESSENGER_SEND_COMPLETE_CALLBACK on_user_defined_send_complete_callback, void* user_context)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_134: [If `messenger_handle` is NULL, amqp_messenger_send_async() shall fail and return a non-zero value]  
	if (messenger_handle == NULL || message == NULL || on_user_defined_send_complete_callback == NULL)
	{
		LogError("invalid argument (messenger_handle=%p, message=%p, on_user_defined_send_complete_callback=%p)", messenger_handle, message, on_user_defined_send_complete_callback);
		result = __FAILURE__;
	}
	else
	{
		MESSAGE_HANDLE cloned_message;

		if ((cloned_message = message_clone(message)) == NULL)
		{
			LogError("Failed cloning AMQP message");
			result = __FAILURE__;
		}
		else
		{
			MESSAGE_SEND_CONTEXT* message_context;
			AMQP_MESSENGER_INSTANCE *instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

			if ((message_context = create_message_send_context()) == NULL)
			{
				LogError("Failed creating context for sending message");
				message_destroy(cloned_message);
				result = __FAILURE__;
			}
			else
			{
				message_context->message = cloned_message;
				message_context->messenger = instance;
				message_context->on_send_complete_callback = on_user_defined_send_complete_callback;
				message_context->user_context = user_context;

				if (message_queue_add(instance->send_queue, (MQ_MESSAGE_HANDLE)cloned_message, on_message_processing_completed_callback, (void*)message_context) != RESULT_OK)
				{
					LogError("Failed adding message to send queue");
					destroy_message_send_context(message_context);
					message_destroy(cloned_message);
					result = __FAILURE__;
				}
				else
				{
					result = RESULT_OK;
				}
			}
		}
	}

	return result;
}

int amqp_messenger_get_send_status(AMQP_MESSENGER_HANDLE messenger_handle, AMQP_MESSENGER_SEND_STATUS* send_status)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_144: [If `messenger_handle` is NULL, amqp_messenger_get_send_status() shall fail and return a non-zero value] 
	if (messenger_handle == NULL || send_status == NULL)
	{
		LogError("invalid argument (messenger_handle=%p, send_status=%p)", messenger_handle, send_status);
		result = __FAILURE__;
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;
		bool is_empty;

		if (message_queue_is_empty(instance->send_queue, &is_empty) != 0)
		{
			LogError("Failed verifying if send queue is empty");
			result = __FAILURE__;
		}
		else
		{
			*send_status = (is_empty ? AMQP_MESSENGER_SEND_STATUS_IDLE : AMQP_MESSENGER_SEND_STATUS_BUSY);
			result = RESULT_OK;
		}
	}

	return result;
}

int amqp_messenger_start(AMQP_MESSENGER_HANDLE messenger_handle, SESSION_HANDLE session_handle)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_029: [If `messenger_handle` is NULL, amqp_messenger_start() shall fail and return __FAILURE__]
	if (messenger_handle == NULL)
	{
		result = __FAILURE__;
		LogError("amqp_messenger_start failed (messenger_handle is NULL)");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_030: [If `session_handle` is NULL, amqp_messenger_start() shall fail and return __FAILURE__]
	else if (session_handle == NULL)
	{
		result = __FAILURE__;
		LogError("amqp_messenger_start failed (session_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_031: [If `instance->state` is not AMQP_MESSENGER_STATE_STOPPED, amqp_messenger_start() shall fail and return __FAILURE__]
		if (instance->state != AMQP_MESSENGER_STATE_STOPPED)
		{
			result = __FAILURE__;
			LogError("amqp_messenger_start failed (current state is %d; expected AMQP_MESSENGER_STATE_STOPPED)", instance->state);
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_032: [`session_handle` shall be saved on `instance->session_handle`]
			instance->session_handle = session_handle;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_115: [If no failures occurr, `instance->state` shall be set to AMQP_MESSENGER_STATE_STARTING, and `instance->on_state_changed_callback` invoked if provided]
			update_messenger_state(instance, AMQP_MESSENGER_STATE_STARTING);

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_056: [If no failures occurr, amqp_messenger_start() shall return 0]
			result = RESULT_OK;
		}
	}

	return result;
}

int amqp_messenger_stop(AMQP_MESSENGER_HANDLE messenger_handle)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_057: [If `messenger_handle` is NULL, amqp_messenger_stop() shall fail and return a non-zero value]
	if (messenger_handle == NULL)
	{
		result = __FAILURE__;
		LogError("amqp_messenger_stop failed (messenger_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_058: [If `instance->state` is AMQP_MESSENGER_STATE_STOPPED, amqp_messenger_stop() shall fail and return a non-zero value]
		if (instance->state == AMQP_MESSENGER_STATE_STOPPED)
		{
			result = __FAILURE__;
			LogError("amqp_messenger_stop failed (messenger is already stopped)");
		}
		else
		{
			update_messenger_state(instance, AMQP_MESSENGER_STATE_STOPPING);

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_152: [amqp_messenger_stop() shall close and destroy `instance->message_sender` and `instance->message_receiver`]  
			destroy_message_sender(instance);
			destroy_message_receiver(instance);

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_162: [amqp_messenger_stop() shall move all items from `instance->in_progress_list` to the beginning of `instance->wait_to_send_list`]
			if (message_queue_move_all_back_to_pending(instance->send_queue) != RESULT_OK)
			{
				LogError("Messenger failed to move events in progress back to wait_to_send list");
				
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_163: [If not all items from `instance->in_progress_list` can be moved back to `instance->wait_to_send_list`, `instance->state` shall be set to AMQP_MESSENGER_STATE_ERROR, and `instance->on_state_changed_callback` invoked]
				update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				result = __FAILURE__;
			}
			else
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_164: [If all items get successfuly moved back to `instance->wait_to_send_list`, `instance->state` shall be set to AMQP_MESSENGER_STATE_STOPPED, and `instance->on_state_changed_callback` invoked]
				update_messenger_state(instance, AMQP_MESSENGER_STATE_STOPPED);
				result = RESULT_OK;
			}
		}
	}

	return result;
}

// @brief
//     Sets the messenger module state based on the state changes from messagesender and messagereceiver
static void process_state_changes(AMQP_MESSENGER_INSTANCE* instance)
{
	// Note: messagesender and messagereceiver are still not created or already destroyed 
	//       when state is AMQP_MESSENGER_STATE_STOPPED, so no checking is needed there.

	if (instance->state == AMQP_MESSENGER_STATE_STARTED)
	{
		if (instance->message_sender_current_state != MESSAGE_SENDER_STATE_OPEN)
		{
			LogError("messagesender reported unexpected state %d while messenger was started", instance->message_sender_current_state);
			update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
		}
		else if (instance->message_receiver != NULL && instance->message_receiver_current_state != MESSAGE_RECEIVER_STATE_OPEN)
		{
			if (instance->message_receiver_current_state == MESSAGE_RECEIVER_STATE_OPENING)
			{
				bool is_timed_out;
				if (is_timeout_reached(instance->last_message_receiver_state_change_time, MAX_MESSAGE_RECEIVER_STATE_CHANGE_TIMEOUT_SECS, &is_timed_out) != RESULT_OK)
				{
					LogError("messenger got an error (failed to verify messagereceiver start timeout)");
					update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				}
				else if (is_timed_out)
				{
					LogError("messenger got an error (messagereceiver failed to start within expected timeout (%d secs))", MAX_MESSAGE_RECEIVER_STATE_CHANGE_TIMEOUT_SECS);
					update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				}
			}
			else if (instance->message_receiver_current_state == MESSAGE_RECEIVER_STATE_ERROR ||
				instance->message_receiver_current_state == MESSAGE_RECEIVER_STATE_IDLE)
			{
				LogError("messagereceiver reported unexpected state %d while messenger is starting", instance->message_receiver_current_state);
				update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
			}
		}
	}
	else
	{
		if (instance->state == AMQP_MESSENGER_STATE_STARTING)
		{
			if (instance->message_sender_current_state == MESSAGE_SENDER_STATE_OPEN)
			{
				update_messenger_state(instance, AMQP_MESSENGER_STATE_STARTED);
			}
			else if (instance->message_sender_current_state == MESSAGE_SENDER_STATE_OPENING)
			{
				bool is_timed_out;
				if (is_timeout_reached(instance->last_message_sender_state_change_time, MAX_MESSAGE_SENDER_STATE_CHANGE_TIMEOUT_SECS, &is_timed_out) != RESULT_OK)
				{
					LogError("messenger failed to start (failed to verify messagesender start timeout)");
					update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				}
				else if (is_timed_out)
				{
					LogError("messenger failed to start (messagesender failed to start within expected timeout (%d secs))", MAX_MESSAGE_SENDER_STATE_CHANGE_TIMEOUT_SECS);
					update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				}
			}
			// For this module, the only valid scenario where messagesender state is IDLE is if 
			// the messagesender hasn't been created yet or already destroyed.
			else if ((instance->message_sender_current_state == MESSAGE_SENDER_STATE_ERROR) ||
				(instance->message_sender_current_state == MESSAGE_SENDER_STATE_CLOSING) ||
				(instance->message_sender_current_state == MESSAGE_SENDER_STATE_IDLE && instance->message_sender != NULL))
			{
				LogError("messagesender reported unexpected state %d while messenger is starting", instance->message_sender_current_state);
				update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
			}
		}
		// message sender and receiver are stopped/destroyed synchronously, so no need for state control.
	}
}

void amqp_messenger_do_work(AMQP_MESSENGER_HANDLE messenger_handle)
{
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_065: [If `messenger_handle` is NULL, amqp_messenger_do_work() shall fail and return]
	if (messenger_handle == NULL)
	{
		LogError("amqp_messenger_do_work failed (messenger_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		process_state_changes(instance);

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_151: [If `instance->state` is AMQP_MESSENGER_STATE_STARTING, amqp_messenger_do_work() shall create and open `instance->message_sender`]
		if (instance->state == AMQP_MESSENGER_STATE_STARTING)
		{
			if (instance->message_sender == NULL)
			{
				if (create_message_sender(instance) != RESULT_OK)
				{
					update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
				}
			}
		}
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_066: [If `instance->state` is not AMQP_MESSENGER_STATE_STARTED, amqp_messenger_do_work() shall return]
		else if (instance->state == AMQP_MESSENGER_STATE_STARTED)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_067: [If `instance->receive_messages` is true and `instance->message_receiver` is NULL, a message_receiver shall be created]
			if (instance->receive_messages == true &&
				instance->message_receiver == NULL &&
				create_message_receiver(instance) != RESULT_OK)
			{
				LogError("amqp_messenger_do_work warning (failed creating the message receiver [%s])", instance->device_id);
			}
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_092: [If `instance->receive_messages` is false and `instance->message_receiver` is not NULL, it shall be destroyed]
			else if (instance->receive_messages == false && instance->message_receiver != NULL)
			{
				destroy_message_receiver(instance);
			}

			message_queue_do_work(instance->send_queue);

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_161: [If amqp_messenger_do_work() fail sending events for `instance->event_send_retry_limit` times in a row, it shall invoke `instance->on_state_changed_callback`, if provided, with error code AMQP_MESSENGER_STATE_ERROR]
			if (instance->send_error_count >= instance->max_send_error_count)
			{
				LogError("amqp_messenger_do_work failed (failed sending events; reached max number of consecutive failures)");
				update_messenger_state(instance, AMQP_MESSENGER_STATE_ERROR);
			}
		}
	}
}

void amqp_messenger_destroy(AMQP_MESSENGER_HANDLE messenger_handle)
{
	if (messenger_handle == NULL)
	{
		LogError("invalid argument (messenger_handle is NULL)");
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		if (instance->state != AMQP_MESSENGER_STATE_STOPPED)
		{
			(void)amqp_messenger_stop(messenger_handle);
		}

		if (instance->send_queue != NULL)
		{
			message_queue_destroy(instance->send_queue);
		}
		if (instance->device_id != NULL)
		{
			free((void*)instance->device_id);
		}
		if (instance->iothub_host_fqdn != NULL)
		{
			free((void*)instance->iothub_host_fqdn);
		}
		if (instance->devices_path_format != NULL)
		{
			free((void*)instance->devices_path_format);
		}
		if (instance->send_link_target_suffix != NULL)
		{
			free((void*)instance->send_link_target_suffix);
		}
		if (instance->receive_link_source_suffix != NULL)
		{
			free((void*)instance->receive_link_source_suffix);
		}

		free((void*)instance);
	}
}

AMQP_MESSENGER_HANDLE amqp_messenger_create(const AMQP_MESSENGER_CONFIG* messenger_config)
{
	AMQP_MESSENGER_HANDLE handle;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_001: [If parameter `messenger_config` is NULL, amqp_messenger_create() shall return NULL]
	if (messenger_config == NULL)
	{
		handle = NULL;
		LogError("invalid argument (messenger_config is NULL)");
	}
	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_002: [If `messenger_config->device_id` is NULL, amqp_messenger_create() shall return NULL]
	else if (messenger_config->device_id == NULL || 
		messenger_config->iothub_host_fqdn == NULL || 
		messenger_config->devices_path_format == NULL || 
		messenger_config->receive_link_source_suffix == NULL ||
		messenger_config->send_link_target_suffix == NULL)
	{
		handle = NULL;
		LogError("invalid argument (device_id=%p, iothub_host_fqdn=%p, devices_path_format=%p, receive_link_source_suffix=%p, send_link_target_suffix=%s)", 
			messenger_config->device_id, messenger_config->iothub_host_fqdn, 
			messenger_config->devices_path_format, messenger_config->receive_link_source_suffix, messenger_config->send_link_target_suffix);
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_006: [amqp_messenger_create() shall allocate memory for the messenger instance structure (aka `instance`)]
		if ((instance = (AMQP_MESSENGER_INSTANCE*)malloc(sizeof(AMQP_MESSENGER_INSTANCE))) == NULL)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_007: [If malloc() fails, amqp_messenger_create() shall fail and return NULL]
			handle = NULL;
			LogError("failed allocating AMQP_MESSENGER_INSTANCE");
		}
		else
		{
			memset(instance, 0, sizeof(AMQP_MESSENGER_INSTANCE));
			instance->state = AMQP_MESSENGER_STATE_STOPPED;
			instance->message_sender_current_state = MESSAGE_SENDER_STATE_IDLE;
			instance->message_sender_previous_state = MESSAGE_SENDER_STATE_IDLE;
			instance->message_receiver_current_state = MESSAGE_RECEIVER_STATE_IDLE;
			instance->message_receiver_previous_state = MESSAGE_RECEIVER_STATE_IDLE;
			instance->last_message_sender_state_change_time = INDEFINITE_TIME;
			instance->last_message_receiver_state_change_time = INDEFINITE_TIME;
			instance->max_send_error_count = DEFAULT_MAX_SEND_ERROR_COUNT;

			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_008: [amqp_messenger_create() shall save a copy of `messenger_config->device_id` into `instance->device_id`]
			if (mallocAndStrcpy_s(&instance->device_id, messenger_config->device_id) != 0)
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_009: [If STRING_construct() fails, amqp_messenger_create() shall fail and return NULL]
				handle = NULL;
				LogError("failed copying device_id");
			}
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_010: [amqp_messenger_create() shall save a copy of `messenger_config->iothub_host_fqdn` into `instance->iothub_host_fqdn`]
			else if (mallocAndStrcpy_s(&instance->iothub_host_fqdn, messenger_config->iothub_host_fqdn) != 0)
			{
				// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_011: [If STRING_construct() fails, amqp_messenger_create() shall fail and return NULL]
				handle = NULL;
				LogError("failed copying iothub_host_fqdn)");
			}
			else if (mallocAndStrcpy_s(&instance->devices_path_format, messenger_config->devices_path_format) != 0)
			{
				handle = NULL;
				LogError("failed copying devices_path_format");
			}
			else if (mallocAndStrcpy_s(&instance->send_link_target_suffix, messenger_config->send_link_target_suffix) != 0)
			{
				handle = NULL;
				LogError("failed copying send_link_target_suffix");
			}
			else if (mallocAndStrcpy_s(&instance->receive_link_source_suffix, messenger_config->receive_link_source_suffix) != 0)
			{
				handle = NULL;
				LogError("failed copying receive_link_source_suffix");
			}
			else if (messenger_config->send_link_attach_properties != NULL &&
				(instance->send_link_attach_properties = Map_Clone(messenger_config->send_link_attach_properties)) == NULL)
			{
				handle = NULL;
				LogError("failed copying send link attach properties");
			}
			else if (messenger_config->receive_link_attach_properties != NULL &&
				(instance->receive_link_attach_properties = Map_Clone(messenger_config->receive_link_attach_properties)) == NULL)
			{
				handle = NULL;
				LogError("failed copying receive link attach properties");
			}
			else
			{
				MESSAGE_QUEUE_CONFIG mq_config;
				mq_config.max_retry_count = DEFAULT_EVENT_SEND_RETRY_LIMIT;
				mq_config.max_message_enqueued_time_secs = DEFAULT_EVENT_SEND_TIMEOUT_SECS;
				mq_config.max_message_processing_time_secs = 0;
				mq_config.on_process_message_callback = on_process_message_callback;

				if ((instance->send_queue = message_queue_create(&mq_config)) == NULL)
				{
					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_133: [If singlylinkedlist_create() fails, amqp_messenger_create() shall fail and return NULL] 
					handle = NULL;
					LogError("failed creating message queue");
				}
				else
				{
					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_013: [`messenger_config->on_state_changed_callback` shall be saved into `instance->on_state_changed_callback`]
					instance->on_state_changed_callback = messenger_config->on_state_changed_callback;

					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_014: [`messenger_config->on_state_changed_context` shall be saved into `instance->on_state_changed_context`]
					instance->on_state_changed_context = messenger_config->on_state_changed_context;

					// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_015: [If no failures occurr, amqp_messenger_create() shall return a handle to `instance`]
					handle = (AMQP_MESSENGER_HANDLE)instance;
				}
			}
		}

		if (handle == NULL)
		{
			amqp_messenger_destroy((AMQP_MESSENGER_HANDLE)instance);
		}
	}

	return handle;
}

int amqp_messenger_set_option(AMQP_MESSENGER_HANDLE messenger_handle, const char* name, void* value)
{
	int result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_167: [If `messenger_handle` or `name` or `value` is NULL, amqp_messenger_set_option shall fail and return a non-zero value]
	if (messenger_handle == NULL || name == NULL || value == NULL)
	{
		LogError("invalid argument (one of the followin are NULL: messenger_handle=%p, name=%p, value=%p)",
			messenger_handle, name, value);
		result = __FAILURE__;
	}
	else
	{
		AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;

		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_168: [If name matches MESSENGER_SAVED_MQ_OPTIONS, `value` shall be saved on `instance->event_send_timeout_secs`]
		if (strcmp(MESSENGER_OPTION_EVENT_SEND_TIMEOUT_SECS, name) == 0)
		{
			if (message_queue_set_max_message_enqueued_time_secs(instance->send_queue, *(size_t*)value) == 0)
			{
				LogError("Failed setting option %s", MESSENGER_OPTION_EVENT_SEND_TIMEOUT_SECS);
				result = __FAILURE__;
			}
			else
			{
				result = RESULT_OK;
			}
		}
		else
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_171: [If name does not match any supported option, authentication_set_option shall fail and return a non-zero value]
			LogError("invalid argument (option with name '%s' is not suppported)", name);
			result = __FAILURE__;
		}
	}

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_172: [If no errors occur, amqp_messenger_set_option shall return 0]
	return result;
}

OPTIONHANDLER_HANDLE amqp_messenger_retrieve_options(AMQP_MESSENGER_HANDLE messenger_handle)
{
	OPTIONHANDLER_HANDLE result;

	// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_173: [If `messenger_handle` is NULL, amqp_messenger_retrieve_options shall fail and return NULL]
	if (messenger_handle == NULL)
	{
		LogError("Failed to retrieve options from messenger instance (messenger_handle is NULL)");
		result = NULL;
	}
	else
	{
		// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_174: [An OPTIONHANDLER_HANDLE instance shall be created using OptionHandler_Create]
		result = OptionHandler_Create(amqp_messenger_clone_option, amqp_messenger_destroy_option, (pfSetOption)amqp_messenger_set_option);

		if (result == NULL)
		{
			// Codes_SRS_IOTHUBTRANSPORT_AMQP_MESSENGER_09_175: [If an OPTIONHANDLER_HANDLE instance fails to be created, amqp_messenger_retrieve_options shall fail and return NULL]
			LogError("Failed to retrieve options from messenger instance (OptionHandler_Create failed)");
		}
		else
		{
			AMQP_MESSENGER_INSTANCE* instance = (AMQP_MESSENGER_INSTANCE*)messenger_handle;
			OPTIONHANDLER_HANDLE mq_options;

			if ((mq_options = message_queue_retrieve_options(instance->send_queue)) == NULL)
			{
				LogError("failed to retrieve options from send queue)");
				OptionHandler_Destroy(result);
				result = NULL;
			}
			else if (OptionHandler_AddOption(result, MESSENGER_SAVED_MQ_OPTIONS, (void*)mq_options) != OPTIONHANDLER_OK)
			{
				LogError("failed adding option '%s'", MESSENGER_SAVED_MQ_OPTIONS);
				OptionHandler_Destroy(mq_options);
				OptionHandler_Destroy(result);
				result = NULL;
			}
		}
	}

	return result;
}

void amqp_messenger_destroy_disposition_info(AMQP_MESSENGER_MESSAGE_DISPOSITION_INFO* disposition_info)
{
	if (disposition_info == NULL)
	{
		LogError("Invalid argument (disposition_info is NULL)");
	}
	else
	{
		destroy_message_disposition_info(disposition_info);
	}
}
