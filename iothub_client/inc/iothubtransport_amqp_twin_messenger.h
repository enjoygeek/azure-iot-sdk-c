// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef IOTHUBTRANSPORT_AMQP_TWIN_MESSENGER
#define IOTHUBTRANSPORT_AMQP_TWIN_MESSENGER

#include "azure_c_shared_utility/umock_c_prod.h"
#include "azure_c_shared_utility/optionhandler.h"
#include "azure_uamqp_c/session.h"
#include "iothub_client_private.h"

#ifdef __cplusplus
extern "C"
{
#endif

	typedef struct TWIN_MESSENGER_INSTANCE* TWIN_MESSENGER_HANDLE;

	typedef enum TWIN_MESSENGER_SEND_STATUS_TAG
	{
		TWIN_MESSENGER_SEND_STATUS_IDLE,
		TWIN_MESSENGER_SEND_STATUS_BUSY
	} TWIN_MESSENGER_SEND_STATUS;

	typedef enum TWIN_REPORT_STATE_RESULT_TAG
	{
		TWIN_REPORT_STATE_RESULT_OK,
		TWIN_REPORT_STATE_RESULT_ERROR,
		TWIN_REPORT_STATE_RESULT_TIMEOUT,
		TWIN_REPORT_STATE_RESULT_MESSENGER_DESTROYED
	} TWIN_REPORT_STATE_RESULT;

	typedef enum TWIN_MESSENGER_STATE_TAG
	{
		TWIN_MESSENGER_STATE_STARTING,
		TWIN_MESSENGER_STATE_STARTED,
		TWIN_MESSENGER_STATE_STOPPING,
		TWIN_MESSENGER_STATE_STOPPED,
		TWIN_MESSENGER_STATE_ERROR
	} TWIN_MESSENGER_STATE;

	typedef enum TWIN_UPDATE_TYPE_TAG 
	{
		TWIN_UPDATE_TYPE_PARTIAL,
		TWIN_UPDATE_TYPE_COMPLETE
 	} TWIN_UPDATE_TYPE;

	typedef void(*TWIN_MESSENGER_STATE_CHANGED_CALLBACK)(void* context, TWIN_MESSENGER_STATE previous_state, TWIN_MESSENGER_STATE new_state);
	typedef void(*TWIN_MESSENGER_REPORT_STATE_COMPLETE_CALLBACK)(TWIN_REPORT_STATE_RESULT result, int status_code, void* context);
	typedef void(*TWIN_STATE_UPDATE_CALLBACK)(TWIN_UPDATE_TYPE update_type, const char* payload, size_t size, void* context);

	typedef struct TWIN_MESSENGER_CONFIG_TAG
	{
		const char* device_id;
		char* iothub_host_fqdn;
		TWIN_MESSENGER_STATE_CHANGED_CALLBACK on_state_changed_callback;
		void* on_state_changed_context;
	} TWIN_MESSENGER_CONFIG;

	MOCKABLE_FUNCTION(, TWIN_MESSENGER_HANDLE, twin_messenger_create, const TWIN_MESSENGER_CONFIG*, messenger_config);
	MOCKABLE_FUNCTION(, int, twin_messenger_report_state_async, TWIN_MESSENGER_HANDLE, messenger_handle, CONSTBUFFER_HANDLE, data, TWIN_MESSENGER_REPORT_STATE_COMPLETE_CALLBACK, on_report_state_complete_callback, const void*, context);
	MOCKABLE_FUNCTION(, int, twin_messenger_subscribe, TWIN_MESSENGER_HANDLE, messenger_handle, TWIN_STATE_UPDATE_CALLBACK, on_twin_state_update_callback, void*, context);
	MOCKABLE_FUNCTION(, int, twin_messenger_unsubscribe, TWIN_MESSENGER_HANDLE, messenger_handle);
	MOCKABLE_FUNCTION(, int, twin_messenger_get_send_status, TWIN_MESSENGER_HANDLE, messenger_handle, TWIN_MESSENGER_SEND_STATUS*, send_status);
	MOCKABLE_FUNCTION(, int, twin_messenger_start, TWIN_MESSENGER_HANDLE, messenger_handle, SESSION_HANDLE, session_handle); 
	MOCKABLE_FUNCTION(, int, twin_messenger_stop, TWIN_MESSENGER_HANDLE, messenger_handle);
	MOCKABLE_FUNCTION(, void, twin_messenger_do_work, TWIN_MESSENGER_HANDLE, messenger_handle);
	MOCKABLE_FUNCTION(, void, twin_messenger_destroy, TWIN_MESSENGER_HANDLE, messenger_handle);
	MOCKABLE_FUNCTION(, int, twin_messenger_set_option, TWIN_MESSENGER_HANDLE, messenger_handle, const char*, name, void*, value);
	MOCKABLE_FUNCTION(, OPTIONHANDLER_HANDLE, twin_messenger_retrieve_options, TWIN_MESSENGER_HANDLE, messenger_handle);

#ifdef __cplusplus
}
#endif

#endif /*IOTHUBTRANSPORT_AMQP_TWIN_MESSENGER*/
