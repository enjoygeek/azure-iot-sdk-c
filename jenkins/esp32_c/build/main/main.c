// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdio.h>

#include "iothub_client_sample_mqtt.h"

#define EXAMPLE_WIFI_SSID "not used for compile test"
#define EXAMPLE_WIFI_PASS "not used"

// The ESP32 toolset is looking for an app_main to generate an executable
void app_main()
{
    // This code will not run and is just here to test compile and link
    iothub_client_sample_mqtt_run();

    printf("goodbye");
}
