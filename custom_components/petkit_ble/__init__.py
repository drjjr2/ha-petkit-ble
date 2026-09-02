"""Petkit BLE Water Fountain integration for Home Assistant."""
from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import Event, HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.update_coordinator import UpdateFailed

from .const import DOMAIN
from .coordinator import PetkitBLECoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.SWITCH, Platform.BINARY_SENSOR]

# Service schemas
SERVICE_RESET_FILTER = "reset_filter"
SERVICE_SET_DEVICE_CONFIG = "set_device_config"

SERVICE_RESET_FILTER_SCHEMA = vol.Schema({})

SERVICE_SET_DEVICE_CONFIG_SCHEMA = vol.Schema({
    vol.Optional("smart_time_on"): cv.positive_int,
    vol.Optional("smart_time_off"): cv.positive_int,
    vol.Optional("led_brightness", default=80): vol.All(vol.Coerce(int), vol.Range(min=0, max=100)),
})

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Petkit BLE from a config entry."""
    coordinator = PetkitBLECoordinator(hass, entry)

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    # Start the coordinator (this replaces async_config_entry_first_refresh for ActiveBluetoothProcessorCoordinator)
    entry.async_create_task(hass, coordinator.async_start())

    # Make sure a live BLE connection gets a clean disconnect before the HA
    # process actually goes away on a plain restart/stop.
    #
    # async_unload_entry() (which calls coordinator.async_shutdown() and,
    # through it, a proper ble_manager.disconnect_device() -> client.disconnect())
    # is NOT called during a normal `homeassistant.restart`/`homeassistant.stop`.
    # Confirmed by reading HA core's HomeAssistant.async_stop() directly: it
    # fires EVENT_HOMEASSISTANT_STOP, cancels all background tasks, fires
    # EVENT_HOMEASSISTANT_FINAL_WRITE/CLOSE, and shuts down — config entries
    # are never unloaded as part of that sequence. Unloading is a separate,
    # explicit operation only triggered by a real entry reload/removal.
    #
    # Without this listener, a live BLE connection to the fountain was simply
    # abandoned mid-restart: the process (and the task holding the BleakClient)
    # just gets cancelled, so no BLE disconnect (LL_TERMINATE_IND) is ever sent
    # to the fountain. The fountain's own BLE stack has no clean signal that
    # its central went away and has to notice via its own supervision timeout
    # — and going by observed behavior, doesn't reliably resume advertising
    # afterward. Observed live 2026-09-02: fountain connected and healthy for
    # 10+ hours, HA restarted for routine updates, and the fountain never
    # became visible to any Bluetooth scanner again until it was manually
    # power-cycled — with both the round-13 connect-lock fix and round-15
    # backoff fix confirmed behaving correctly throughout, ruling out either
    # of those as the cause of this particular incident.
    #
    # Registering via entry.async_on_unload also means this listener is
    # correctly torn down on a real unload/reload, so it can't fire twice or
    # reference a stale coordinator.
    async def _async_handle_ha_stop(event: Event) -> None:
        """Cleanly disconnect from the fountain before HA's process exits."""
        await coordinator.async_shutdown()

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_handle_ha_stop)
    )

    # Register services
    async def handle_reset_filter(call: ServiceCall) -> None:
        """Handle the reset filter service call."""
        await coordinator.async_reset_filter()
        await coordinator.async_request_refresh()

    async def handle_set_device_config(call: ServiceCall) -> None:
        """Handle the set device config service call."""
        # Build config data array based on current device config
        current_config = coordinator.device.config
        
        # Extract parameters with defaults from current config
        smart_time_on = call.data.get("smart_time_on", current_config.get("smart_time_on", 30))
        smart_time_off = call.data.get("smart_time_off", current_config.get("smart_time_off", 60))
        led_brightness = call.data.get("led_brightness", current_config.get("led_brightness", 80))
        
        # Build the configuration array (this matches the device's expected format)
        config_data = [
            smart_time_on,
            smart_time_off,
            current_config.get("led_switch", 1),
            led_brightness,
            current_config.get("led_on_byte1", 0),
            current_config.get("led_on_byte2", 0),
            current_config.get("led_off_byte1", 0),
            current_config.get("led_off_byte2", 0),
            current_config.get("do_not_disturb_switch", 0),
            current_config.get("dnd_on_byte1", 0),
            current_config.get("dnd_on_byte2", 0),
            current_config.get("dnd_off_byte1", 0),
            current_config.get("dnd_off_byte2", 0),
            current_config.get("is_locked", 0)
        ]
        
        await coordinator.async_set_device_config(config_data)
        await coordinator.async_request_refresh()

    hass.services.async_register(
        DOMAIN,
        SERVICE_RESET_FILTER,
        handle_reset_filter,
        schema=SERVICE_RESET_FILTER_SCHEMA,
    )

    hass.services.async_register(
        DOMAIN,
        SERVICE_SET_DEVICE_CONFIG,
        handle_set_device_config,
        schema=SERVICE_SET_DEVICE_CONFIG_SCHEMA,
    )

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()

        # Remove services if no more entries
        if not hass.data[DOMAIN]:
            hass.services.async_remove(DOMAIN, SERVICE_RESET_FILTER)
            hass.services.async_remove(DOMAIN, SERVICE_SET_DEVICE_CONFIG)

    return unload_ok