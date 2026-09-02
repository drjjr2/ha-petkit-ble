"""Data update coordinator for Petkit BLE integration."""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.active_update_processor import ActiveBluetoothProcessorCoordinator
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.update_coordinator import UpdateFailed

from .const import DOMAIN, DEFAULT_SCAN_INTERVAL, CONF_ADDRESS, CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
from .ha_bluetooth_adapter import HABluetoothAdapter

# Import the Petkit library modules (included in the integration)
import sys
import os

# Add current directory to path so we can import PetkitW5BLEMQTT
sys.path.insert(0, os.path.dirname(__file__))

from PetkitW5BLEMQTT.device import Device
from PetkitW5BLEMQTT.event_handlers import EventHandlers
from PetkitW5BLEMQTT.commands import Commands
from PetkitW5BLEMQTT.constants import Constants

_LOGGER = logging.getLogger(__name__)

class PetkitBLEData:
    """Data class for Petkit BLE device."""
    
    def __init__(self, device: Device) -> None:
        """Initialize the data."""
        self.device = device
        
    def update(self, service_info: bluetooth.BluetoothServiceInfoBleak) -> None:
        """Update device data from bluetooth service info."""
        # Update RSSI from advertisement
        if hasattr(self.device, '_rssi'):
            self.device.status = {"rssi": service_info.rssi}

class PetkitBLECoordinator(ActiveBluetoothProcessorCoordinator[PetkitBLEData]):
    """Petkit BLE data update coordinator using HA's Bluetooth integration."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the coordinator."""
        self.entry = entry
        self.address = entry.data[CONF_ADDRESS]
        self.update_interval = entry.options.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL)
        
        # Initialize Petkit BLE components with HA Bluetooth adapter
        self.device = Device(self.address)
        self.commands = Commands(ble_manager=None, device=self.device, logger=_LOGGER)
        self.event_handlers = EventHandlers(
            device=self.device, 
            commands=self.commands, 
            logger=_LOGGER
        )
        
        # Use HA Bluetooth adapter instead of direct BLE manager
        self.ble_manager = HABluetoothAdapter(
            hass=hass,
            address=self.address,
            event_handler=self.event_handlers,
            logger=_LOGGER
        )
        
        # Complete the circular references
        self.commands.ble_manager = self.ble_manager
        # Fix missing mac attribute in Commands class
        self.commands.mac = self.address
        
        # Set BLE manager reference in device for connection status access
        self.device.set_ble_manager(self.ble_manager)
        
        # Initialize data processor
        self.data = PetkitBLEData(self.device)
        
        # Define poll method for this instance
        async def _async_poll(service_info: bluetooth.BluetoothServiceInfoBleak) -> PetkitBLEData:
            """Poll the device for updated data."""
            try:
                # Only poll if device is already initialized (prevent duplicate initialization)
                if not self._initialized:
                    _LOGGER.debug("Device not yet initialized, skipping poll")
                    return self.data
                
                # Check if device is still connected before polling
                if not self.ble_manager.connected_devices.get(self.address):
                    _LOGGER.debug("Device not connected during poll, skipping")
                    return self.data
                    
                # Get fresh device data using existing commands with timing
                _LOGGER.debug("Polling device for data updates")
                
                await self.commands.get_battery()
                await asyncio.sleep(0.4)  # Allow time for response
                
                await self.commands.get_device_state()
                await asyncio.sleep(0.4)
                
                await self.commands.get_device_update() 
                await asyncio.sleep(0.6)  # Longer wait for final response
                
                # Update data object
                self.data.update(service_info)

                # Push any newly-learned model/firmware/name to the device
                # registry (no-ops once it's already up to date).
                self._sync_device_registry()

                # Notify listeners of the update
                self.async_update_listeners()

                _LOGGER.debug("Device poll completed")
                return self.data
                
            except Exception as err:
                _LOGGER.debug(f"Error polling device: {err}")
                # Don't raise UpdateFailed - just return existing data
                # This prevents the coordinator from failing completely
                return self.data

        def _needs_poll(service_info: bluetooth.BluetoothServiceInfoBleak, last_poll: float | None) -> bool:
            """Check if we need to poll the device."""
            # Always poll for active data updates
            return True
        
        super().__init__(
            hass,
            _LOGGER,
            address=self.address,
            mode=bluetooth.BluetoothScanningMode.ACTIVE,
            update_method=self.data.update,
            needs_poll_method=_needs_poll,
            poll_method=_async_poll,
            connectable=True,
        )
        
        self._consumer_task = None
        self._initialized = False
        self._listeners: set = set()
        self._initialization_task = None
        
        # Listen for options updates
        self.entry.add_update_listener(self.async_options_updated)

    async def async_start(self) -> None:
        """Start the coordinator and immediately initialize connection."""
        # Start the base coordinator first (not async in ActiveBluetoothProcessorCoordinator)
        super().async_start()
        
        # Immediately attempt device initialization regardless of BT discovery
        if not self._initialized:
            self._initialization_task = asyncio.create_task(self._initialization_loop())
        
    async def _initialization_loop(self) -> None:
        """Continuously attempt device initialization until successful."""
        retry_count = 0
        # No max retries - keep trying indefinitely
        
        while not self._initialized:
            try:
                _LOGGER.info(f"Initialization attempt {retry_count + 1}")
                await self._initialize_device()
                if self._initialized:
                    _LOGGER.info("Device initialization successful")
                    break
            except Exception as err:
                _LOGGER.warning(f"Initialization attempt {retry_count + 1} failed: {err}")
                
            retry_count += 1
            
            # Use immediate retry with minimal delays while there's a real
            # chance this is a quick blip, then keep backing off further the
            # longer it drags on instead of settling on a low ceiling
            # forever. This loop and ha_bluetooth_adapter's own
            # _immediate_reconnection_loop() were both previously capping
            # out at 5s between attempts indefinitely — meaning roughly two
            # real BLE connection attempts against the fountain every 5
            # seconds, forever, for as long as it stayed unreachable.
            # Observed live 2026-09-01: after enough of that, the fountain
            # stopped advertising entirely and needed a manual power cycle
            # to recover — very plausibly this retry pressure wedging its
            # own BLE stack, not anything on the HA/proxy side. Ramping on
            # up to a full minute between attempts once a streak has gone
            # on for a while gives up much less aggressively on the
            # fountain's radio while it's out of range or power-cycling on
            # its own, without meaningfully slowing down recovery from an
            # ordinary short blip.
            if retry_count < 5:
                delay = 0.5  # 500ms for first 5 attempts
            elif retry_count < 10:
                delay = 1.0  # 1 second for next 5 attempts
            elif retry_count < 20:
                delay = 2.0  # 2 seconds for next 10 attempts
            else:
                delay = min(60.0, 5.0 + (retry_count - 20) * 1.0)  # ramp up to 60s

            _LOGGER.debug(f"Waiting {delay}s before next initialization attempt...")
            await asyncio.sleep(delay)

    async def _async_setup(self) -> None:
        """Set up the coordinator during first refresh."""
        await self._initialize_device()

    def _sync_device_registry(self) -> None:
        """Push freshly-learned device info (model/firmware/name) into HA's
        device registry.

        The entity classes' device_info property is only actually consulted
        once, when HA first registers the device — entities are added
        synchronously in async_setup_entry, before the coordinator's own
        connection attempt (a background task) has had any chance to talk
        to the fountain. So the Device Info card is permanently stuck on
        model "Uninitialized" / firmware "Unknown" even hours after the
        real values (serial, firmware, product name) have arrived — nothing
        ever tells the registry to look again after that first pass.

        Call this once real values are known (after a successful data
        refresh) to explicitly push them. Deliberately keyed on the same
        (DOMAIN, address) identifier used at initial registration, and only
        that — never on the entities' own device_info identifiers, which
        switch to the serial once known — so this can only ever update the
        one existing device entry, never create a second one.
        """
        if self.device.serial == "Uninitialized":
            return  # nothing real to report yet

        registry = dr.async_get(self.hass)
        device_entry = registry.async_get_device(identifiers={(DOMAIN, self.address)})
        if device_entry is None:
            return

        model = self.device.product_name or "Water Fountain"
        sw_version = str(self.device.firmware) if self.device.firmware else "Unknown"
        name = (
            self.device.name_readable
            if self.device.name_readable != "Uninitialized"
            else "Water Fountain"
        )

        if (
            device_entry.model == model
            and device_entry.sw_version == sw_version
            and device_entry.name == name
        ):
            return  # already up to date, skip the no-op registry write

        registry.async_update_device(
            device_entry.id,
            model=model,
            sw_version=sw_version,
            name=name,
        )
        _LOGGER.info(
            f"Updated device registry with real device info: "
            f"model='{model}', sw_version='{sw_version}', name='{name}'"
        )

    async def _initialize_device(self) -> None:
        """Initialize the BLE connection and device."""
        try:
            _LOGGER.info(f"Initializing BLE connection to device {self.address}")
            
            # Scan for devices first to populate connectiondata
            _LOGGER.info("Scanning for Petkit devices...")
            await self.ble_manager.scan()
            
            # Connect to the specific device using HA Bluetooth
            _LOGGER.info(f"Attempting to connect to device {self.address}")
            
            # Enable immediate reconnection mode
            if hasattr(self.ble_manager, '_immediate_reconnect'):
                self.ble_manager._immediate_reconnect = True

            # Watch for HA re-discovering this device's advertisements so we can
            # reconnect the instant it's back in range instead of relying purely
            # on polling. Registered *before* the connect attempt below (and
            # unconditionally on every pass through _initialization_loop, not
            # just after a successful connect) — start_advertisement_watch()
            # is idempotent (no-ops if already registered), and if this first
            # connect attempt fails, we still want HA's scanner to nudge a
            # reconnect the moment the device shows back up, instead of
            # sitting on this loop's own up-to-5s polling cadence. This was
            # the gap behind two same-day incidents where the fountain was
            # confirmed back in the Kitchen Panel's nearby-devices list but
            # nothing reconnected until a manual reload — the watch didn't
            # exist yet because no connection had ever succeeded.
            if hasattr(self.ble_manager, "start_advertisement_watch"):
                self.ble_manager.start_advertisement_watch()

            # Registering the watch above can itself race with the connect
            # attempt below: if the device is already advertising, the watch's
            # callback can fire immediately and kick off the adapter's own
            # _immediate_reconnection_loop() in the background — which can win
            # and connect before we get here. If we then call connect_device()
            # unconditionally anyway, bleak/BlueZ can only hold one connection
            # to the peripheral at a time, so this second attempt tears down
            # the perfectly good connection the watch just established, which
            # then immediately fires the disconnect callback and starts the
            # whole reconnect cycle over — observed live: the fountain
            # connected and streamed real data for ~17s via the watch path,
            # then got yanked by this exact race. Skip our own connect_device()
            # call entirely when the address is already in connected_devices —
            # whichever path got there first wins, and this one just proceeds
            # to wire up notifications on the connection that already exists.
            if self.address in self.ble_manager.connected_devices:
                _LOGGER.info(
                    f"{self.address} already connected (advertisement-watch "
                    f"reconnect won the race) — skipping redundant connect_device() call"
                )
            elif not await self.ble_manager.connect_device(self.address):
                raise UpdateFailed(f"Could not connect to device {self.address}")

            # Start message consumer
            _LOGGER.info("Starting message consumer...")
            self._consumer_task = asyncio.create_task(
                self.ble_manager.message_consumer(self.address, Constants.WRITE_UUID)
            )
            
            # Allow BLE stack to stabilize after connection before subscribing to
            # notifications. The fountain's own BLE stack routinely isn't ready to
            # accept a notify-subscribe (CCCD write) the instant the physical link
            # comes up — subscribing immediately loses that race in various ways
            # (GATT error 133/129, "insufficient authorization", flat disconnects)
            # depending on exact timing. This delay must happen BEFORE
            # start_notifications, not after — a post-hoc sleep here does nothing
            # for the subscribe call that already raced and lost.
            _LOGGER.debug("Waiting for BLE stack to stabilize before subscribing...")
            await asyncio.sleep(0.5)

            # Start notifications for device updates
            _LOGGER.info("Starting BLE notifications...")
            if not await self.ble_manager.start_notifications(self.address, Constants.READ_UUID):
                # start_notifications() already retried internally and gave up.
                # A connection with no working notify-subscribe never receives
                # data, so don't let initialization continue and mark this
                # "connected" — that leaves the entity showing connected while
                # actually dead (observed: sensor stuck on "connected" for
                # hours with no data updates). Raise so the outer handler runs
                # cleanup and the normal retry loop gets a real fresh attempt.
                raise UpdateFailed(
                    f"Could not start notifications for {self.address} after connecting"
                )

            # Verify client is actually ready for writes
            client = self.ble_manager.connected_devices.get(self.address)
            if client and hasattr(client, 'is_connected'):
                retry_count = 0
                while not client.is_connected and retry_count < 5:
                    _LOGGER.debug(f"Client not ready, waiting... (attempt {retry_count + 1}/5)")
                    await asyncio.sleep(0.2)
                    retry_count += 1
                    
                if not client.is_connected:
                    raise UpdateFailed("Client not ready after 5 attempts")
                    
                _LOGGER.debug("Client verified ready for communication")
            
            # Initialize device data and connection using existing logic
            # Check if we have connection data before trying to initialize device data
            if self.address in self.ble_manager.connectiondata:
                _LOGGER.info("Using discovered connection data for device initialization")
                self.commands.init_device_data()
            else:
                _LOGGER.warning(f"No connection data for {self.address}, using defaults")
                # Set basic device info manually
                self.device.name = "Petkit Water Fountain"
                self.device.name_readable = "Petkit Water Fountain"  
                self.device.product_name = "Petkit BLE Water Fountain"
                self.device.device_type = 14  # Default device type for W5
                self.device.type_code = 14
            
            _LOGGER.info("Performing minimal device initialization...")
            
            # Instead of full init_device_connection(), do minimal required initialization
            try:
                # Get basic device details first
                _LOGGER.debug("Getting device details...")
                await self.commands.get_device_details()
                await asyncio.sleep(1.0)
                
                # Initialize device if needed
                if not hasattr(self.device, 'device_initialized') or not self.device.device_initialized:
                    _LOGGER.debug("Initializing device...")
                    await self.commands.init_device()
                    await asyncio.sleep(1.5)
                
                # Get basic device info  
                _LOGGER.debug("Getting device info...")
                await self.commands.get_device_info()
                await asyncio.sleep(0.75)
                
                _LOGGER.info("Minimal device initialization completed")
                
            except Exception as init_err:
                _LOGGER.warning(f"Minimal initialization failed: {init_err}")
                # Continue anyway - we'll try to get data without full initialization
            
            # Set basic device information directly since communication is working
            if self.device.serial == "Uninitialized":
                self.device.serial = f"PETKIT_{self.address.replace(':', '')[-6:]}"
                
            if not hasattr(self.device, 'name') or not self.device.name or self.device.name == "Uninitialized":
                self.device.name = f"Water Fountain"
                self.device.name_readable = f"Water Fountain"
            
            # Always ensure we have a proper product name for the device model
            if not hasattr(self.device, 'product_name') or not self.device.product_name or self.device.product_name == "Uninitialized":
                self.device.product_name = "Petkit BLE Water Fountain"
                
            # Set a default firmware version if none received yet
            if not hasattr(self.device, 'firmware') or self.device.firmware == 0:
                self.device.firmware = 1.0  # Default firmware version
            
            _LOGGER.info(f"Set device info: serial='{self.device.serial}', name='{self.device.name_readable}', firmware='{self.device.firmware}'")
            
            # Since we've set the device info directly, mark as initialized immediately
            self._initialized = True
            _LOGGER.info(f"Device initialized successfully: {self.device.serial}")
            
            # Force an update to notify Home Assistant that device is ready
            self.async_update_listeners()
            _LOGGER.info("Notified Home Assistant that device is ready")
            
            # Start regular data polling since ActiveBluetoothProcessorCoordinator might not trigger automatically
            _LOGGER.info("Starting regular data polling...")
            asyncio.create_task(self._start_regular_polling())
            
        except Exception as err:
            import traceback
            _LOGGER.error("Device initialization failed: %s", err)
            _LOGGER.debug("Full traceback:\n%s", traceback.format_exc())
            await self._cleanup()
            # Don't raise here - let the system retry later
            # This prevents the integration from failing completely on startup

    async def async_shutdown(self) -> None:
        """Shutdown the coordinator and cleanup resources.

        Only ever called for a genuine unload/reload or (as of round 16) HA
        stopping/restarting — never from _initialize_device()'s own retry-failure
        path, which calls _cleanup() directly. That means it's safe to disable
        ble_manager's auto-reconnect-on-disconnect behavior here without
        affecting normal retry behavior.

        Round 17 fix: _cleanup() below calls ble_manager.disconnect_device(),
        which calls client.disconnect(). bleak fires the SAME
        disconnected_callback (_on_client_disconnected in
        ha_bluetooth_adapter.py) for that clean, intentional disconnect as it
        does for a real unexpected drop — and that callback unconditionally
        schedules a fresh _immediate_reconnection_loop whenever
        self._immediate_reconnect is on. So round 16's clean shutdown
        disconnect was itself triggering 2-3 brand new BLE connection
        attempts against the fountain in the same ~250ms window, right before
        HA's own shutdown sequence cancelled those tasks too — confirmed live
        via logs from the 2026-09-02 ~08:56 restart test (disconnect at
        08:56:43.519, reconnection attempts #1/#2/#3 at 08:56:43.527-.730,
        "Immediate reconnection loop cancelled" at 08:56:43.774). That burst
        of connect attempts getting cut off mid-flight, right as the proxy's
        own connection to HA is also going away, is very plausibly what
        actually wedged the fountain's BLE stack this time — worse than the
        pre-round-16 behavior of just silently dropping the link once.
        Setting _immediate_reconnect False first means the disconnect callback
        still fires (bookkeeping still gets cleaned up) but no longer spawns a
        new reconnect loop, so the shutdown disconnect stays a single clean
        disconnect and nothing else.
        """
        if hasattr(self.ble_manager, "_immediate_reconnect"):
            self.ble_manager._immediate_reconnect = False
        await self._cleanup()

    async def async_options_updated(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Handle options update."""
        old_interval = self.update_interval
        self.update_interval = entry.options.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL)
        
        if old_interval != self.update_interval:
            _LOGGER.info(f"Update interval changed from {old_interval} to {self.update_interval} seconds")
            # No need to restart the polling task, the change will take effect on the next iteration

    async def _cleanup(self) -> None:
        """Cleanup resources."""
        # Remove options update listener
        try:
            self.entry.async_remove_update_listener(self.async_options_updated)
        except Exception:
            # Listener may not be registered/already removed, or (on newer HA
            # core) ConfigEntry may not expose this method at all — either way
            # this is best-effort cleanup and shouldn't block the rest of
            # teardown or get counted as an initialization failure.
            pass

        if hasattr(self.ble_manager, "stop_advertisement_watch"):
            self.ble_manager.stop_advertisement_watch()

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        
        # _cleanup() is called from two very different places: a real unload/
        # reload (from a different task — safe to cancel _initialization_task
        # here), and _initialize_device()'s own except block on a failed
        # connect attempt, which runs *inside* _initialization_task itself
        # (_initialization_loop -> _initialize_device -> here). In that second
        # case, cancel() schedules a CancelledError against the task that is
        # currently executing this very code — it lands at the next await
        # point (often inside this method's own remaining awaits) and
        # unwinds _initialization_loop() entirely, since CancelledError isn't
        # an Exception subclass and isn't caught by that loop's `except
        # Exception`. The loop's "no max retries — keep trying indefinitely"
        # promise then silently breaks after exactly one failed attempt: the
        # logs show a single "Device initialization failed" error and then
        # total silence until something else (the advertisement watch, or a
        # manual reload_config_entry) rescues it — this is very likely the
        # real explanation behind most of the "power-cycled, showed back up,
        # still wouldn't reconnect without a manual reload" incidents over
        # the past several days, not just the advertisement-watch timing
        # round 8 fixed. Guard against cancelling the task we're already
        # running inside of.
        current_task = asyncio.current_task()
        if (
            self._initialization_task
            and not self._initialization_task.done()
            and self._initialization_task is not current_task
        ):
            self._initialization_task.cancel()
            # Don't await cancelled task, just let it clean up
            try:
                # Give it a brief moment to process the cancellation
                await asyncio.sleep(0.1)
            except Exception as e:
                _LOGGER.debug(f"Error during initialization task cancellation: {e}")
                
        # Stop notifications and disconnect
        if self.address in self.ble_manager.connected_devices:
            await self.ble_manager.stop_notifications(self.address, Constants.READ_UUID)
            await self.ble_manager.disconnect_device(self.address)
            
        self._initialized = False

    async def async_set_device_mode(self, state: int, mode: int) -> None:
        """Set device mode (power and operation mode)."""
        await self.commands.set_device_mode(state, mode)
        
    async def async_reset_filter(self) -> None:
        """Reset the device filter."""
        await self.commands.set_reset_filter()
        
    async def async_set_device_config(self, config_data: list) -> None:
        """Set device configuration."""
        await self.commands.set_device_config(config_data)
    
    async def async_request_refresh(self) -> None:
        """Request a fresh update from the device."""
        if not self._initialized:
            _LOGGER.debug("Device not initialized, skipping refresh request")
            return
            
        try:
            # Check if device is still connected before attempting commands
            if not self.ble_manager.connected_devices.get(self.address):
                _LOGGER.warning("Device not connected during refresh request, triggering immediate reconnection")
                # Push the current (disconnected/reconnecting) status to entities
                # right away instead of leaving them showing stale data. Without
                # this, entities only ever refresh on a *successful* poll/refresh
                # — while the device is down and repeatedly failing to reconnect,
                # nothing calls async_update_listeners() at all, so e.g.
                # sensor.water_fountain_connection can keep showing "connected"
                # for the entire outage (observed: over an hour) even though the
                # underlying connection_status has long since flipped away from
                # CONNECTED. This makes the UI catch up within one poll interval
                # instead of staying frozen until the next successful reconnect.
                self.async_update_listeners()
                # Don't wait for reconnection to complete, just trigger it
                asyncio.create_task(self._attempt_reconnection())
                return
            
            # Get fresh device data using existing commands with delays for BLE stability
            _LOGGER.debug("Requesting device data refresh")
            
            await self.commands.get_battery()
            await asyncio.sleep(0.5)  # Small delay between commands for BLE stability
            
            await self.commands.get_device_state() 
            await asyncio.sleep(0.5)
            
            await self.commands.get_device_update()
            await asyncio.sleep(0.3)
            
            # Allow time for responses to be processed
            await asyncio.sleep(1.0)
            
            # Log current device data for debugging
            _LOGGER.debug(f"Current device status: {self.device.status}")
            _LOGGER.debug(f"Current device config: {self.device.config}")
            _LOGGER.debug(f"Current device info: {self.device.info}")

            # Push any newly-learned model/firmware/name to the device
            # registry (no-ops once it's already up to date).
            self._sync_device_registry()

            # Notify all listeners that data has been updated
            self.async_update_listeners()
            _LOGGER.debug("Device data refresh completed - listeners notified")
            
        except Exception as err:
            _LOGGER.warning("Failed to refresh device data: %s", err)
            # Don't raise the exception - just log the warning
            # This prevents the switch operation from failing completely
    
    async def _attempt_reconnection(self) -> None:
        """Attempt to reconnect to the device."""
        try:
            _LOGGER.info("Attempting immediate reconnection to device")
            
            # Enable immediate reconnection mode in adapter
            if hasattr(self.ble_manager, '_immediate_reconnect'):
                self.ble_manager._immediate_reconnect = True
            
            # Use the immediate reconnection loop. Go through _schedule_reconnect
            # rather than calling _immediate_reconnection_loop directly — it's the
            # single gatekeeper that prevents this from racing a reconnect attempt
            # already in flight from the disconnect callback or advertisement watcher.
            if hasattr(self.ble_manager, '_schedule_reconnect'):
                self.ble_manager._schedule_reconnect(self.address)

                connected_event = getattr(self.ble_manager, '_connected_event', None)
                if connected_event is not None:
                    try:
                        await asyncio.wait_for(connected_event.wait(), timeout=15.0)
                    except asyncio.TimeoutError:
                        pass

                # If reconnected, restart message consumer
                if self.address in self.ble_manager.connected_devices:
                    if self._consumer_task and not self._consumer_task.done():
                        self._consumer_task.cancel()
                    
                    self._consumer_task = asyncio.create_task(
                        self.ble_manager.message_consumer(self.address, Constants.WRITE_UUID)
                    )
                    if await self.ble_manager.start_notifications(self.address, Constants.READ_UUID):
                        _LOGGER.info("Device reconnection successful")
                    else:
                        # Connected but notify-subscribe never came up — not a
                        # usable connection (no data will ever arrive). Don't
                        # report success; tear down and let the reconnection
                        # loop take another real attempt instead of leaving
                        # this looking "connected" while actually dead.
                        _LOGGER.warning(
                            "Device reconnected but notifications failed to start — "
                            "treating as not connected and retrying"
                        )
                        if hasattr(self.ble_manager, "disconnect_device"):
                            await self.ble_manager.disconnect_device(
                                self.address, trigger_reconnect=True
                            )
                else:
                    _LOGGER.warning("Device reconnection in progress...")
            else:
                # Fallback to standard reconnection
                if await self.ble_manager.connect_device(self.address):
                    # Restart message consumer and notifications
                    if self._consumer_task and not self._consumer_task.done():
                        self._consumer_task.cancel()

                    self._consumer_task = asyncio.create_task(
                        self.ble_manager.message_consumer(self.address, Constants.WRITE_UUID)
                    )
                    if await self.ble_manager.start_notifications(self.address, Constants.READ_UUID):
                        _LOGGER.info("Device reconnection successful")
                    else:
                        _LOGGER.warning(
                            "Device reconnected but notifications failed to start — "
                            "treating as not connected and retrying"
                        )
                        if hasattr(self.ble_manager, "disconnect_device"):
                            await self.ble_manager.disconnect_device(
                                self.address, trigger_reconnect=True
                            )
                else:
                    _LOGGER.error("Device reconnection failed")
        except Exception as err:
            _LOGGER.error(f"Error during reconnection attempt: {err}")
        finally:
            # Push whatever the outcome was (reconnected, still reconnecting,
            # or failed) to entities right away rather than waiting for the
            # next poll tick — covers every exit path above, including the
            # exception case.
            self.async_update_listeners()

    def async_add_listener(self, update_callback, context=None) -> callable:
        """Add a listener for data updates."""
        self._listeners.add(update_callback)
        
        def remove_listener():
            self._listeners.discard(update_callback)
        
        return remove_listener

    def async_remove_listener(self, update_callback) -> None:
        """Remove a listener."""
        self._listeners.discard(update_callback)

    def async_update_listeners(self) -> None:
        """Update all listeners."""
        for update_callback in self._listeners:
            update_callback()

    async def _start_regular_polling(self) -> None:
        """Start regular polling loop to fetch device data."""
        poll_interval = self.update_interval
        _LOGGER.info(f"Starting regular polling every {poll_interval} seconds")
        
        while self._initialized:
            try:
                await asyncio.sleep(poll_interval)
                
                if not self._initialized:
                    break
                    
                _LOGGER.debug("Regular poll: requesting device data refresh")
                await self.async_request_refresh()
                
            except asyncio.CancelledError:
                _LOGGER.info("Regular polling cancelled")
                break
            except Exception as err:
                _LOGGER.warning(f"Error in regular polling: {err}")
                # Continue polling even if one cycle fails
                await asyncio.sleep(5)  # Short delay before retry

    @property
    def current_data(self) -> dict[str, Any]:
        """Return the current device data for entities."""
        return {
            "status": self.device.status,
            "config": self.device.config,
            "info": self.device.info,
            "name": self.device.name_readable,
            "product_name": self.device.product_name,
            "firmware": self.device.firmware,
            "serial": self.device.serial,
        }