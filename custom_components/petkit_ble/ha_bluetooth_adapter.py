"""Home Assistant Bluetooth adapter for Petkit BLE integration."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from bleak_retry_connector import establish_connection
from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant, callback
from homeassistant.util import dt as dt_util
from homeassistant.helpers.device_registry import format_mac

_LOGGER = logging.getLogger(__name__)

class ConnectionStatus(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"

class HABluetoothAdapter:
    """Adapter to bridge HA's bluetooth integration with existing Petkit BLE library."""
    
    def __init__(self, hass: HomeAssistant, address: str, event_handler=None, logger=None):
        """Initialize the HA Bluetooth adapter."""
        self.hass = hass
        self.address = address
        self.event_handler = event_handler
        self.logger = logger or _LOGGER
        self.connected_devices = {}
        self.available_devices = {}
        self.connectiondata = {}
        self.queue = asyncio.Queue(10)
        self.callback = None
        self.device = False
        self._client = None
        self._ble_device = None
        
        # Connection status tracking (same as BLEManager)
        self._connection_status = ConnectionStatus.DISCONNECTED
        self._last_seen = None
        self._connection_attempts = 0
        self._last_connection_attempt = None
        self._connection_error = None
        self._retry_delay = 0.1  # Start with immediate retry (100ms)
        self._max_retry_delay = 30.0  # Maximum retry delay
        self._max_connection_attempts = 1000  # Very high to allow continuous retries
        self._last_logged_status = None  # Track last logged status to prevent spam
        self._last_reset_time = time.time()
        self._reset_interval = 300.0  # Reset connection attempts every 5 minutes
        self._immediate_reconnect = True  # Flag for immediate reconnection
        self._reconnection_task = None  # Track reconnection task
        self._unsub_bluetooth_callback = None  # HA advertisement-callback unsubscribe handle
        self._connected_event = asyncio.Event()  # Set while a client is connected

    async def scan(self) -> dict[str, Any]:
        """Scan for Petkit BLE devices using HA's bluetooth integration."""
        try:
            # Get discovered devices from HA's bluetooth integration
            discovered_devices = bluetooth.async_discovered_service_info(self.hass)
            
            # Filter for Petkit devices
            petkit_devices = {}
            for service_info in discovered_devices:
                if service_info.name and any(
                    device_type in service_info.name 
                    for device_type in ["W4", "W5", "CTW2"]
                ):
                    # Create a mock device object compatible with existing library
                    # Convert service_data to the format expected by the library
                    # The combine_byte_arrays function expects a dict with .values()
                    service_data_dict = {}
                    if service_info.service_data:
                        service_data_dict = service_info.service_data
                    else:
                        # Default service data with device type identifier for W5
                        service_data_dict = {"default": [0, 0, 0, 0, 0, 206]}  # 206 = W5 device type
                    
                    mock_device = type('MockDevice', (), {
                        'name': service_info.name,
                        'address': service_info.address,
                        'rssi': service_info.rssi,
                        'details': {
                            'props': {
                                'RSSI': service_info.rssi,
                                'ServiceData': service_data_dict
                            }
                        }
                    })()
                    
                    petkit_devices[service_info.address] = mock_device
                    self.connectiondata[service_info.address] = mock_device
                    
            self.available_devices = petkit_devices
            
            for address, device in petkit_devices.items():
                self.logger.info(f"Found HA BLE device: {device.name} ({address})")
                
            return petkit_devices
            
        except Exception as err:
            self.logger.error(f"Error scanning for devices: {err}")
            return {}

    async def connect_device(self, address: str) -> bool:
        """Connect to device using HA's bluetooth integration."""
        try:
            # Update status based on whether this is initial connection or retry
            if self._connection_attempts == 0:
                self._update_connection_status(ConnectionStatus.CONNECTING)
                self.logger.info(f"🔄 Initial BLE connection attempt to {address}")
            else:
                self._update_connection_status(ConnectionStatus.RECONNECTING)
                if self._connection_attempts % 10 == 0:  # Log every 10th attempt
                    self.logger.info(f"🔄 BLE reconnection attempt #{self._connection_attempts} to {address}")
            
            self._last_connection_attempt = time.time()
            
            # Get BLE device from HA's bluetooth integration
            self._ble_device = bluetooth.async_ble_device_from_address(
                self.hass, address, connectable=True
            )
            
            if not self._ble_device:
                error_msg = f"Device {address} not found in HA bluetooth scan"
                self._connection_attempts += 1
                
                if self._connection_attempts % 5 == 0:  # Log every 5th failure
                    self.logger.warning(f"⚠️ Device not found after {self._connection_attempts} attempts, will keep trying...")
                
                if not self._should_attempt_retry():
                    self._update_connection_status(ConnectionStatus.FAILED, error_msg)
                else:
                    self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
                return False
            
            self.logger.debug(f"Device found in scan, establishing BLE connection...")

            # Use bleak-retry-connector directly with the BLE device.
            # - disconnected_callback gives us an immediate, event-driven signal the
            #   instant BlueZ tears down the link, instead of only finding out on the
            #   next write/poll (which could be many seconds later).
            # - use_services_cache=False avoids BlueZ handing back a stale GATT
            #   service/characteristic cache after the fountain reboots or was paired
            #   elsewhere, which otherwise causes writes to silently go nowhere.
            from bleak import BleakClient
            self._client = await establish_connection(
                BleakClient,
                self._ble_device,
                address,
                disconnected_callback=self._on_client_disconnected,
                use_services_cache=False,
                timeout=10.0  # Reduced timeout for faster retries
            )

            self.connected_devices[address] = self._client
            self._update_connection_status(ConnectionStatus.CONNECTED)
            self._update_last_seen()
            
            self.logger.info(f"✅ BLE connection established to {address} after {self._connection_attempts + 1} attempt(s)")
            
            return True
            
        except asyncio.TimeoutError:
            self._connection_attempts += 1
            error_msg = f"Connection timeout (attempt #{self._connection_attempts})"
            
            if self._connection_attempts % 3 == 0:  # Log every 3rd timeout
                self.logger.warning(f"⏱️ Connection timeout after {self._connection_attempts} attempts, continuing...")
            
            if not self._should_attempt_retry():
                self._update_connection_status(ConnectionStatus.FAILED, error_msg)
            else:
                self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
            
            return False
            
        except Exception as err:
            self._connection_attempts += 1
            error_msg = f"Connection attempt {self._connection_attempts} failed: {err}"
            
            if self._connection_attempts % 5 == 0:  # Log every 5th error
                self.logger.warning(f"❌ Connection failed {self._connection_attempts} times: {err}")
            
            if not self._should_attempt_retry():
                self._update_connection_status(ConnectionStatus.FAILED, error_msg)
            else:
                self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
            
            return False

    def _on_client_disconnected(self, client) -> None:
        """Called by bleak the instant BlueZ tears down the connection.

        This runs synchronously on the event loop (not a coroutine), so it must
        not await anything. Its only job is to update our bookkeeping and kick
        off a reconnect immediately, instead of waiting for the next write or
        poll to notice the client has gone away.
        """
        address = self.address
        self.logger.warning(f"🔌 BLE client for {address} disconnected unexpectedly")
        self.connected_devices.pop(address, None)
        self._update_connection_status(ConnectionStatus.RECONNECTING, "Unexpected disconnect")

        if self._immediate_reconnect:
            self._schedule_reconnect(address)

    def _schedule_reconnect(self, address: str) -> None:
        """Ensure exactly one reconnection loop is running for this device.

        Several places can notice a dropped connection at roughly the same time
        (the disconnect callback, a failed write, the advertisement watcher, the
        message consumer). Without a single gatekeeper, each of them used to
        spawn its own `_immediate_reconnection_loop` task, and overlapping
        connection attempts against the same BlueZ device is a good way to make
        a flaky peripheral even flakier. This is the only place that should
        create that task.
        """
        if self._reconnection_task and not self._reconnection_task.done():
            return
        self._reconnection_task = asyncio.create_task(self._immediate_reconnection_loop(address))

    def start_advertisement_watch(self) -> None:
        """React the instant HA's scanner sees this device advertise again.

        Without this, reconnection relies entirely on polling (asking HA "have
        you seen it?" on a timer), which means a real-world reconnect can lag
        behind the device actually being back in range. HA already fires this
        callback the moment a matching advertisement comes in, so use it to
        trigger a reconnect attempt immediately instead of waiting on the poll.
        """
        if self._unsub_bluetooth_callback:
            return

        @callback
        def _on_advertisement(service_info, change) -> None:
            if not self.connected_devices.get(self.address) and self._immediate_reconnect:
                self.logger.debug(
                    f"📡 Advertisement seen for {self.address} while disconnected — triggering reconnect"
                )
                self._schedule_reconnect(self.address)

        self._unsub_bluetooth_callback = bluetooth.async_register_callback(
            self.hass,
            _on_advertisement,
            {"address": self.address},
            bluetooth.BluetoothScanningMode.PASSIVE,
        )

    def stop_advertisement_watch(self) -> None:
        """Unregister the advertisement-watch callback, if one is active."""
        if self._unsub_bluetooth_callback:
            self._unsub_bluetooth_callback()
            self._unsub_bluetooth_callback = None

    async def disconnect_device(self, address: str, trigger_reconnect: bool = False) -> bool:
        """Disconnect from device.
        
        Args:
            address: Device address to disconnect
            trigger_reconnect: If True, immediately attempt reconnection
        """
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                if hasattr(client, 'disconnect'):
                    await client.disconnect()
                del self.connected_devices[address]
                self._update_connection_status(ConnectionStatus.DISCONNECTED)
                
                # Trigger immediate reconnection if requested and enabled
                if trigger_reconnect and self._immediate_reconnect:
                    self.logger.info("Triggering immediate reconnection after disconnect")
                    self._schedule_reconnect(address)
                
                return True
            return False
        except Exception as err:
            error_msg = f"Error disconnecting from {address}: {err}"
            # Force removal from connected devices even if disconnect fails
            if address in self.connected_devices:
                del self.connected_devices[address]
            self._update_connection_status(ConnectionStatus.DISCONNECTED, error_msg)
            
            # Trigger immediate reconnection on unexpected disconnect
            if self._immediate_reconnect:
                self.logger.info("Triggering immediate reconnection after unexpected disconnect")
                self._schedule_reconnect(address)
            
            return False

    async def read_characteristic(self, address: str, characteristic_uuid: str) -> bytes | None:
        """Read characteristic using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                data = await client.read_gatt_char(characteristic_uuid)
                self.logger.info(f"Read data from {characteristic_uuid}: {data}")
                return data
            else:
                self.logger.error(f"Device {address} not connected")
                return None
        except Exception as err:
            self.logger.error(f"Error reading characteristic {characteristic_uuid}: {err}")
            return None

    async def write_characteristic(self, address: str, characteristic_uuid: str, data: bytes) -> bool:
        """Write characteristic using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                # Check if client is still connected before attempting write
                if hasattr(client, 'is_connected') and not client.is_connected:
                    self.logger.warning(f"Client for {address} reports not connected, triggering immediate reconnection...")
                    del self.connected_devices[address]
                    self._update_connection_status(ConnectionStatus.RECONNECTING, "Client disconnected during write")
                    # Trigger immediate reconnection
                    if self._immediate_reconnect:
                        self._schedule_reconnect(address)
                    return False
                    
                await client.write_gatt_char(characteristic_uuid, data)
                self.logger.debug(f"Write complete to {characteristic_uuid}")
                self._update_last_seen()
                return True
            else:
                self.logger.debug(f"Device {address} not connected for write operation")
                # Attempt immediate reconnection if not connected
                if self._immediate_reconnect and self._connection_status != ConnectionStatus.CONNECTING:
                    self._schedule_reconnect(address)
                return False
        except Exception as err:
            error_msg = f"Write failed: {err}"
            self.logger.warning(f"Error writing to characteristic {characteristic_uuid}: {err}")
            # Mark as disconnected so reconnection will be attempted
            if address in self.connected_devices:
                del self.connected_devices[address]
            self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
            # Trigger immediate reconnection
            if self._immediate_reconnect:
                self._schedule_reconnect(address)
            return False

    async def start_notifications(self, address: str, characteristic_uuid: str) -> bool:
        """Start notifications using HA's bluetooth client.

        Retries a couple of times with a short backoff before giving up. The
        settle delay before this is called (see callers) covers the common
        case, but this device occasionally still isn't ready on the first
        try — retrying here in place is much cheaper than tearing down and
        re-establishing the whole connection, which is what happened before
        (every failure here used to propagate up and trigger a full
        reconnect, which is the main source of the connect/disconnect churn).
        """
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                if address not in self.connected_devices:
                    self.logger.error(f"Device {address} not connected")
                    return False

                client = self.connected_devices[address]
                await client.start_notify(characteristic_uuid, self._handle_notification_wrapper)
                self.logger.info(f"Notifications started for {characteristic_uuid}")
                return True
            except Exception as err:
                if attempt < max_attempts:
                    self.logger.debug(
                        f"Notify-subscribe attempt {attempt}/{max_attempts} failed for "
                        f"{characteristic_uuid}: {err} — retrying shortly"
                    )
                    await asyncio.sleep(0.5)
                else:
                    self.logger.error(
                        f"Error starting notifications for {characteristic_uuid} "
                        f"after {max_attempts} attempts: {err}"
                    )
        return False

    async def stop_notifications(self, address: str, characteristic_uuid: str) -> bool:
        """Stop notifications using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                await client.stop_notify(characteristic_uuid)
                self.logger.info(f"Notifications stopped for {characteristic_uuid}")
                return True
            else:
                self.logger.error(f"Device {address} not connected")
                return False
        except Exception as err:
            self.logger.error(f"Error stopping notifications for {characteristic_uuid}: {err}")
            return False

    async def _handle_notification_wrapper(self, sender, data):
        """Wrapper for notification handling."""
        # Update last seen timestamp on successful notification
        self._update_last_seen()
        self.logger.debug(f"📨 Received BLE notification from {sender}: {data.hex() if data else 'None'}")
        if self.event_handler:
            try:
                await self.event_handler.handle_notification(sender, data)
                self.logger.debug("✅ Notification processed successfully")
            except Exception as err:
                self.logger.error(f"❌ Error processing notification: {err}")
        else:
            self.logger.warning("⚠️ No event handler configured for notifications")

    async def heartbeat(self, interval: int) -> None:
        """Heartbeat method compatible with existing library."""
        # This will be called by the existing Commands class
        # The actual heartbeat logic should be handled by HA's coordinator
        pass

    async def message_consumer(self, address: str, characteristic_uuid: str) -> None:
        """Message consumer compatible with existing library."""
        while True:
            try:
                if not self.connected_devices.get(address):
                    # Immediate reconnection if not connected
                    if (self._connection_status not in [ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING] and
                        self._should_attempt_retry()):
                        
                        if self._immediate_reconnect:
                            # Use immediate reconnection loop instead of backoff
                            self._schedule_reconnect(address)
                            await asyncio.sleep(0.5)
                        else:
                            await self._attempt_reconnection_with_backoff(address)
                    elif self._connection_status == ConnectionStatus.FAILED:
                        # Even if failed, keep trying with immediate reconnection
                        if self._immediate_reconnect:
                            self.logger.info("Connection previously failed, retrying immediately...")
                            # Reset attempts to allow more retries
                            self._connection_attempts = 0
                            self._schedule_reconnect(address)
                            await asyncio.sleep(0.5)
                        else:
                            await asyncio.sleep(30)
                    else:
                        # A reconnect attempt is already in flight (scheduled by the
                        # disconnect callback, the advertisement watcher, or a prior
                        # pass through this loop) — just wait for it rather than
                        # spinning a tight poll loop.
                        await asyncio.sleep(0.5)

                    continue
                    
                message = await self.queue.get()
                success = await self.write_characteristic(address, characteristic_uuid, message)
                if success:
                    self._update_last_seen()
                self.queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as err:
                self.logger.error(f"Error in message consumer: {err}")
                if self.queue.qsize() > 0:
                    self.queue.task_done()
                # Use immediate reconnection on error
                if self._immediate_reconnect:
                    self._schedule_reconnect(address)
                    await asyncio.sleep(0.5)
                else:
                    await self._attempt_reconnection_with_backoff(address)
                
    async def _attempt_reconnection_with_backoff(self, address):
        """Attempt reconnection with exponential backoff."""
        if not self._should_attempt_retry():
            return
        
        retry_delay = self._calculate_retry_delay()
        
        # Only log the delay if status changed (prevents spam)
        if self._connection_status != ConnectionStatus.RECONNECTING:
            self.logger.info(f"Waiting {retry_delay:.1f}s before HA BLE reconnection attempt")
        
        await asyncio.sleep(retry_delay)
        
        success = await self.connect_device(address)
        if not success and self._should_attempt_retry():
            # Will be called again by the consumer loop
            pass

    async def message_producer(self, message: bytes) -> None:
        """Add message to queue for processing."""
        await self.queue.put(message)
    
    @property
    def connection_status(self):
        """Get current connection status."""
        return self._connection_status.value
    
    @property
    def last_seen(self):
        """Get timestamp of last successful communication."""
        return self._last_seen
    
    @property
    def connection_attempts(self):
        """Get number of consecutive failed connection attempts."""
        return self._connection_attempts
    
    @property
    def connection_error(self):
        """Get last connection error message."""
        return self._connection_error
    
    def _update_connection_status(self, status, error=None):
        """Update connection status with controlled logging."""
        old_status = self._connection_status
        self._connection_status = status
        
        if error:
            self._connection_error = str(error)
        
        # Keep the connected-event in sync so anything waiting on "are we up yet"
        # (message_consumer, coordinator reconnection helpers) reacts immediately
        # rather than polling.
        if status == ConnectionStatus.CONNECTED:
            self._connected_event.set()
        else:
            self._connected_event.clear()

        # Only log when status actually changes
        if old_status != status or self._last_logged_status != status:
            if status == ConnectionStatus.CONNECTED:
                self.logger.info(f"HA BLE Connection established - Status: {status.value}")
                self._connection_attempts = 0  # Reset on successful connection
                self._retry_delay = 1.0  # Reset retry delay
            elif status == ConnectionStatus.DISCONNECTED:
                self.logger.info(f"HA BLE Connection closed - Status: {status.value}")
            elif status == ConnectionStatus.CONNECTING:
                self.logger.info(f"HA BLE Attempting connection - Status: {status.value}")
            elif status == ConnectionStatus.RECONNECTING:
                self.logger.info(f"HA BLE Reconnecting (attempt {self._connection_attempts + 1}/{self._max_connection_attempts}) - Status: {status.value}")
            elif status == ConnectionStatus.FAILED:
                self.logger.error(f"HA BLE Connection failed after {self._max_connection_attempts} attempts - Status: {status.value}")
                if error:
                    self.logger.error(f"Last error: {error}")
            
            self._last_logged_status = status
    
    def _update_last_seen(self):
        """Update last seen timestamp."""
        self._last_seen = dt_util.now().isoformat()
    
    def _calculate_retry_delay(self):
        """Calculate exponential backoff delay."""
        if self._immediate_reconnect:
            # For immediate reconnection, use minimal delays
            if self._connection_attempts < 5:
                return 0.1  # 100ms for first 5 attempts
            elif self._connection_attempts < 10:
                return 0.5  # 500ms for next 5 attempts
            elif self._connection_attempts < 20:
                return 1.0  # 1 second for next 10 attempts
            else:
                # Gradually increase but stay relatively low
                return min(5.0, 1.0 + (self._connection_attempts - 20) * 0.5)
        else:
            # Original exponential backoff for non-immediate mode
            delay = min(self._retry_delay * (2 ** self._connection_attempts), self._max_retry_delay)
            return delay
    
    async def _immediate_reconnection_loop(self, address: str) -> None:
        """Immediately and continuously attempt to reconnect.

        Only ever entered via `_schedule_reconnect`, which is the single
        gatekeeper that ensures at most one of these runs at a time — don't
        call this directly or duplicate/overlapping reconnect attempts can
        start racing each other against the same BlueZ device.
        """
        while not self.connected_devices.get(address):
            try:
                if not self._should_attempt_retry():
                    # Reset attempts after hitting max to allow continuous retries
                    self.logger.info("Resetting connection attempts for continuous retry")
                    self._connection_attempts = 0
                    self._connection_status = ConnectionStatus.DISCONNECTED
                
                # Only log every few attempts to avoid spam
                if self._connection_attempts % 5 == 0 or self._connection_attempts < 3:
                    self.logger.info(f"🔁 Immediate reconnection attempt #{self._connection_attempts + 1}")
                    
                success = await self.connect_device(address)
                
                if success:
                    self.logger.info(f"✅ Immediate reconnection successful after {self._connection_attempts} attempts!")
                    # Restart notifications after successful reconnection. Give the
                    # BLE stack a moment to settle first — subscribing the instant
                    # the link comes back up is the main reason this loop used to
                    # fire again a few seconds later (see coordinator._initialize_device
                    # for the same fix on the initial-connect path).
                    try:
                        from .PetkitW5BLEMQTT.constants import Constants
                        await asyncio.sleep(0.5)
                        await self.start_notifications(address, Constants.READ_UUID)
                        self.logger.info("📡 Notifications restarted successfully")
                    except Exception as e:
                        self.logger.warning(f"Failed to restart notifications: {e}")
                    break
                else:
                    # Very short delay before next attempt
                    delay = self._calculate_retry_delay()
                    if self._connection_attempts % 10 == 0:  # Log delay every 10 attempts
                        self.logger.debug(f"Reconnection failed, retrying in {delay}s (attempt #{self._connection_attempts})")
                    await asyncio.sleep(delay)
                    
            except asyncio.CancelledError:
                self.logger.info("Immediate reconnection loop cancelled")
                break
            except Exception as err:
                self.logger.error(f"Error in immediate reconnection loop: {err}")
                await asyncio.sleep(0.5)  # Brief pause on error
        
        self._reconnection_task = None
    
    def _should_attempt_retry(self):
        """Check if we should attempt another retry."""
        # Auto-reset connection attempts if enough time has passed
        current_time = time.time()
        if (current_time - self._last_reset_time) >= self._reset_interval:
            if self._connection_attempts >= self._max_connection_attempts:
                self.logger.info(f"Auto-resetting connection attempts after {self._reset_interval}s timeout")
                self._connection_attempts = 0
                self._retry_delay = 1.0
                self._last_reset_time = current_time
                # Only reset to RECONNECTING if we were FAILED, otherwise keep current status
                if self._connection_status == ConnectionStatus.FAILED:
                    self._connection_status = ConnectionStatus.DISCONNECTED
        
        return self._connection_attempts < self._max_connection_attempts
    
    def reset_connection_state(self):
        """Reset connection tracking state for clean restart."""
        self._connection_status = ConnectionStatus.DISCONNECTED
        self._connection_attempts = 0
        self._retry_delay = 1.0
        self._connection_error = None
        self._last_connection_attempt = None
        self._last_logged_status = None
        self._last_reset_time = time.time()