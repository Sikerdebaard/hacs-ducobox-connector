from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.core import HomeAssistant
from ..const import SCAN_INTERVAL, HTTP_TIMEOUT_BUFFER, EXECUTOR_TIMEOUT_BUFFER, WRITE_TIMEOUT
from .devices import DucoboxSensorEntityDescription, DucoboxNodeSensorEntityDescription
from .utils import safe_get
from typing import Any, Optional
from ducopy import DucoPy
from ducopy.rest.models import ConfigNodeRequest
import logging
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.device_registry import DeviceInfo
import time
import json
import asyncio
from datetime import timedelta
from requests.adapters import HTTPAdapter


_LOGGER = logging.getLogger(__name__)


class TimeoutSSLAdapter(HTTPAdapter):
    """HTTPAdapter with timeout that preserves ducopy's SSL handling.

    This adapter inherits SSL functionality from ducopy's CustomHostNameCheckingAdapter
    while adding timeout support to prevent indefinite hangs.
    """

    def __init__(self, timeout, ssl_context=None, custom_host_mapping=None, *args, **kwargs):
        self.timeout = timeout
        self._ssl_context = ssl_context
        self._custom_host_mapping = custom_host_mapping
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        """Initialize pool manager with custom SSL context if available."""
        if self._ssl_context:
            kwargs['ssl_context'] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)

    def cert_verify(self, conn, url, verify, cert):
        """Disable hostname verification for IP-based connections."""
        if self._custom_host_mapping:
            host = self._custom_host_mapping(url)
            conn.assert_hostname = host or False
        else:
            conn.assert_hostname = False
        return super().cert_verify(conn, url, verify, cert)

    def send(self, request, **kwargs):
        """Send request with default timeout."""
        kwargs.setdefault('timeout', self.timeout)
        return super().send(request, **kwargs)

class DucoboxCoordinator(DataUpdateCoordinator):
    """Coordinator to manage data updates for Ducobox sensors."""

    def __init__(self, hass: HomeAssistant, duco_client: DucoPy):
        super().__init__(
            hass,
            _LOGGER,
            name="Ducobox Connectivity Board",
            update_interval=SCAN_INTERVAL,
        )

        # Add timeout adapter to prevent coordinator stalling
        # The ducopy library doesn't support timeout parameters, so we add it via adapter
        # This preserves ducopy's SSL handling while adding timeout support
        http_timeout_seconds = (SCAN_INTERVAL - HTTP_TIMEOUT_BUFFER).total_seconds()

        try:
            # Check if ducopy has already mounted a custom adapter
            existing_adapter = duco_client.client.session.get_adapter("https://")

            # Create timeout adapter that preserves SSL context if present
            ssl_context = getattr(existing_adapter, '_ssl_context', None)
            custom_host_mapping = getattr(existing_adapter, '_custom_host_mapping', None)

            _LOGGER.info(
                "Configured HTTP timeout: %.0f seconds (scan interval: %.0f seconds)",
                http_timeout_seconds,
                SCAN_INTERVAL.total_seconds()
            )
        except Exception as e:
            _LOGGER.warning(
                "Could not extract SSL context from ducopy adapter: %s. "
                "Falling back to simple timeout adapter.",
                e
            )
            ssl_context = None
            custom_host_mapping = None

        adapter = TimeoutSSLAdapter(
            timeout=http_timeout_seconds,
            ssl_context=ssl_context,
            custom_host_mapping=custom_host_mapping
        )

        duco_client.client.session.mount("http://", adapter)
        duco_client.client.session.mount("https://", adapter)

        self.duco_client = duco_client
        self._static_data = None

        # Timeout for executor jobs - longer than HTTP timeout to allow for retries
        self._executor_timeout = (SCAN_INTERVAL + EXECUTOR_TIMEOUT_BUFFER).total_seconds()
        _LOGGER.debug(
            "Configured executor timeout: %.0f seconds",
            self._executor_timeout
        )

    async def _async_update_data(self) -> dict:
        """Fetch data from the Ducobox API with timeout protection."""
        start_time = time.time()
        try:
            _LOGGER.debug("Starting data fetch cycle")

            # Wrap executor job with timeout to prevent indefinite hangs
            # Timeout covers DNS, TCP, SSL handshake, and HTTP requests
            data = await asyncio.wait_for(
                self.hass.async_add_executor_job(self._fetch_data),
                timeout=self._executor_timeout
            )

            elapsed = time.time() - start_time
            _LOGGER.debug(
                "Data fetch completed successfully in %.2f seconds",
                elapsed
            )
            return data

        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            _LOGGER.error(
                "Timeout fetching data from Ducobox API after %.2f seconds "
                "(timeout: %.0f seconds). Device may be unresponsive. "
                "Check network connectivity and device status.",
                elapsed,
                self._executor_timeout
            )
            raise UpdateFailed(
                f"Timeout fetching data after {elapsed:.0f}s - device unresponsive"
            ) from None

        except Exception as e:
            elapsed = time.time() - start_time
            _LOGGER.error(
                "Failed to fetch data from Ducobox API after %.2f seconds: %s",
                elapsed,
                e,
                exc_info=True  # Include stack trace for debugging
            )
            raise UpdateFailed(f"Failed to fetch data from Ducobox API: {e}") from e

    async def _async_setup(self) -> None:
        """Do initialization logic with timeout protection."""
        try:
            _LOGGER.debug("Fetching static data from /action/nodes")

            self._static_data = await asyncio.wait_for(
                self.hass.async_add_executor_job(self._fetch_once_data),
                timeout=self._executor_timeout
            )

            _LOGGER.debug("Static data fetch completed successfully")

        except asyncio.TimeoutError:
            _LOGGER.error(
                "Timeout fetching static data from /action/nodes after %.0f seconds. "
                "Device may be unresponsive during initialization.",
                self._executor_timeout
            )
            raise UpdateFailed(
                "Timeout during initialization - device unresponsive"
            ) from None

    def _fetch_once_data(self) -> dict:
        """Fetch static data from /action/nodes endpoint."""
        data = {}

        try:
            endpoint_start = time.time()
            _LOGGER.debug("Fetching /action/nodes endpoint (static data)")

            node_actions = self.duco_client.raw_get('/action/nodes')
            data['action_nodes'] = node_actions

            _LOGGER.debug(
                "/action/nodes completed in %.2f seconds",
                time.time() - endpoint_start
            )

        except Exception as e:
            _LOGGER.error(
                "Error fetching static data from /action/nodes: %s",
                e,
                exc_info=True
            )
            raise e

        return data

    def _fetch_data(self) -> dict:
        """Fetch data from Ducobox API endpoints with detailed logging."""
        duco_client = self.duco_client

        if duco_client is None:
            raise Exception("Duco client is not initialized")

        data = {}

        try:
            # Track timing for each endpoint
            endpoint_start = time.time()

            _LOGGER.debug("Fetching /info endpoint")
            data['info'] = duco_client.get_info()
            _LOGGER.debug(
                "/info completed in %.2f seconds",
                time.time() - endpoint_start
            )

            endpoint_start = time.time()
            _LOGGER.debug("Fetching /info/nodes endpoint")
            nodes_response = duco_client.get_nodes()
            node_count = len(nodes_response.Nodes) if nodes_response and hasattr(nodes_response, 'Nodes') else 0
            _LOGGER.debug(
                "/info/nodes completed in %.2f seconds, found %d nodes",
                time.time() - endpoint_start,
                node_count
            )

            if nodes_response and hasattr(nodes_response, 'Nodes'):
                data['nodes'] = [node.dict() for node in nodes_response.Nodes]
            else:
                data['nodes'] = []

            endpoint_start = time.time()
            _LOGGER.debug("Fetching /config/nodes endpoint")
            config_nodes = duco_client.raw_get('/config/nodes')
            data['config_nodes'] = config_nodes
            _LOGGER.debug(
                "/config/nodes completed in %.2f seconds",
                time.time() - endpoint_start
            )

            # Build mappings
            data['mappings'] = {'node_id_to_name': {}, 'node_id_to_type': {}}
            for node in data['nodes']:
                node_id = node.get('Node')
                node_type = safe_get(node, 'General', 'Type', 'Val') or 'Unknown'
                node_name = f"{node_id}:{node_type}"

                data['mappings']['node_id_to_name'][node_id] = node_name
                data['mappings']['node_id_to_type'][node_id] = node_type

            return {**data, **self._static_data}

        except Exception as e:
            _LOGGER.error(
                "Error fetching data from Ducobox API: %s",
                e,
                exc_info=True  # Include stack trace
            )
            raise e

    async def async_set_value(self, node_id, key, value):
        """Send an update to the device with timeout protection."""
        try:
            data = json.dumps({
                key: {'Val': int(round(value, 0))},
            }, separators=(',', ':'))

            _LOGGER.debug(
                "Setting value for node %d, key %s to %s: %s",
                node_id, key, value, data
            )

            # Use shorter timeout for write operations
            await asyncio.wait_for(
                self.hass.async_add_executor_job(
                    self.duco_client.raw_patch, f'/config/nodes/{node_id}', data
                ),
                timeout=WRITE_TIMEOUT
            )

            _LOGGER.info(
                "Successfully set value for node %d, key %s to %s",
                node_id, key, value
            )

        except asyncio.TimeoutError:
            _LOGGER.error(
                "Timeout setting value for node %d, key %s after %.0f seconds",
                node_id, key, WRITE_TIMEOUT
            )
            raise UpdateFailed(
                f"Timeout setting value for node {node_id}"
            ) from None

        except Exception as e:
            _LOGGER.error(
                "Failed to set value for node %d, key %s: %s",
                node_id, key, e,
                exc_info=True
            )
            raise

    async def async_set_ventilation_state(self, node_id, option, action):
        """Set ventilation state with timeout protection."""
        try:
            _LOGGER.debug(
                "Setting ventilation state for node %d, action %s to %s",
                node_id, action, option
            )

            # Use shorter timeout for write operations
            await asyncio.wait_for(
                self.hass.async_add_executor_job(
                    self.duco_client.change_action_node, action, option, node_id
                ),
                timeout=WRITE_TIMEOUT
            )

            _LOGGER.info(
                "Successfully set ventilation state for node %d, action %s to %s",
                node_id, action, option
            )

        except asyncio.TimeoutError:
            _LOGGER.error(
                "Timeout setting ventilation state for node %d after %.0f seconds",
                node_id, WRITE_TIMEOUT
            )
            raise UpdateFailed(
                f"Timeout setting ventilation state for node {node_id}"
            ) from None

        except Exception as e:
            _LOGGER.error(
                "Failed to set ventilation state for node %d, action %s: %s",
                node_id, action, e,
                exc_info=True
            )
            raise

class DucoboxSensorEntity(CoordinatorEntity[DucoboxCoordinator], SensorEntity):
    """Representation of a Ducobox sensor entity."""
    entity_description: DucoboxSensorEntityDescription

    def __init__(
        self,
        coordinator: DucoboxCoordinator,
        description: DucoboxSensorEntityDescription,
        device_info: DeviceInfo,
        unique_id: str,
    ) -> None:
        """Initialize a Ducobox sensor entity."""
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_device_info = device_info
        self._attr_unique_id = unique_id
        self._attr_name = f"{device_info['name']} {description.name}"

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success

    @property
    def native_value(self) -> Any:
        """Return the state of the sensor."""
        try:
            return self.entity_description.value_fn(self.coordinator.data)
        except Exception as e:
            _LOGGER.debug(f"Error getting value for {self.name}: {e}")
            return None

class DucoboxNodeSensorEntity(CoordinatorEntity[DucoboxCoordinator], SensorEntity):
    """Representation of a Ducobox node sensor entity."""
    entity_description: DucoboxNodeSensorEntityDescription

    def __init__(
        self,
        coordinator: DucoboxCoordinator,
        node_id: int,
        description: DucoboxNodeSensorEntityDescription,
        device_info: DeviceInfo,
        unique_id: str,
        device_id: str,
        node_name: str,
    ) -> None:
        """Initialize a Ducobox node sensor entity."""
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_device_info = device_info
        self._attr_unique_id = unique_id
        self._node_id = node_id
        self._attr_name = f"{node_name} {description.name}"

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success

    @property
    def native_value(self) -> Any:
        """Return the state of the sensor."""
        nodes = self.coordinator.data.get('nodes', [])
        for node in nodes:
            if node.get('Node') == self._node_id:
                try:
                    return self.entity_description.value_fn(node)
                except Exception as e:
                    _LOGGER.debug(f"Error getting value for {self.name}: {e}")
                    return None
        return None
