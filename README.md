# hc-zwave

Bridges Z-Wave devices into HomeCore via the zwave-js-server WebSocket API.

Works with [ZwaveJS UI](https://zwave-js.github.io/zwave-js-ui/) or a standalone zwave-js-server instance.

## Supported devices

Z-Wave devices are dynamically discovered and mapped by a built-in translator. Common device classes:

- Lights (dimmers, switches, RGBW)
- Switches and relays
- Door/window sensors
- Motion sensors
- Temperature, humidity, and power sensors
- Locks
- Thermostats
- Garage door controllers
- Meters (energy, water, gas)

Device names sync from ZwaveJS UI node names.

## Setup

1. Copy `config/config.toml.example` to `config/config.toml`
2. Set the `url` to your zwave-js-server WebSocket endpoint (default `ws://localhost:3000`)
3. Add a `[[plugins]]` entry in `homecore.toml`

## Prerequisites

- ZwaveJS UI (or standalone zwave-js-server) running with WebSocket enabled
- Default WebSocket port is 3000 — check ZwaveJS UI Settings > WS Server
