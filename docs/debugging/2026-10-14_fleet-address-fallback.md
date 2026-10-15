# Fleet stuck on a sensor's VPN address after a restart (2026-10-14)

## The question

2026-10-14, branch `feat/fleet-mode` at 04ed277, local fleet (`rfobserver fleet`
on the workstation, 192.168.97.169) with two Jetson sensors found on mDNS.
After `sudo systemctl restart rfobserver` on `ubuntu` (192.168.97.177), the
Sensors page showed `ubuntu` at `10.242.0.103` (its ZeroTier address) instead
of `192.168.97.177`, and it stayed there after the sensor was back up.

## The answer

When the main address stops answering, `SensorHub.refresh` falls back to the
sensor's other mDNS addresses. On success it called
`Registry.add(id, alt_address, ...)`, which replaced the main address but left
`alt_addresses` alone, so the old main address was dropped from the sensor's
list (the saved entry became main `10.242.0.103`, alternatives
`["10.242.0.103"]`). The fleet then had no way back: polls only tried the
addresses on file, and mDNS only re-announces on a change. Fixed in 6b4b27d.

## The procedure that produced it

1. Read `fleet.json` in the fleet's state directory (what the fleet will try):
   `[('ubuntu', '10.242.0.103', ['10.242.0.103'])]`. The LAN address is gone,
   which isolates the fault to the registry update, not to discovery.
2. Checked where the main address can change: `Registry.add` (manual add and
   the fallback in `refresh`) and `Registry.seen_on_mdns`. Only the fallback
   path runs during a sensor restart.
3. Confirmed `seen_on_mdns` would not have repaired it either: it kept the
   current main address whenever mDNS still listed it, and `10.242.0.103` is
   listed.

## The evidence

Before the fix (saved state):

    [('nano-super', '192.168.97.153', []), ('ubuntu', '10.242.0.103', ['10.242.0.103'])]

After the fix, same saved state, fleet restarted:

    [('nano-super', '192.168.97.153', [], True), ('ubuntu', '192.168.97.177', ['10.242.0.103'], True)]

## The fix (6b4b27d)

- `Registry.use_address` swaps the main address and keeps every other one,
  including the old main.
- `refresh` tries an mDNS sensor's addresses in preference order
  (`mdns.rank_addresses`: this host's default-route subnet first), so the poll
  after the preferred address answers again moves back to it. A manually added
  sensor still tries its own address first.
- `seen_on_mdns` gives an mDNS sensor the best-ranked address on every sighting.

Tests: `test_hub_refresh_falls_back_and_returns_to_the_best_address`,
`test_manual_sensor_keeps_its_own_address_first`,
`test_registry_use_address_keeps_every_address`.

## Measured and rejected (do not retry)

None; the first reading of `fleet.json` showed the cause.

## Measurement traps

- The Sensors page showed the bad address but not that the LAN address had
  been dropped. Read `fleet.json` (address and alternatives) instead.

## Open

- mDNS addresses are ranked once per sighting and per poll; a sensor reachable
  only over the VPN from this fleet ranks the VPN address first, which is
  intended, but this was not tested against a real off-subnet sensor.
