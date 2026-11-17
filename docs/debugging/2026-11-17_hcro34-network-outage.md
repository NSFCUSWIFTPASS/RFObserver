# HCRO .34 unreachable for 38 h: the NIC stopped passing traffic with the link up

## The question

2026-11-17. ".34 is back, figure out what happened and why it went down." hcro-jet-004
(rf-nano-cc, 10.1.42.34), Jetson Orin Nano Super, JetPack L4T 5.15.199-tegra, Realtek NIC on PCIe
(`enP8p1s0`, vendor driver `r8168` 8.053.00-NAPI), RFObserver 0.14.0b0. Unreachable from
2026-11-16 18:24 UTC (first noticed: no ping, no SSH, no ARP reply even from .31 on the same LAN)
until it was power-cycled at 2026-11-17 11:58 MST.

## The answer

.34 did not go down. It kept running and recording the whole time; its Ethernet interface stopped
passing traffic in both directions at 21:33 MST on 2026-11-15 while the link stayed up, and nothing
was logged by the kernel or the driver. With no traffic, its 02:04 DHCP renewal failed silently and
the 24 h lease expired at 14:04 on 2026-11-16; NetworkManager then retried DHCP every few minutes
for 22 h without an answer. The power cycle at 11:58 MST on 2026-11-17 brought it back. Whether the
NIC or the switch port wedged is not determined; the other five sensors on the same network were
reachable throughout.

## Procedure

1. **Reboot or not** (`uptime -s`, `last -x`, `journalctl --list-boots`): booted 11:59 today. `last`
   and the kernel log say "Nov 15 21:10" for that boot because the Jetson has no battery-backed
   clock: it resumes from the time systemd-timesyncd last saved, then NTP corrects it. That
   timestamp is not the outage time.
2. **What ran up to the reboot**: the boot marker located from the end of `/var/log/syslog`
   (`tail -c 400M | grep -m`, not a full-file grep, which evicts the page cache on a field sensor).
   The line before it is rfobserver at 11:58:45 today, cut off mid-write: the system was running
   until a sudden power cut.
3. **Network events** (NetworkManager lines in the same window): from 2026-11-16 14:05 to the
   reboot, every attempt ends `ip-config -> failed (reason 'ip-config-unavailable')` after the 45 s
   DHCP timeout. 658 failures.
4. **DHCP history**: leases were renewed every 12 h at 02:04 and 14:04 (24 h lease). The last one
   is 2026-11-15 14:04:41; the 2026-11-16 02:04 renewal is missing; at 14:04:41 the lease expired
   (`state changed no lease`).
5. **When traffic stopped**: last inbound request (fleet hub `GET /api/identity` from .31)
   2026-11-15 21:33:27; last outbound success (OpenZMS `201 Created`) 21:33:46. At 21:38:58
   NetworkManager dropped to `CONNECTED_SITE` (its connectivity check failed); NTP timed out from
   21:45. Inbound and outbound stopped together.
6. **Kernel and driver**: no `NETDEV WATCHDOG`, no `r8168` message, no link down/up, no AER or
   PCIe error between boot and the power cut. The only recurring kernel message is
   `nvme nvme0: I/O ... timeout, completion polled` (NVMe on the same PCIe complex).

## Evidence

```
Nov 15 14:04:41 NetworkManager dhcp4 (enP8p1s0): state changed new lease, address=10.1.42.34
   (no Nov 16 02:04 renewal)
Nov 15 21:33:27 last GET /api/identity from 10.1.42.31
Nov 15 21:33:46 last OpenZMS POST 201 Created
Nov 15 21:38:58 NetworkManager state is now CONNECTED_SITE
Nov 15 21:45:23 systemd-timesyncd: Timed out waiting for reply from 10.1.11.15:123
Nov 16 14:04:41 dhcp4 (enP8p1s0): state changed no lease
Nov 16 14:05:26 device (enP8p1s0): activated -> failed (reason 'ip-config-unavailable')
   ... 658 DHCP failures ...
Nov 17 11:58:45 rfobserver[1564173]: ... (line truncated: power cut)
Nov 17 11:59 boot
```

NVMe "completion polled" timeouts in kern.log: .34 2-47 per day (Nov 11-17); .31 146, .33 1040,
.36 33 in total. All sensors use `r8168`.

After the reboot: health ok, rf-db backlog 0 (136,216 records sent in the first 38 min: the outage
period was resent from the bookmark).

## Measured and REJECTED (do not retry)

- **Sensor powered off or crashed**: syslog runs continuously up to 11:58:45 on 2026-11-17.
- **Network-wide outage**: .31, .32, .33, .35 and .36 were reachable over the same network during
  the outage (all queried 2026-11-16).
- **Link down / cable unplugged**: NetworkManager never logged a carrier change; the device stayed
  in its connection cycle with the link up.
- **Lost DHCP lease as the cause**: the lease was lost, but 16.5 h after traffic stopped; .34 was
  already unreachable on 2026-11-16 at 11:24 MST with its lease still valid until 14:04.
- **IP address conflict**: no conflict or duplicate-address message in the log (NetworkManager's
  IPv4 DAD is off, `ipv4.dad-timeout -1`, so a conflict would not have been logged either). Against
  it: .31 got no ARP reply at all for 10.1.42.34 on 2026-11-16 (a device holding the address would
  have answered), and after the lease expired 658 DHCP discovers, which are broadcasts that do not
  depend on the IP, got no offer in 22 h. A brief conflict at 21:33 that triggered the wedge cannot
  be excluded.

## Measurement traps

- The boot shows as "Nov 15 21:10" in `last`, `journalctl --list-boots` and kern.log (saved clock,
  no RTC). The real boot time is from `uptime -s` after NTP sync.
- Files in `/dev/shm` written over SSH vanish when the session ends (logind `RemoveIPC`); use
  `/tmp` for scratch on the sensors.

## Open, not yet answered

- NIC (r8168 / its PCIe link) or switch port: not distinguishable from the sensor's logs, and the
  interface counters were reset by the power cycle. The router's DHCP log (10.1.42.1) would tell:
  .34's discovers arriving there means .34 could send but not receive. .34 runs
  prometheus-node-exporter, so a site Prometheus may hold its interface counters up to 21:33. If it recurs, before power-cycling: read
  `ip -s link show enP8p1s0` (do RX and TX counters move?), `ethtool -S enP8p1s0`, and try
  `ip link set enP8p1s0 down/up` from the console; ask whether the switch shows the port's MAC.
- Whether this is related to the NVMe completion-polled timeouts (missed interrupts on the same
  PCIe complex).
- Whether .34 had shorter episodes before (a full-syslog search for missed lease renewals was not
  run, to avoid evicting the DB from the page cache).
- No automatic recovery was installed at the time. `deploy/netwatch/` (rfobs-netwatch) now does
  it when installed: saves the counters after 5 min with the link up and no traffic, then bounces
  the interface, reloads r8168, and as a last step reboots at most once per 6 h.
