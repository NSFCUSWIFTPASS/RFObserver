# HCRO sensor time sync: is White Rabbit PTP available on the sensor LAN?

## Question (2026-10-28)

For millisecond-aligned scheduled IQ captures across sensors, how well are the
HCRO sensors' clocks synchronized, and can the White Rabbit (WR) timing gear at
HCRO serve them PTP over the LAN?

Hardware: hcro-jet-001 (10.1.42.31), Jetson, L4T kernel 5.15.148-tegra,
RFObserver 0.12.0b0, USRP on USB, NIC enP8p1s0 (Realtek r8168 8.053.00-NAPI).

## Answer

Not yet. The sensor runs systemd-timesyncd against the public pool
(0.fr.pool.ntp.org, poll 34 min, jitter 2.6 ms): expect a few ms of error per
sensor and more between sensors. No PTP master announces on the sensor LAN: the
only PTP traffic is one device (MAC 64:fb:81:2f:79:6b) sending Delay_Req about
once a second, with a free-running clock (reads July 2024), i.e. a PTP slave
with no master. The radio has no PPS/10 MHz reference ("Setting clock to host
time" at every start).

## Procedure

1. Read-only host report (`timesync_check.sh`, run over ssh with `bash -s`):
   timedatectl, time services, NIC timestamping (`ethtool -T`), rfobserver
   journal lines for the radio clock. Isolates which clock discipline is in use
   and whether PTP hardware timestamping is possible.
2. Listen-only PTP probe (`ptp_probe.py`, run with sudo for 20 to 30 s): opens
   UDP 319/320 with the 224.0.1.129 / 224.0.0.107 groups and an AF_PACKET socket
   for ethertype 0x88F7, sends nothing, decodes the PTP header, and from two-step
   Sync/Follow_Up pairs estimates host clock minus PTP time. Isolates whether a
   master serves this segment, which transport and domain it uses, and how far
   NTP is from it. Both scripts live in the session scratchpad; copy them into
   this repo if they are needed again.

## Evidence

timesync_check.sh on hcro-jet-001:

```
System clock synchronized: yes
NTP service: active
ServerName=0.fr.pool.ntp.org
PollIntervalUSec=34min 8s
NTPMessage={ ... Stratum=1, ... Reference=GPS, ... Jitter=2.634ms }
systemd-timesyncd active; chrony, ntpd, ptp4l, phc2sys, timemaster inactive
--- enP8p1s0
driver: r8168
Capabilities: software-transmit software-receive software-system-clock
PTP Hardware Clock: none
no /dev/ptp* devices
rfobserver.capture.receiver INFO Setting clock to host time   (Oct 22, Oct 27 x2)
```

ptp_probe.py (fixed version), 30 s:

```
L2 frame 60 B: 01 1b 19 00 00 00 64 fb 81 2f 79 6b 88 f7 01 02 00 2c 00 00 ... 64 fb 81 ff fe 2f 79 6b 00 01 e2 4f 01 7f 00 00 66 8e 20 fb ...
L2 64:fb:81:ff:fe:2f:79:6b domain 0 Delay_Req v2: 28
```

Decoded: dst 01:1b:19:00:00:00 (PTP L2 multicast), src 64:fb:81:2f:79:6b,
ethertype 0x88F7, messageType 1 (Delay_Req), PTPv2, length 44, domain 0,
sequence 0xe24f.. (about 58,000 sent), originTimestamp seconds 0x668e20fb
(2024-07-10): the sender's clock was never set by a master. No Announce, Sync or
Follow_Up in 30 s, on L2 or UDP.

## Measured and REJECTED (do not retry)

- "PTP is already on the LAN, just run ptp4l": rejected. 30 s of listening saw
  no Announce/Sync from any master, only one slave's Delay_Req.
- Hardware-timestamped PTP on the Jetson NIC: not available on r8168
  (PTP Hardware Clock: none). PTP here means software timestamps (`ptp4l -S`),
  expected 10 to 100 us.

## Measurement traps

- First probe run decoded L2 frames from byte 0, but AF_PACKET SOCK_RAW frames
  include the 14-byte Ethernet header. The destination MAC 01:1b:... read as
  "messageType 1 (Delay_Req), version 11", clock all zeros. Fixed by skipping
  14 bytes (plus 4 per VLAN tag) and dumping the first frames as hex.
- tcpdump is not installed and sudo needs a password, so the packet capture in
  timesync_check.sh was skipped; the probe replaced it.
- A full `journalctl -u rfobserver` grep takes minutes on these sensors.

## Open

- What device is 64:fb:81:2f:79:6b (the WR node with its LAN port set as a
  slave, or something else)? `ip neigh | grep -i 64:fb:81` on the sensor
  returned nothing: the sensor has never exchanged IP traffic with it (it may
  speak only L2 PTP, or sit on another subnet sharing the segment).
- Decision 2026-10-28: scheduled captures go ahead on NTP; WR/PTP later.
- Can the WR gear be configured as a PTP master (domain 0, L2 or UDP) on the
  sensor port/VLAN? Then re-run the probe for the NTP-vs-PTP offset.
- Only hcro-jet-001 was checked; the other sensors are assumed to match.
