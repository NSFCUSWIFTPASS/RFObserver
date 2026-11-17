# rfobs-netwatch

Recovers a sensor whose network interface stops passing traffic while its
link stays up. HCRO .34 did this for 38 h on 2026-11-15 with nothing in the
kernel log, until it was power-cycled
([docs/debugging/2026-11-17_hcro34-network-outage.md](../../docs/debugging/2026-11-17_hcro34-network-outage.md)).

A timer runs `rfobs-netwatch` every minute. While neither the gateway nor
any peer in `NETWATCH_PEERS` answers ping and the link has carrier, it takes
one step per run, each once per episode, timed from the first failed check:

| after | step |
|---|---|
| 5 min | save `ip -s link`, `ethtool -S`, the neighbour table and the PCIe link status to `/var/log/rfobs-netwatch/evidence-<time>.txt`, twice 10 s apart |
| 10 min | take the interface down and up, reconnect it in NetworkManager |
| 20 min | reload the NIC driver (`r8168`) |
| 40 min | reboot, at most once per 6 h |

With no carrier (a cable or switch fault) it only logs and waits. When the
network answers again it logs how long it was out and which steps it took.
Everything goes to the journal: `journalctl -u rfobs-netwatch`.

The evidence file answers the question .34 left open: if the RX/TX counters
still move between the two samples the NIC is working and the fault is on the
switch side; if they are frozen the NIC (or its PCIe link) has stopped.

## Install

As root, from the repo root, on each sensor:

```bash
install -m 755 deploy/netwatch/rfobs-netwatch.sh /usr/local/sbin/rfobs-netwatch
install -m 644 deploy/netwatch/rfobs-netwatch.service deploy/netwatch/rfobs-netwatch.timer /etc/systemd/system/
install -m 644 -D deploy/netwatch/README.md /usr/local/share/doc/rfobs-netwatch/README.md
[ -e /etc/default/rfobs-netwatch ] || install -m 644 deploy/netwatch/rfobs-netwatch.default /etc/default/rfobs-netwatch
systemctl daemon-reload
systemctl enable --now rfobs-netwatch.timer
```

Check it: `systemctl list-timers rfobs-netwatch.timer` and
`journalctl -u rfobs-netwatch` (silent while the network is fine).

## Remove

```bash
systemctl disable --now rfobs-netwatch.timer
rm /etc/systemd/system/rfobs-netwatch.{service,timer} /usr/local/sbin/rfobs-netwatch
systemctl daemon-reload
```
