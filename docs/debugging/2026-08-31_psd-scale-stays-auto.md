# PSD display scale stays on auto after setting Scale Min/Max in config

- **Date:** 2026-08-31
- **Build/host:** RFObserver on the live SDR box at `http://10.1.42.31:8080` (HCRO), deployed HEAD `a35a079`.
- **Symptom:** In config -> Display, setting Scale Min = -65.0 and Scale Max = -100.0
  had no effect; the live dashboard spectrum/waterfall kept auto-scaling.

## The answer

The two values were **inverted**. -100 dB is lower than -65 dB, so the user set
Min ABOVE Max. The dashboard has a guard that discards a degenerate pinned range
(`dynamicMin >= dynamicMax`) and reverts to auto-scaling. It was working exactly
as written; the input was upside down. Correct entry is Min = -100 (floor),
Max = -65 (ceiling).

The guard did this **silently**, which is why it read as "the setting is ignored"
rather than "the setting was rejected." Fix shipped: reject an inverted range at
`/config/apply` (HTTP 400) and warn inline on the config page, refusing to save
until Min < Max.

## The procedure that produced it

Each probe controlled for one hypothesis, in order:

1. **Is the scale feature even deployed?** `git merge-base --is-ancestor cc99ce7 a35a079`
   -> yes. The pinning JS (`scaleMinDb`/`scaleMaxDb`) is in the running code.
   Rules out "old build without the feature."
2. **Does the deployed code actually pin?** Reproduced the WS payload + dashboard
   render with the exact code: setting -90 pins legend/colors to -90/-30. So the
   mechanism works when given a valid range. Isolates the bug to input, not logic.
3. **Is it stale browser JS / caching?** Checked for `Cache-Control` on the
   dashboard route: none. Advised hard reload + clear cache. User did both;
   symptom persisted. Rules out the client-cache hypothesis.
4. **What are the actual values?** User reported Min = -65.0, Max = -100.0.
   Read the guard (`dashboard.html:939-942`): `if (dynamicMin >= dynamicMax)` ->
   fall back to `emaMin/emaMax` (auto). `-65 >= -100` is true. Root cause.

The "control" throughout was step 2: proving the same code pins correctly with a
valid range meant the fault had to be in the input or the WS transport, not the
render path.

## Evidence

Guard, `src/rfobserver/web/templates/dashboard.html:937-943`:

```js
dynamicMin = (scaleMinDb != null) ? scaleMinDb : emaMin;
dynamicMax = (scaleMaxDb != null) ? scaleMaxDb : emaMax;
if (dynamicMin >= dynamicMax) {
    // Degenerate pinned range - ignore the pins rather than divide by zero
    dynamicMin = emaMin;
    dynamicMax = emaMax;
}
```

Post-fix browser verification against a local mock server:

```
inverted submit (-65/-100): posted=false, inline error shown (display:block)
valid submit   (-100/-65):  posted=true, server stored min=-100.0 max=-65.0
```

## Measured and REJECTED (do not retry)

- **Stale browser JavaScript.** Plausible because the dashboard route sets no
  cache headers, but a hard reload + cache clear did NOT fix it. The tab was
  running current code the whole time.
- **Missing/undeployed scale feature.** `cc99ce7` is an ancestor of the deployed
  `a35a079`; the feature was present.
- **Adding `Cache-Control: no-cache` to the dashboard route.** Considered as the
  fix while the stale-JS theory was live. It was never the cause, so not done.

## Measurement traps hit

- The stale-JS theory was self-reinforcing: no cache headers on the route made it
  look likely, and "did you hard reload?" is a cheap first ask. It cost a round
  trip. The trap was theorizing about the client before asking for the exact
  values the user had entered - which settled it in one step.
- Inverted dB values are easy to enter because "Max" intuitively reads as "the
  big number" while in dBFS the ceiling is the value closest to zero (least
  negative). The help text now spells out "Min is the lower dB, Max the higher."

## Fix

- `src/rfobserver/web/routes/config.py`: reject `PSD_SCALE_MIN_DB >= PSD_SCALE_MAX_DB`
  with HTTP 400 before any mutation, resolving each bound to its prospective value
  (submitted value, else the stored one; a cleared bound is None/auto and skips
  the check).
- `src/rfobserver/web/templates/config.html`: inline error under the Display card,
  live on input and on submit; submit is blocked (no POST) while inverted.
- Tests: `tests/unit/test_web_routes.py::TestConfigApply` (inverted/equal rejected,
  valid accepted, validation against a stored bound, clearing a bound skips check).

## Open, not yet answered

- None for this symptom. The guard's silent fallback is now surfaced to the user.
