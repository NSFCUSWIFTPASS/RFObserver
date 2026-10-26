# FM demod module: gaps, wrong rate, and a click every chunk

## The question

2026-10-15. "Let's fix the FM module as well, it seems to be broken", then, after the first
fix was deployed: "I've set 103 to 100e6 center, I've added FM demod and set it to 101.5,
there's still clicking period sound" and "I can hear the clicking sound in the audio, drops
come as silence".

Hardware/build: `ubuntu` Jetson (.177, Orin Nano, JetPack 5.1.3, CUDA 11.4, cupy-cuda11x
13.6.0), B200mini at 56 Msps, chunk 2,048,000 samples (36.57 ms), PSD backend cuda.
Listener in a browser over ZeroTier (10.242.0.100 -> http://10.242.0.103/).

Symptoms measured before any fix (module at 922.3 MHz, audio socket read for 15 s from the
workstation): 44,078 audio samples/s delivered against 48,000 declared; message gaps median
35 ms, max 1,996 ms; 362 messages for about 410 chunks.

## The answer

Four separate defects. (1) The demod thread put PCM straight onto an `asyncio.Queue` owned by
the event loop, which is not thread-safe: the audio socket woke only on its 2 s timeout.
(2) Audio was decimated by an integer factor without the low-pass that was computed for it:
50 kHz out, labelled 48 kHz. (3) The box-car decimation (224:1) cut every chunk to a
multiple of 224 and threw away the 192-sample remainder, so the discriminator saw a phase
jump at every chunk boundary: the periodic click, 27 times a second. (4) The browser player
silenced a whole 8192-sample block whenever its buffer was short: the silent drops.

## The procedure

1. Read `modules/fm_demod.py`, `modules/base.py`, `web/routes/modules.py`, the player in
   `dashboard.html`. Listed suspects: queue hand-off across threads, output rate, unused taps,
   int16 wrap, band check.
2. Synthetic station, offline, on .177's GPU (`fm_bench.py` in the session scratchpad): an FM
   carrier 7.3 MHz off centre, 1 kHz tone at 37.5 kHz deviation, fed chunk by chunk to
   `_demod_chunk`. Isolates the DSP from the service, the network and the browser. Reports
   output rate, recovered tone, tone-to-rest SNR, ms per chunk.
3. Live service on .177: create a module over the REST API, read `/api/ws/audio/{id}` for
   15-30 s, record samples/s and message gaps, read the module's status counters. Isolates
   delivery (thread hand-off, queue) from the DSP.
4. After the first fix, the user still heard a periodic click. Added to the synthetic bench a
   least-squares sine fit and the residual near chunk boundaries vs elsewhere. Isolates a
   chunk-periodic artefact from noise.
5. Status counters while the user listened (`chunks_dropped`, `audio_dropped`, sampled every
   10 s, without opening a second audio socket: two readers split one module's queue).
   Separates server-side drops from client-side underruns.

## The evidence

Synthetic bench, before any fix:

    output rate 50012 Hz (declared 48000); tone peak 1000.2 Hz; tone-to-rest SNR 43.5 dB;
    proc ms/chunk med 24.9 max 35.7 (chunk = 36.6 ms)

After the first fix (4efbcb7):

    output rate 47995 Hz; tone-to-rest SNR 51.1 dB; proc ms/chunk med 12.4 max 27.0
    residual rms 0.00206; max |res| near chunk edges 0.0158 vs elsewhere 0.0056

After carrying the decimation remainder:

    output rate 48000 Hz; tone-to-rest SNR 84.1 dB; proc ms/chunk med 13.0 max 23.5
    residual rms 0.00344; max |res| near chunk edges 0.0084 vs elsewhere 0.0084

Live delivery on .177 after the first fix, steady state (30 s, after start-up):

    steady: 47992 samples/s, gap p95 61 max 97 ms; chunks processed 822 dropped 0 in 30s

Status while the user listened (no drops while connected; the 586 predate Play):

    19:09:08 processed 7611 audio_dropped 586
    19:09:18 processed 7884 audio_dropped 586
    19:09:28 processed 8159 audio_dropped 586

## Measured and REJECTED (do not retry)

- "The demod is too slow for real time": rejected. 24.9 ms (before) and 12-13 ms (after) per
  36.6 ms chunk; `chunks_dropped` 0 over 822 chunks live.
- "The server drops audio while the user listens": rejected by the counters above
  (`audio_dropped` flat while connected). Silent drops are client-side underruns.
- "Tone-to-rest SNR shows clicks": it does not. 51.1 dB looked clean while a click sat at
  every chunk boundary; the boundary residual (0.0158 vs 0.0056) is the measure that shows it.

## Measurement traps hit

- The first live run showed 39 dropped chunks and a 1,880 ms gap. Both were start-up: the
  first chunks after a service restart wait about 1.5 s for CuPy to compile kernels. Measure
  after the first audio arrives.
- Reading a module's audio socket while the user listens halves the user's audio (one queue,
  two readers). Use the status counters instead.
- A unit test at 5.6 Msps with a 2-sample remainder did not catch defect (3); it needs a
  remainder comparable to production (20 samples at 5.6 Msps, 192 at 56 Msps), and the
  boundary-vs-elsewhere ratio is swamped there by the rate error, so the test bounds the
  absolute residual instead (0.14 before, 0.0003 after).

## Open, not yet answered

- Whether the remaining silent drops over ZeroTier are network jitter or the dashboard's main
  thread (the ScriptProcessorNode runs on it). The player now shows "Buffer x s, dropouts n"
  and grows the buffer by 200 ms per dropout (up to 2 s); AudioWorklet would move playback off
  the main thread but needs a secure context (HTTPS or localhost).
- One audio socket per module: two listeners split the audio.

## Addendum, same day: the static is the signal, not the demod

Question: "there also some static noise" (after bf19007 deployed). Answer: the station is
too weak at this antenna; more gain does not help.

Procedure: a manual IQ recording on .177 (100 MHz centre, 56 Msps), then on its GPU the
power in each 200 kHz FM channel from the first gap-free 0.5 s, against the median of empty
channels (100.5, 100.7, 100.9, 101.3, 101.7, 102.3 MHz). The empty channels are the noise
reference, so the number is the station's carrier-to-noise in its channel, independent of
gain. Then the same at 60 dB gain, then gain restored to 40.

    gain 40 dB: 101.5 MHz is 5.6 dB above empty channels (101.9: 3.2); ADC clipping 0.000%; rms 0.0129
    gain 60 dB: 101.5 MHz is 4.1 dB above empty channels (101.9: 2.2); ADC clipping 0.000%; rms 0.1079

Broadcast FM needs roughly 10-12 dB carrier-to-noise to demodulate cleanly; below that,
hiss and random "threshold clicks" are inherent. +20 dB of gain raised everything 18.5 dB
and the station's margin fell 1.5 dB: the noise arrives with the signal (antenna and
environment), so the fix is an antenna for 88-108 MHz, not gain or DSP.

Measured and REJECTED (do not retry): RX gain 60 dB (margin 5.6 -> 4.1 dB). Adjacent-channel
leakage through the box-car decimation: the neighbours at 101.3/101.7 are at the noise
floor, so it cannot be the static here (it would matter next to a strong station).

## Addendum, 2026-10-18: faint crackle on a strong station (RTL-SDR)

Question: "I have an FM antenna connected to rtl-sdr and I'm tuned to 101.5, pretty good
quality (SNRs high), but there are still really low volume crackles (barely any)".
Hardware: Nooelec NESDR SMArt v5 on nano-super, 2.4 MS/s at 101.0 MHz, gain 30.

Answer: neighbouring stations folding into the channel. The first stage was a box-car
average straight down to ~267 kHz (/9), which rejects a station 200 kHz away by only about
13 dB; after decimation it lands inside the channel. Replaced by a FIR channel filter
(Kaiser, 60 dB, flat to 100 kHz) before decimating, with a box-car only in front of large
decimations (56 MS/s: /14 box-car to 4 MHz, whose leaks lie MHz away, then /16 FIR).

Procedure: a 4.07 s gap-free IQ capture of the live sensor; the module run offline over it
(`fm_analyze.py`, `fm_ab.py`, `fm_new.py` in the session scratchpad). Checked, in order:
receiver drops and module chunk drops (none), ADC clipping (0.0003%), impulses above
8 sigma in the audio (none), neighbour levels at +/-200, 400 kHz (5-9 dB below 101.5).
Then the same audio through a 129-tap FIR before the module (A/B WAVs): -25.5 dB difference
overall, concentrated in the treble (10-15 kHz, -5.3 dB relative), which is where folded
neighbours and their crackle land. The module with the new filter matches that FIR
prototype to -32 dB.

    synthetic, 2.4 MS/s, neighbour +200 kHz and 10 dB stronger: tone residual rms 0.130 (box-car), < 0.005 (FIR)
    56 MS/s bench on .177: 10.0 ms/chunk before, 11.4 after; nano-super 15.9 / 16.1

Measured and REJECTED (do not retry): dropouts (receiver and module counters flat);
clipping; impulsive interference (nothing above 8 sigma).

Measurement trap: the first FIR version took 21 ms/chunk on .177 (38.9 on nano-super, slower
than real time) although the filter itself was 2.6 ms. CuPy's `reshape(-1, d).mean(axis=1)`
takes 9.6 ms for d = 14 against 1.1 ms for d = 224 on a 2M-sample chunk; a one-thread-per-
output elementwise kernel takes 0.5 ms. cuBLAS (`@` with a weight vector, 0.6 ms) is not
installed on nano-super, so the module does not use it.

Open: whether any crackle remains for the listener after this change (not yet heard).
Multipath on a strong station would need an equaliser (CMA), not a filter.
