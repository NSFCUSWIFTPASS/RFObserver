"""Section timing of the fused loop to explain the modest speedup."""
import time, numpy as np, scipy.fft, numexpr as ne
from rfobserver.processing.spectral import PSDGridConfig
ne.set_num_threads(ne.detect_number_of_cores())
print("numexpr threads:", ne.nthreads)

SR=26_000_000; rng=np.random.default_rng(0); n=int(SR*0.5)
data=(rng.standard_normal(n)+1j*rng.standard_normal(n)).astype(np.complex64)
cfg=PSDGridConfig(num_bins=256, time_resolution_ms=0.2)
nperseg=cfg.num_bins; hop=int(nperseg*0.5)
slice_samples=int(SR*cfg.time_resolution_ms/1000.0)
fps=max(1,(slice_samples-nperseg)//hop+1)
ass=nperseg+(fps-1)*hop; n_slices=n//ass
hann=np.hanning(nperseg).astype(data.dtype)
slices=data[:n_slices*ass].reshape(n_slices,ass); sr,sc=slices.strides
acc={"win":0.,"fft":0.,"pow":0.,"mean":0.,"db":0.}

def run():
    grid=np.empty((n_slices,nperseg),dtype=np.float32)
    for ci in range(0,n_slices,50):
        ce=min(ci+50,n_slices); ns=ce-ci
        w3d=np.lib.stride_tricks.as_strided(slices[ci:ce],(ns,fps,nperseg),(sr,hop*sc,sc))
        t=time.perf_counter(); flat=ne.evaluate("w3d*hann").reshape(ns*fps,nperseg); acc["win"]+=time.perf_counter()-t
        t=time.perf_counter(); spec=scipy.fft.fft(flat,axis=1,workers=-1); acc["fft"]+=time.perf_counter()-t
        t=time.perf_counter(); psd=ne.evaluate("real(spec)**2+imag(spec)**2"); acc["pow"]+=time.perf_counter()-t
        t=time.perf_counter(); grid[ci:ce]=psd.reshape(ns,fps,nperseg).mean(axis=1); acc["mean"]+=time.perf_counter()-t
    t=time.perf_counter(); ne.evaluate("where(grid>0,10.0*log10(grid),-200.0)",out=grid,casting="unsafe"); acc["db"]+=time.perf_counter()-t
    return grid

run()  # warm
for k in acc: acc[k]=0.
N=10; t0=time.perf_counter()
for _ in range(N): run()
tot=(time.perf_counter()-t0)/N*1000
print(f"total fused: {tot:.1f} ms/call")
for k,v in acc.items(): print(f"  {k:5s}: {v/N*1000:7.1f} ms")

# is numexpr actually threading? compare pow with 1 vs all threads
big=(rng.standard_normal((20000,256))+1j*rng.standard_normal((20000,256))).astype(np.complex64)
for nt in (1, ne.detect_number_of_cores()):
    ne.set_num_threads(nt); ne.evaluate("real(big)**2+imag(big)**2")
    t=time.perf_counter()
    for _ in range(50): ne.evaluate("real(big)**2+imag(big)**2")
    print(f"  numexpr pow {nt} thread(s): {(time.perf_counter()-t)/50*1000:.2f} ms")
