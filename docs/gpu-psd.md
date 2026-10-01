# GPU PSD (Jetson)

The PSD grid can run on the Jetson GPU instead of the CPU. On an Orin Nano at
26 Msps with 2048 bins it took 4 ms instead of 41-69 ms of worker time per
39 ms chunk, about 36 ms instead of 61-95 ms for the whole chunk. Select it with
`RFOBS_PSD_BACKEND=cuda` or "PSD Compute" on the Config page; the default is
`cpu`. The result matches the CPU path to float32 rounding.

It needs a small CUDA library built on the Jetson. `deploy/install.sh` builds it
when the CUDA toolkit is present; otherwise install the toolkit and build it by
hand before (re)installing the package:

```bash
sudo apt-get install cuda-nvcc-12-6 cuda-cudart-dev-12-6 libcufft-dev-12-6  # JetPack 6.x
./deploy/build_psd_cuda.sh
sudo pip3 install .
```

Without the library or a CUDA device, a `cuda` setting falls back to the CPU and
logs why once. The change applies when the pipeline reconfigures, like FFT bins.
