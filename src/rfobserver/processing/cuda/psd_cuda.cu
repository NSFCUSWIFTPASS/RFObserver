// GPU implementation of rfobserver.processing.spectral.compute_psd_grid.
//
// Same math as the CPU path: overlapping Hann-windowed segments, batched FFT,
// |X|^2 averaged over the FFTs of each time slice, scaled by window_norm,
// converted to dB with a -200 dB floor, and fftshifted along frequency.
//
// Built by scripts/build_psd_cuda.sh into libpsdcuda.so and loaded through
// ctypes by rfobserver.processing.psd_cuda. One PsdCtx holds the cuFFT plan
// and device buffers for one grid geometry; it is not thread safe, so the
// Python side serializes calls per context.
//
// Input is complex64 (interleaved float32 I/Q). It is either copied from host
// memory (zero_copy = 0) or read in place by the kernels (zero_copy = 1),
// which requires memory from psd_host_alloc (mapped pinned). On Jetson the
// GPU and CPU share DRAM, so the zero-copy path skips the input copy.
#include <cuda_runtime.h>
#include <cufft.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#define THREADS 256

struct PsdCtx {
    int nperseg, hop, fps, n_slices, ass, n_in, total_ffts;
    float window_norm;
    cufftComplex *d_in, *d_win;
    float *d_hann, *d_out;
    cufftHandle plan;
};

static char g_err[256] = "";

static int fail(const char* what, int code) {
    snprintf(g_err, sizeof(g_err), "%s (code %d)", what, code);
    return code ? code : -1;
}

// One block per FFT segment b: gather the overlapping samples, apply Hann.
__global__ void k_extract(const cufftComplex* __restrict__ in, cufftComplex* __restrict__ win,
                          const float* __restrict__ hann, int nperseg, int hop, int fps, int ass) {
    int b = blockIdx.x;
    int s = b / fps, f = b - s * fps;
    const cufftComplex* src = in + (long)s * ass + (long)f * hop;
    cufftComplex* dst = win + (long)b * nperseg;
    for (int k = threadIdx.x; k < nperseg; k += blockDim.x) {
        cufftComplex c = src[k];
        float h = hann[k];
        c.x *= h;
        c.y *= h;
        dst[k] = c;
    }
}

// One block per time slice s: mean |X|^2 over its fps segments, scale, dB, fftshift.
__global__ void k_power_db(const cufftComplex* __restrict__ win, float* __restrict__ out,
                           int fps, int nperseg, float scale /* window_norm / fps */) {
    int s = blockIdx.x;
    const cufftComplex* p = win + (long)s * fps * nperseg;
    float* row = out + (long)s * nperseg;
    int half = nperseg / 2;
    for (int k = threadIdx.x; k < nperseg; k += blockDim.x) {
        float acc = 0.f;
        for (int f = 0; f < fps; ++f) {
            cufftComplex c = p[(long)f * nperseg + k];
            acc += c.x * c.x + c.y * c.y;
        }
        float v = acc * scale;
        int sk = k + half;
        if (sk >= nperseg) sk -= nperseg;
        row[sk] = v > 0.f ? 10.f * log10f(v) : -200.f;
    }
}

extern "C" const char* psd_last_error(void) { return g_err; }

extern "C" int psd_device_count(void) {
    int n = 0;
    if (cudaGetDeviceCount(&n) != cudaSuccess) return 0;
    return n;
}

extern "C" void psd_free(PsdCtx* c) {
    if (!c) return;
    if (c->plan) cufftDestroy(c->plan);
    cudaFree(c->d_in);
    cudaFree(c->d_win);
    cudaFree(c->d_hann);
    cudaFree(c->d_out);
    free(c);
}

// hann: nperseg float32. Returns NULL on failure (see psd_last_error).
extern "C" PsdCtx* psd_init(int nperseg, int hop, int fps, int n_slices, int ass, int n_in,
                            float window_norm, const float* hann) {
    PsdCtx* c = (PsdCtx*)calloc(1, sizeof(PsdCtx));
    if (!c) return NULL;
    c->nperseg = nperseg;
    c->hop = hop;
    c->fps = fps;
    c->n_slices = n_slices;
    c->ass = ass;
    c->n_in = n_in;
    c->total_ffts = n_slices * fps;
    c->window_norm = window_norm;
    size_t n_win = (size_t)c->total_ffts * nperseg;
    size_t n_out = (size_t)n_slices * nperseg;
    cudaError_t e;
    if ((e = cudaMalloc(&c->d_in, (size_t)n_in * sizeof(cufftComplex))) != cudaSuccess ||
        (e = cudaMalloc(&c->d_win, n_win * sizeof(cufftComplex))) != cudaSuccess ||
        (e = cudaMalloc(&c->d_hann, nperseg * sizeof(float))) != cudaSuccess ||
        (e = cudaMalloc(&c->d_out, n_out * sizeof(float))) != cudaSuccess) {
        fail(cudaGetErrorString(e), (int)e);
        psd_free(c);
        return NULL;
    }
    if ((e = cudaMemcpy(c->d_hann, hann, nperseg * sizeof(float), cudaMemcpyHostToDevice)) !=
        cudaSuccess) {
        fail(cudaGetErrorString(e), (int)e);
        psd_free(c);
        return NULL;
    }
    int n[1] = {nperseg};
    cufftResult r = cufftPlanMany(&c->plan, 1, n, NULL, 1, nperseg, NULL, 1, nperseg, CUFFT_C2C,
                                  c->total_ffts);
    if (r != CUFFT_SUCCESS) {
        c->plan = 0;
        fail("cufftPlanMany failed", (int)r);
        psd_free(c);
        return NULL;
    }
    return c;
}

// in: n_in complex64. out: n_slices * nperseg float32 (host). Returns 0 on success.
extern "C" int psd_run(PsdCtx* c, const void* in, int zero_copy, float* out) {
    const cufftComplex* src = (const cufftComplex*)in;
    cudaError_t e;
    if (!zero_copy) {
        e = cudaMemcpy(c->d_in, in, (size_t)c->n_in * sizeof(cufftComplex), cudaMemcpyHostToDevice);
        if (e != cudaSuccess) return fail(cudaGetErrorString(e), (int)e);
        src = c->d_in;
    }
    int tpb = c->nperseg < THREADS ? c->nperseg : THREADS;
    k_extract<<<c->total_ffts, tpb>>>(src, c->d_win, c->d_hann, c->nperseg, c->hop, c->fps,
                                      c->ass);
    if ((e = cudaGetLastError()) != cudaSuccess) return fail(cudaGetErrorString(e), (int)e);
    cufftResult r = cufftExecC2C(c->plan, c->d_win, c->d_win, CUFFT_FORWARD);
    if (r != CUFFT_SUCCESS) return fail("cufftExecC2C failed", (int)r);
    k_power_db<<<c->n_slices, tpb>>>(c->d_win, c->d_out, c->fps, c->nperseg,
                                      c->window_norm / (float)c->fps);
    if ((e = cudaGetLastError()) != cudaSuccess) return fail(cudaGetErrorString(e), (int)e);
    e = cudaMemcpy(out, c->d_out, (size_t)c->n_slices * c->nperseg * sizeof(float),
                   cudaMemcpyDeviceToHost);
    if (e != cudaSuccess) return fail(cudaGetErrorString(e), (int)e);
    return 0;
}

// Mapped pinned host memory the kernels can read in place (zero copy).
extern "C" void* psd_host_alloc(size_t nbytes) {
    void* p = NULL;
    cudaError_t e = cudaHostAlloc(&p, nbytes, cudaHostAllocMapped);
    if (e != cudaSuccess) {
        fail(cudaGetErrorString(e), (int)e);
        return NULL;
    }
    return p;
}

extern "C" void psd_host_free(void* p) {
    if (p) cudaFreeHost(p);
}
