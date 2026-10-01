// CUDA compute_psd_grid v2: selectable input memory mode + optimized kernels.
//
// Input modes (where the complex64 IQ lives when psd_run2 is called):
//   0  pageable host (numpy)  -> cudaMemcpy H2D into device buffer   (v1 behaviour)
//   1  pinned host (staging)  -> cudaMemcpy H2D into device buffer
//   2  pinned mapped host     -> kernels read it directly (zero-copy), output zero-copy
//   3  managed (unified)      -> kernels read it directly, output managed
// Kernel sets:
//   opt=0  v1 generic kernels (per-element runtime div/mod), separate dB pass
//   opt=1  block-per-window extract, block-per-slice fused power+mean+dB+shift
#include <cufft.h>
#include <cuda_runtime.h>
#include <math.h>
#include <stdlib.h>

struct PsdCtx {
    int nperseg, hop, fps, n_slices, ass, n_in, total_ffts;
    float window_norm;
    cufftComplex *d_in, *d_win;
    float *d_hann, *d_grid, *d_out;
    cufftComplex *h_pin, *h_pin_dev;   // pinned mapped input
    float *h_opin, *h_opin_dev;        // pinned mapped output
    cufftComplex *m_in;                // managed input
    float *m_out;                      // managed output
    cufftHandle plan;
};

// ---------- v1 generic kernels ----------
__global__ void k_extract(const cufftComplex* in, cufftComplex* win, const float* hann,
                          long total, int nperseg, int hop, int fps, int ass) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total) return;
    int k = idx % nperseg;
    long b = idx / nperseg;
    int s = b / fps, f = b % fps;
    cufftComplex c = in[(long)s * ass + (long)f * hop + k];
    float h = hann[k];
    c.x *= h; c.y *= h;
    win[idx] = c;
}
__global__ void k_power_mean(const cufftComplex* win, float* grid, int n_slices, int fps, int nperseg) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= (long)n_slices * nperseg) return;
    int s = idx / nperseg, k = idx % nperseg;
    float acc = 0.f;
    for (int f = 0; f < fps; ++f) {
        cufftComplex c = win[((long)(s * fps + f)) * nperseg + k];
        acc += c.x * c.x + c.y * c.y;
    }
    grid[idx] = acc / (float)fps;
}
__global__ void k_db_shift(const float* grid, float* out, int n_slices, int nperseg, float wn) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= (long)n_slices * nperseg) return;
    int s = idx / nperseg, k = idx % nperseg;
    float v = grid[idx] * wn;
    out[(long)s * nperseg + (k + nperseg / 2) % nperseg] = v > 0.f ? 10.f * log10f(v) : -200.f;
}

// ---------- v2 optimized kernels ----------
// One block per window, thread = bin. No per-element division.
__global__ void k_extract_blk(const cufftComplex* __restrict__ in, cufftComplex* __restrict__ win,
                              const float* __restrict__ hann, int nperseg, int hop, int fps, int ass) {
    int b = blockIdx.x, k = threadIdx.x;
    int s = b / fps, f = b - s * fps;          // once per thread, scalar-uniform per block
    cufftComplex c = in[(long)s * ass + (long)f * hop + k];
    float h = hann[k];
    c.x *= h; c.y *= h;
    win[(long)b * nperseg + k] = c;
}
// One block per slice: |z|^2, mean, window_norm, dB, fftshift in one pass.
__global__ void k_pmdb_blk(const cufftComplex* __restrict__ win, float* __restrict__ out,
                           int fps, int nperseg, float scale /* window_norm / fps */) {
    int s = blockIdx.x, k = threadIdx.x;
    const cufftComplex* p = win + (long)s * fps * nperseg + k;
    float acc = 0.f;
    for (int f = 0; f < fps; ++f) {
        cufftComplex c = p[(long)f * nperseg];
        acc += c.x * c.x + c.y * c.y;
    }
    float v = acc * scale;
    int sk = k + nperseg / 2;
    if (sk >= nperseg) sk -= nperseg;
    out[(long)s * nperseg + sk] = v > 0.f ? 10.f * log10f(v) : -200.f;
}

extern "C" PsdCtx* psd_init(int nperseg, int hop, int fps, int n_slices, int ass,
                            int n_in, float window_norm, const float* hann) {
    PsdCtx* c = (PsdCtx*)calloc(1, sizeof(PsdCtx));
    c->nperseg = nperseg; c->hop = hop; c->fps = fps; c->n_slices = n_slices;
    c->ass = ass; c->n_in = n_in; c->total_ffts = n_slices * fps;
    c->window_norm = window_norm;
    long nwin = (long)c->total_ffts * nperseg;
    long ng = (long)n_slices * nperseg;
    size_t in_b = (size_t)n_in * sizeof(cufftComplex), out_b = (size_t)ng * sizeof(float);
    if (cudaMalloc(&c->d_in, in_b) != cudaSuccess) return NULL;
    if (cudaMalloc(&c->d_win, nwin * sizeof(cufftComplex)) != cudaSuccess) return NULL;
    cudaMalloc(&c->d_hann, nperseg * sizeof(float));
    cudaMalloc(&c->d_grid, out_b);
    cudaMalloc(&c->d_out, out_b);
    cudaMemcpy(c->d_hann, hann, nperseg * sizeof(float), cudaMemcpyHostToDevice);
    if (cudaHostAlloc(&c->h_pin, in_b, cudaHostAllocMapped) != cudaSuccess) return NULL;
    cudaHostGetDevicePointer((void**)&c->h_pin_dev, c->h_pin, 0);
    if (cudaHostAlloc(&c->h_opin, out_b, cudaHostAllocMapped) != cudaSuccess) return NULL;
    cudaHostGetDevicePointer((void**)&c->h_opin_dev, c->h_opin, 0);
    if (cudaMallocManaged(&c->m_in, in_b) != cudaSuccess) return NULL;
    if (cudaMallocManaged(&c->m_out, out_b) != cudaSuccess) return NULL;
    int n[1] = {nperseg};
    if (cufftPlanMany(&c->plan, 1, n, NULL, 1, nperseg, NULL, 1, nperseg,
                      CUFFT_C2C, c->total_ffts) != CUFFT_SUCCESS) return NULL;
    return c;
}

// Host pointers to the GPU-visible buffers so Python can write/read them in place.
// which: 1 pinned in, 2 pinned out, 3 managed in, 4 managed out
extern "C" void* psd_buffer(PsdCtx* c, int which) {
    switch (which) {
        case 1: return c->h_pin;
        case 2: return c->h_opin;
        case 3: return c->m_in;
        case 4: return c->m_out;
    }
    return NULL;
}

// t[6] (ms): input stage, extract, FFT, power+mean, dB, output stage.
extern "C" int psd_run2(PsdCtx* c, int mode, int opt, const float* h_in, float* h_out, float* t) {
    cudaEvent_t e[7];
    for (int i = 0; i < 7; ++i) cudaEventCreate(&e[i]);
    long nwin = (long)c->total_ffts * c->nperseg;
    long ng = (long)c->n_slices * c->nperseg;
    size_t in_b = (size_t)c->n_in * sizeof(cufftComplex);
    int tpb = 256;
    const cufftComplex* src;
    float* dst;  // where the final dB grid is written
    cudaEventRecord(e[0]);
    switch (mode) {
        case 0: cudaMemcpy(c->d_in, h_in, in_b, cudaMemcpyHostToDevice); src = c->d_in; dst = c->d_out; break;
        case 1: cudaMemcpy(c->d_in, c->h_pin, in_b, cudaMemcpyHostToDevice); src = c->d_in; dst = c->d_out; break;
        case 2: src = c->h_pin_dev; dst = c->h_opin_dev; break;
        default: src = c->m_in; dst = c->m_out; break;
    }
    cudaEventRecord(e[1]);
    bool blk = opt && c->nperseg <= 1024;
    if (blk)
        k_extract_blk<<<c->total_ffts, c->nperseg>>>(src, c->d_win, c->d_hann, c->nperseg, c->hop, c->fps, c->ass);
    else
        k_extract<<<(nwin + tpb - 1) / tpb, tpb>>>(src, c->d_win, c->d_hann, nwin, c->nperseg, c->hop, c->fps, c->ass);
    cudaEventRecord(e[2]);
    if (cufftExecC2C(c->plan, c->d_win, c->d_win, CUFFT_FORWARD) != CUFFT_SUCCESS) return 1;
    cudaEventRecord(e[3]);
    if (blk) {
        k_pmdb_blk<<<c->n_slices, c->nperseg>>>(c->d_win, dst, c->fps, c->nperseg, c->window_norm / (float)c->fps);
        cudaEventRecord(e[4]);
        cudaEventRecord(e[5]);
    } else {
        k_power_mean<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_win, c->d_grid, c->n_slices, c->fps, c->nperseg);
        cudaEventRecord(e[4]);
        k_db_shift<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_grid, dst, c->n_slices, c->nperseg, c->window_norm);
        cudaEventRecord(e[5]);
    }
    if (mode <= 1) cudaMemcpy(h_out, c->d_out, ng * sizeof(float), cudaMemcpyDeviceToHost);
    cudaEventRecord(e[6]);
    cudaEventSynchronize(e[6]);
    for (int i = 0; i < 6; ++i) cudaEventElapsedTime(&t[i], e[i], e[i + 1]);
    for (int i = 0; i < 7; ++i) cudaEventDestroy(e[i]);
    return cudaGetLastError() == cudaSuccess ? 0 : 2;
}

extern "C" void psd_free(PsdCtx* c) {
    if (!c) return;
    cufftDestroy(c->plan);
    cudaFree(c->d_in); cudaFree(c->d_win); cudaFree(c->d_hann);
    cudaFree(c->d_grid); cudaFree(c->d_out);
    cudaFreeHost(c->h_pin); cudaFreeHost(c->h_opin);
    cudaFree(c->m_in); cudaFree(c->m_out);
    free(c);
}
