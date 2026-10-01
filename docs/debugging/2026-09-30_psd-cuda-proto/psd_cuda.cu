// Native CUDA compute_psd_grid: cuFFT batched FFT + fused kernels.
// Mirrors rfobserver.processing.spectral.compute_psd_grid exactly.
#include <cufft.h>
#include <cuda_runtime.h>
#include <math.h>
#include <stdlib.h>

struct PsdCtx {
    int nperseg, hop, fps, n_slices, ass, n_in, total_ffts;
    float window_norm;
    cufftComplex *d_in, *d_win;
    float *d_hann, *d_grid, *d_out;
    cufftHandle plan;
};

// Extract overlapping windows and apply Hann, contiguous for cuFFT.
__global__ void k_extract(const cufftComplex* in, cufftComplex* win, const float* hann,
                          long total, int nperseg, int hop, int fps, int ass) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total) return;
    int k = idx % nperseg;
    long b = idx / nperseg;
    int s = b / fps, f = b % fps;
    long src = (long)s * ass + (long)f * hop + k;
    cufftComplex c = in[src];
    float h = hann[k];
    c.x *= h; c.y *= h;
    win[idx] = c;
}

// Fused |z|^2 and mean over the fps FFTs of each slice.
__global__ void k_power_mean(const cufftComplex* win, float* grid,
                             int n_slices, int fps, int nperseg) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    long total = (long)n_slices * nperseg;
    if (idx >= total) return;
    int s = idx / nperseg, k = idx % nperseg;
    float acc = 0.f;
    for (int f = 0; f < fps; ++f) {
        cufftComplex c = win[((long)(s * fps + f)) * nperseg + k];
        acc += c.x * c.x + c.y * c.y;
    }
    grid[idx] = acc / (float)fps;
}

// window_norm, dB with -200 floor, and fftshift along the freq axis.
__global__ void k_db_shift(const float* grid, float* out, int n_slices, int nperseg, float wn) {
    long idx = (long)blockIdx.x * blockDim.x + threadIdx.x;
    long total = (long)n_slices * nperseg;
    if (idx >= total) return;
    int s = idx / nperseg, k = idx % nperseg;
    float v = grid[idx] * wn;
    float db = v > 0.f ? 10.f * log10f(v) : -200.f;
    int sk = (k + nperseg / 2) % nperseg;
    out[(long)s * nperseg + sk] = db;
}

extern "C" PsdCtx* psd_init(int nperseg, int hop, int fps, int n_slices, int ass,
                            int n_in, float window_norm, const float* hann) {
    PsdCtx* c = (PsdCtx*)calloc(1, sizeof(PsdCtx));
    c->nperseg = nperseg; c->hop = hop; c->fps = fps; c->n_slices = n_slices;
    c->ass = ass; c->n_in = n_in; c->total_ffts = n_slices * fps;
    c->window_norm = window_norm;
    long nwin = (long)c->total_ffts * nperseg;
    if (cudaMalloc(&c->d_in, (long)n_in * sizeof(cufftComplex)) != cudaSuccess) return NULL;
    if (cudaMalloc(&c->d_win, nwin * sizeof(cufftComplex)) != cudaSuccess) return NULL;
    cudaMalloc(&c->d_hann, nperseg * sizeof(float));
    cudaMalloc(&c->d_grid, (long)n_slices * nperseg * sizeof(float));
    cudaMalloc(&c->d_out, (long)n_slices * nperseg * sizeof(float));
    cudaMemcpy(c->d_hann, hann, nperseg * sizeof(float), cudaMemcpyHostToDevice);
    int n[1] = {nperseg};
    if (cufftPlanMany(&c->plan, 1, n, NULL, 1, nperseg, NULL, 1, nperseg,
                      CUFFT_C2C, c->total_ffts) != CUFFT_SUCCESS) return NULL;
    return c;
}

// h_in: interleaved complex64 (re,im). h_out: n_slices*nperseg float32.
extern "C" int psd_run(PsdCtx* c, const float* h_in, float* h_out) {
    cudaMemcpy(c->d_in, h_in, (long)c->n_in * sizeof(cufftComplex), cudaMemcpyHostToDevice);
    long nwin = (long)c->total_ffts * c->nperseg;
    int tpb = 256;
    k_extract<<<(nwin + tpb - 1) / tpb, tpb>>>(c->d_in, c->d_win, c->d_hann, nwin,
                                               c->nperseg, c->hop, c->fps, c->ass);
    if (cufftExecC2C(c->plan, c->d_win, c->d_win, CUFFT_FORWARD) != CUFFT_SUCCESS) return 1;
    long ng = (long)c->n_slices * c->nperseg;
    k_power_mean<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_win, c->d_grid, c->n_slices, c->fps, c->nperseg);
    k_db_shift<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_grid, c->d_out, c->n_slices, c->nperseg, c->window_norm);
    cudaMemcpy(h_out, c->d_out, ng * sizeof(float), cudaMemcpyDeviceToHost);
    return cudaDeviceSynchronize() == cudaSuccess ? 0 : 2;
}

// Same as psd_run but fills t[6] with per-stage ms: H2D, extract, FFT, powmean, dB, D2H.
extern "C" int psd_run_prof(PsdCtx* c, const float* h_in, float* h_out, float* t) {
    cudaEvent_t e[7];
    for (int i = 0; i < 7; ++i) cudaEventCreate(&e[i]);
    long nwin = (long)c->total_ffts * c->nperseg;
    long ng = (long)c->n_slices * c->nperseg;
    int tpb = 256;
    cudaEventRecord(e[0]);
    cudaMemcpy(c->d_in, h_in, (long)c->n_in * sizeof(cufftComplex), cudaMemcpyHostToDevice);
    cudaEventRecord(e[1]);
    k_extract<<<(nwin + tpb - 1) / tpb, tpb>>>(c->d_in, c->d_win, c->d_hann, nwin,
                                               c->nperseg, c->hop, c->fps, c->ass);
    cudaEventRecord(e[2]);
    cufftExecC2C(c->plan, c->d_win, c->d_win, CUFFT_FORWARD);
    cudaEventRecord(e[3]);
    k_power_mean<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_win, c->d_grid, c->n_slices, c->fps, c->nperseg);
    cudaEventRecord(e[4]);
    k_db_shift<<<(ng + tpb - 1) / tpb, tpb>>>(c->d_grid, c->d_out, c->n_slices, c->nperseg, c->window_norm);
    cudaEventRecord(e[5]);
    cudaMemcpy(h_out, c->d_out, ng * sizeof(float), cudaMemcpyDeviceToHost);
    cudaEventRecord(e[6]);
    cudaEventSynchronize(e[6]);
    for (int i = 0; i < 6; ++i) cudaEventElapsedTime(&t[i], e[i], e[i + 1]);
    for (int i = 0; i < 7; ++i) cudaEventDestroy(e[i]);
    return 0;
}

extern "C" void psd_free(PsdCtx* c) {
    if (!c) return;
    cufftDestroy(c->plan);
    cudaFree(c->d_in); cudaFree(c->d_win); cudaFree(c->d_hann);
    cudaFree(c->d_grid); cudaFree(c->d_out);
    free(c);
}
