/**
 * AudioWorkletProcessor for streaming PCM playback.
 * Receives Int16 PCM samples via the message port and outputs Float32.
 */
class PCMPlayerProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.buffer = new Float32Array(0);
        this.port.onmessage = (e) => {
            // Convert Int16 PCM to Float32 [-1, 1]
            const int16 = new Int16Array(e.data);
            const float32 = new Float32Array(int16.length);
            for (let i = 0; i < int16.length; i++) {
                float32[i] = int16[i] / 32768.0;
            }
            // Append to ring buffer
            const newBuf = new Float32Array(this.buffer.length + float32.length);
            newBuf.set(this.buffer);
            newBuf.set(float32, this.buffer.length);
            this.buffer = newBuf;
            // Cap buffer at 2 seconds to avoid memory growth
            const maxSamples = sampleRate * 2;
            if (this.buffer.length > maxSamples) {
                this.buffer = this.buffer.slice(this.buffer.length - maxSamples);
            }
        };
    }

    process(inputs, outputs) {
        const output = outputs[0][0]; // mono
        if (!output) return true;

        const needed = output.length;
        if (this.buffer.length >= needed) {
            output.set(this.buffer.subarray(0, needed));
            this.buffer = this.buffer.slice(needed);
        } else {
            // Underrun — output silence
            output.fill(0);
        }
        return true;
    }
}

registerProcessor("pcm-player", PCMPlayerProcessor);
