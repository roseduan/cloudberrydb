/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Turns the {@link OutputStream} the read path writes into a stream of gRPC
 * messages.
 *
 * <p>{@code ReadService.readData(context, outputStream)} is the same call the
 * REST resource makes; there it writes into the servlet's octet-stream, here
 * into gRPC messages. Nothing about the bytes changes, which is what makes the
 * data plane byte-identical between transports.
 *
 * <p>THE CANCELLATION CHECK IS THE POINT. Before every flush this asks
 * {@link ServerCallStreamObserver#isCancelled()} and throws if the client has
 * gone. It was measured why that is necessary: after a client cancels, gRPC does
 * NOT report it to the handler -- {@code onNext()} threw nothing across 35
 * further writes, all silently discarded -- and {@code setOnCancelHandler}
 * only fires once the handler has returned. Without this check a cancelled
 * query leaves the agent scanning object storage to completion, burning CPU
 * and egress that no test would notice because the query already returned.
 *
 * <p>It also respects {@link ServerCallStreamObserver#isReady()} as
 * backpressure, so a slow consumer cannot make the agent buffer an entire scan
 * in memory.
 */
public class GrpcChunkOutputStream extends OutputStream {

    /**
     * 64 KiB. Large enough that per-message overhead is noise against a scan,
     * small enough that a cancel is noticed promptly -- the check happens
     * between chunks, so the chunk size IS the cancellation granularity.
     */
    static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    /** How long a client may stop reading before the read is abandoned. */
    static final long DEFAULT_STALL_TIMEOUT_MS = 60_000;

    /**
     * Bytes the transport may queue before {@link ServerCallStreamObserver#isReady()}
     * turns false. grpc-java's default is 32 KiB, less than one chunk, so the
     * call would be not-ready after nearly every chunk and the writer would
     * spend its time waiting rather than producing. 1 MiB keeps a few chunks
     * in flight while still bounding what a slow client can make us buffer.
     */
    static final int ON_READY_THRESHOLD = 1024 * 1024;

    /**
     * Backoff while the transport is not ready: a window that drains within
     * microseconds is noticed within microseconds, and a client that really
     * stopped reading costs a poll every few milliseconds, not a busy CPU.
     */
    private static final long FIRST_BACKOFF_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long MAX_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

    private final ServerCallStreamObserver<byte[]> observer;
    private final byte[] buffer;
    private final long stallTimeoutMs;
    private int used;
    private boolean closed;
    /**
     * Set once sending a chunk failed. The call is then being abandoned, so
     * nothing more is sent: in particular close(), reached through
     * try-with-resources while the failure unwinds, must not wait for the
     * same stalled client a second time.
     */
    private boolean failed;

    public GrpcChunkOutputStream(ServerCallStreamObserver<byte[]> observer) {
        this(observer, DEFAULT_CHUNK_SIZE, DEFAULT_STALL_TIMEOUT_MS);
    }

    GrpcChunkOutputStream(ServerCallStreamObserver<byte[]> observer, int chunkSize) {
        this(observer, chunkSize, DEFAULT_STALL_TIMEOUT_MS);
    }

    GrpcChunkOutputStream(ServerCallStreamObserver<byte[]> observer, int chunkSize,
                          long stallTimeoutMs) {
        this.observer = observer;
        this.buffer = new byte[chunkSize];
        this.stallTimeoutMs = stallTimeoutMs;
        try {
            observer.setOnReadyThreshold(ON_READY_THRESHOLD);
        } catch (IllegalStateException e) {
            // Only allowed while the handler is first invoked; outside that
            // the default threshold still works, just with more waiting.
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (used == buffer.length) {
            flush();
        }
        buffer[used++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = len;
        int from = off;
        while (remaining > 0) {
            if (used == buffer.length) {
                flush();
            }
            int n = Math.min(remaining, buffer.length - used);
            System.arraycopy(b, from, buffer, used, n);
            used += n;
            from += n;
            remaining -= n;
        }
    }

    @Override
    public void flush() throws IOException {
        if (failed) {
            throw new IOException("the gRPC stream has already failed");
        }
        if (used == 0) {
            return;
        }
        try {
            checkCancelled();
            awaitReady();
            byte[] chunk = new byte[used];
            System.arraycopy(buffer, 0, chunk, 0, used);
            used = 0;
            observer.onNext(chunk);
        } catch (IOException | RuntimeException | Error e) {
            failed = true;
            used = 0;
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (!failed) {
            flush();
        }
    }

    private void checkCancelled() throws IOException {
        if (observer.isCancelled()) {
            // An IOException, not a silent return: the read path has resources
            // open and must unwind, and its catch blocks expect the type an
            // OutputStream throws. The CANCELLED cause is what
            // GrpcClientDisconnectPredicate recognises, so core logs this as
            // an aborted read rather than a failure with a stack trace.
            throw new IOException("the gRPC client cancelled the read",
                    Status.CANCELLED.withDescription("the gRPC client cancelled the read")
                            .asRuntimeException());
        }
    }

    /**
     * Wait for the transport to accept more, checking for cancellation while
     * waiting. Without a bound, a client that stops reading and never cancels
     * would park this thread forever.
     *
     * <p>This polls rather than waiting on {@code setOnReadyHandler}: the
     * handler is delivered on the call's serializing executor, which is the
     * very thread blocked here until the read returns, so it would never run
     * in time. The same holds for the cancel handler, hence the
     * {@code isCancelled()} poll.
     */
    private void awaitReady() throws IOException {
        if (observer.isReady()) {
            return;
        }
        long start = System.nanoTime();
        long stallTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(stallTimeoutMs);
        long backoff = FIRST_BACKOFF_NANOS;
        while (!observer.isReady()) {
            checkCancelled();
            if (System.nanoTime() - start > stallTimeoutNanos) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("the gRPC client stopped reading for "
                                + stallTimeoutMs + " ms")
                        .asRuntimeException();
            }
            LockSupport.parkNanos(backoff);
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for the gRPC transport");
            }
            backoff = Math.min(backoff * 2, MAX_BACKOFF_NANOS);
        }
    }
}
