/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.state

import io.airbyte.cdk.load.util.CloseableCoroutine
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Releasable reservation of a quantity of bytes. For large blocks (ie, from [ReservationManager.reserveRatio],
 * provides a submanager that can be used to manage allocating the reservation).
 */
class Reserved<T>(
    private val parentManager: ReservationManager,
    val bytesReserved: Long,
    val value: T,
) : CloseableCoroutine {
    private var released = AtomicBoolean(false)

    suspend fun release() {
        if (!released.compareAndSet(false, true)) {
            return
        }
        parentManager.release(bytesReserved)
    }

    fun <U> replace(value: U): Reserved<U> = Reserved(parentManager, bytesReserved, value)

    override suspend fun close() {
        release()
    }
}

/**
 * Manages reservations of bytes (e.g. memory, disk, etc.) for usage by the destination.
 *
 * TODO: Some degree of logging/monitoring around how accurate we're actually being?
 */
class ReservationManager(val totalCapacityBytes: Long) {
    private var reservedBytes = AtomicLong(0L)
    private val mutex = Mutex()
    private val syncChannel = Channel<Unit>(Channel.UNLIMITED)

    val remainingMemoryBytes: Long
        get() = totalCapacityBytes - reservedBytes.get()

    /* Attempt to reserve memory. If enough memory is not available, waits until it is, then reserves. */
    suspend fun <T> reserveFor(requestedBytes: Long, reservedFor: T): Reserved<T> {
        if (requestedBytes > totalCapacityBytes) {
            throw IllegalArgumentException(
                "Requested ${requestedBytes}b memory exceeds ${totalCapacityBytes}b total"
            )
        }

        mutex.withLock {
            while (reservedBytes.get() + requestedBytes > totalCapacityBytes) {
                syncChannel.receive()
            }
            reservedBytes.addAndGet(requestedBytes)
        }

        return Reserved(this, requestedBytes, reservedFor)
    }

    suspend fun <T> reserveRatio(ratio: Double, reservedFor: T): Reserved<T> {
        val estimatedSize = (totalCapacityBytes.toDouble() * ratio).toLong()
        return reserveFor(estimatedSize, reservedFor)
    }

    suspend fun release(memoryBytes: Long) {
        reservedBytes.addAndGet(-memoryBytes)
        syncChannel.send(Unit)
    }
}
