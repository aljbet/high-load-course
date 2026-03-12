package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class PaymentEventWriter() {
    private val logger = LoggerFactory.getLogger(PaymentEventWriter::class.java)

    private val shards = 64
    private val queuePerShard = 8192

    private val channels = Array(shards) { Channel<suspend () -> Unit>(queuePerShard) }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    init {
        repeat(shards) { shard ->
            scope.launch {
                for (action in channels[shard]) {
                    try {
                        action()
                    } catch (e: Exception) {
                        logger.error("Shard $shard failed to process event", e)
                    }
                }
            }
        }
    }

    suspend fun submit(paymentId: UUID, block: () -> Unit) {
        val shard = shardIndex(paymentId)
        channels[shard].send(block)
    }

    private fun shardIndex(id: UUID): Int {
        val h = id.mostSignificantBits xor id.leastSignificantBits
        return h.toInt() % shards
    }
}