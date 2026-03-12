package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID
import kotlin.math.abs

@Component
class PaymentEventWriter() {
    private val logger = LoggerFactory.getLogger(PaymentEventWriter::class.java)

    private val shards = 400
    private val queuePerShard = 20000

    private val channels = Array(shards) { Channel<suspend () -> Unit>(queuePerShard) }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    init {
        repeat(shards) { shard ->
            scope.launch(Dispatchers.IO) {
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

    suspend fun submit(paymentId: UUID, block: suspend () -> Unit) {
        val shard = shardIndex(paymentId)
        channels[shard].send(block)
    }

    private fun shardIndex(id: UUID): Int {
        val h = id.mostSignificantBits xor id.leastSignificantBits
        return abs(h.toInt()) % shards
    }
}