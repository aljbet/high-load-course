package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID
import kotlin.math.abs

@Component
class Scope {
    @OptIn(ExperimentalCoroutinesApi::class)
    val esServiceCoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val esWriter: OrderedEsWriter = OrderedEsWriter(esServiceCoroutineScope)
}

data class EsWrite(
    val key: UUID,
    val action: suspend () -> Unit
)

class OrderedEsWriter(
    scope: CoroutineScope,
    shards: Int = 400,
    queueSizePerShard: Int = 20000
) {
    val logger: Logger = LoggerFactory.getLogger(OrderedEsWriter::class.java)

    private val channels = Array(shards) { Channel<EsWrite>(queueSizePerShard) }

    init {
        repeat(shards) { i ->
            scope.launch(Dispatchers.IO) {
                for (job in channels[i]) {
                    try {
                        job.action()
                    } catch (e: Exception) {
                        logger.error("[ERROR] Database sending error: ${e.message}")
                    }
                }
            }
        }
    }

    suspend fun submit(key: UUID, action: suspend () -> Unit) {
        val idx = shard(key)
        channels[idx].send(EsWrite(key, action))
    }

    private fun shard(key: UUID): Int {
        val h = key.mostSignificantBits xor key.leastSignificantBits
        return (abs(h.toInt()) % channels.size)
    }
}