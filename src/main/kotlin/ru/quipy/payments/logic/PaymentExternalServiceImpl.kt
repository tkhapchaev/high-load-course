package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.math.floor


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val actualRateLimitPerSec = calculateActualRateLimitPerSec()
    private val rateLimiterWindowDuration = Duration.ofSeconds(1)
    private val rateLimiter = SlidingWindowRateLimiter(actualRateLimitPerSec, rateLimiterWindowDuration)

    private val semaphorePermits = parallelRequests
    private val semaphoreWaitTime = requestAverageProcessingTime
    private val semaphore = Semaphore(semaphorePermits)

    private val unretriableHttpCodes = listOf(400, 401, 403, 404, 405, 408)
    private val failedTransactionRetryCount = 3

    private val requestTimeout = Duration.ofSeconds(10)

    private val threadsCount = calculateThreadsCount()
    private val threadPool = ThreadPoolExecutor(
        threadsCount,
        threadsCount,
        1,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val client = OkHttpClient.Builder().callTimeout(requestTimeout).build()

    init {
        val indent = "\t".repeat(26)
        val serviceInfo = "Initializing PaymentExternalSystemAdapter for $accountName with:\n" +
                          "${indent}SlidingWindowRateLimiter($actualRateLimitPerSec, $rateLimiterWindowDuration)\n" +
                          "${indent}Semaphore($semaphorePermits, $semaphoreWaitTime)\n" +
                          "${indent}RequestTimeout($requestTimeout)\n" +
                          "${indent}ThreadsCount($threadsCount)\n"

        logger.info(serviceInfo)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        threadPool.submit {
            val transactionResult = performTransaction(paymentId, amount, paymentStartedAt)
            if (isTransactionRetriable(transactionResult)) {
                retryTransaction(paymentId, amount, paymentStartedAt, deadline, transactionResult)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun retryTransaction(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, failedTransactionResult: TransactionResult) {
        var transactionResult = failedTransactionResult
        var retryCounter = 0

        while (isTransactionRetriable(transactionResult) && retryCounter < failedTransactionRetryCount) {
            if (now() + requestAverageProcessingTime.toMillis() < deadline) {
                logger.info("[${accountName}] Transaction for payment $paymentId failed. Retrying... (${retryCounter + 1}/$failedTransactionRetryCount)")
                transactionResult = performTransaction(paymentId, amount, paymentStartedAt)
                performTransaction(paymentId, amount, paymentStartedAt)
            } else {
                break
            }

            retryCounter++
        }
    }

    private fun performTransaction(paymentId: UUID, amount: Int, paymentStartedAt: Long) : TransactionResult {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        if (!semaphore.tryAcquire(semaphoreWaitTime.toMillis(), TimeUnit.MILLISECONDS)) {
            logger.warn("Timeout waiting for available slot in semaphore ($semaphoreWaitTime). Skipping payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Too many parallel requests for $accountName")
            }

            return TransactionResult(null, false)
        }

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${formatDurationAsIso(requestTimeout)}")
            post(emptyBody)
        }.build()

        try {
            rateLimiter.tickBlocking()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                return TransactionResult(response.code, body.result)
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }

            return TransactionResult(null, false)
        } finally {
            semaphore.release()
        }
    }

    private fun calculateActualRateLimitPerSec() : Long {
        val rps = floor((1000.0 / requestAverageProcessingTime.toMillis()) * parallelRequests).toLong()

        if (rps >= rateLimitPerSec) {
            logger.warn("Calculated rps value for rate limiter exceeds its limit set in $accountName")
            return rateLimitPerSec.toLong()
        } else {
            return rps
        }
    }

    private fun calculateThreadsCount() : Int {
        return (actualRateLimitPerSec / (1000.0 / requestAverageProcessingTime.toMillis())).toInt()
    }

    private fun isTransactionRetriable(transactionResult: TransactionResult) : Boolean {
        return !transactionResult.isSuccess && transactionResult.httpCode != null && transactionResult.httpCode !in unretriableHttpCodes
    }

    private fun formatDurationAsIso(duration: Duration): String {
        val seconds = duration.toMillis() / 1000.0
        return "PT${"%.3f".format(Locale.US, seconds)}S"
    }
}

public fun now() = System.currentTimeMillis()

class TransactionResult(val httpCode: Int?, val isSuccess: Boolean)