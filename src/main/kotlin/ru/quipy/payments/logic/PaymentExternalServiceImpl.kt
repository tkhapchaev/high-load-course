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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
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

        const val RATE_LIMITER_WINDOW_SIZE_MS = 1000L
        const val SEMAPHORE_WAIT_TIME_MS = 1500L
        const val FAILED_TRANSACTION_RETRY_NUMBER = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rps = calculateLimiterRatePerSecond()
    private val rateLimiter = SlidingWindowRateLimiter(rps, Duration.ofMillis(RATE_LIMITER_WINDOW_SIZE_MS))
    private val semaphore = Semaphore(parallelRequests)

    init {
        logger.info("Initializing PaymentExternalSystemAdapter for $accountName with limiter rate = $rps and semaphore permits = $parallelRequests")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var paymentResult = performTransaction(paymentId, amount, paymentStartedAt)

        if (paymentResult.httpCode == null) {
            return
        }

        var retryCounter = 0

        while (retryCounter < FAILED_TRANSACTION_RETRY_NUMBER - 1 && !paymentResult.isSuccess && paymentResult.httpCode !in listOf(400, 401, 403, 404, 405)) {
            if (now() + requestAverageProcessingTime.toMillis() < deadline) {
                logger.info("[${accountName}] Transaction for payment $paymentId failed. Retrying... (${retryCounter + 1}/$FAILED_TRANSACTION_RETRY_NUMBER)")
                paymentResult = performTransaction(paymentId, amount, paymentStartedAt)
            } else {
                break
            }

            retryCounter++
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun performTransaction(paymentId: UUID, amount: Int, paymentStartedAt: Long) : PaymentResult {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        if (!semaphore.tryAcquire(SEMAPHORE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
            logger.warn("Timeout waiting for available slot in semaphore ($SEMAPHORE_WAIT_TIME_MS). Skipping payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Too many parallel requests for $accountName")
            }

            return PaymentResult(null, false)
        }

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
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

                return PaymentResult(response.code, body.result)
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
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

            return PaymentResult(null, false);
        } finally {
            semaphore.release()
        }
    }

    private fun calculateLimiterRatePerSecond() : Long {
        val rps = floor((1000.0 / requestAverageProcessingTime.toMillis()) * parallelRequests).toLong()

        if (rps >= rateLimitPerSec) {
            logger.warn("Calculated rps value for rate limiter exceeds its limit set in [$accountName]")
            return rateLimitPerSec.toLong()
        } else {
            return rps
        }
    }
}

public fun now() = System.currentTimeMillis()

class PaymentResult(val httpCode: Int?, val isSuccess: Boolean)