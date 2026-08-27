package io.github.immaghzbad.aetherst.core

import android.content.Context
import android.net.TrafficStats
import android.os.Process
import io.github.immaghzbad.aetherst.platform.PlatformContext
import io.github.immaghzbad.aetherst.platform.getSettings
import io.github.immaghzbad.aetherst.shared.data.AetherConfigRepository
import io.github.immaghzbad.aetherst.shared.data.ActiveProxyProvider
import io.github.immaghzbad.aetherst.shared.data.LogRepository
import io.github.immaghzbad.aetherst.shared.model.SessionTraffic
import io.github.immaghzbad.aetherst.shared.model.*
import io.github.immaghzbad.aetherst.shared.platform.Bridge
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

object SocksGate {
    private val _readiness = MutableStateFlow(SocksReadiness.NOT_READY)
    val readiness: StateFlow<SocksReadiness> = _readiness.asStateFlow()
    fun setReady(v: SocksReadiness) { _readiness.value = v }
    suspend fun awaitReady(timeoutMs: Long = 5000): Boolean {
        return try {
            withTimeout(timeoutMs) { readiness.first { it == SocksReadiness.PROBED_OK }; true }
        } catch (_: Exception) { false }
    }
}

class ConnectionController private constructor(context: Context) {
    private val appContext = context.applicationContext
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val runner = AetherProcessRunner(appContext)
    private val mutex = Mutex()
    private val activeAttemptId = AtomicLong(0)
    private val loginCodeChannel = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)

    private var timerJob: Job? = null
    private var durationSeconds = 0L
    private var baseTx = 0L
    private var baseRx = 0L
    private var isManualTraffic = false
    private var lastManualTx = 0L
    private var lastManualRx = 0L

    companion object {
        const val ACTION_STATUS_CHANGED = "io.github.immaghzbad.aetherst.STATUS_CHANGED"

        @Volatile
        private var INSTANCE: ConnectionController? = null

        private val _status = MutableStateFlow(ConnectionStatus.STOPPED)
        val status: StateFlow<ConnectionStatus> = _status.asStateFlow()
        
        @Volatile
        var lastKnownStatus: ConnectionStatus = ConnectionStatus.STOPPED
            private set

        private val _elapsedSeconds = MutableStateFlow(0L)
        val elapsedSeconds: StateFlow<Long> = _elapsedSeconds.asStateFlow()

        private val _sessionTraffic = MutableStateFlow(SessionTraffic())
        val sessionTraffic: StateFlow<SessionTraffic> = _sessionTraffic.asStateFlow()

        private val _isWaitingForCode = MutableStateFlow(false)
        val isWaitingForCode: StateFlow<Boolean> = _isWaitingForCode.asStateFlow()

        private fun notifyStatusChanged(context: Context, newStatus: ConnectionStatus) {
            lastKnownStatus = newStatus
            _status.value = newStatus
            Bridge.statusOverride.value = newStatus
            val intent = android.content.Intent(ACTION_STATUS_CHANGED)
            intent.putExtra("status", newStatus.name)
            intent.setPackage(context.packageName)
            context.sendBroadcast(intent)
        }

        fun getInstance(context: Context): ConnectionController {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: ConnectionController(context.applicationContext).also { INSTANCE = it }
            }
        }

        fun updateIsWaitingForCode(waiting: Boolean) {
            _isWaitingForCode.value = waiting
            io.github.immaghzbad.aetherst.shared.platform.Bridge.isWaitingForCode.value = waiting
        }
    }

    init {
        CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate).launch {
            runner.connectionStatus.drop(1).collect { coreStatus ->
                handleCoreStatus(coreStatus)
            }
        }
    }


    fun submitLoginCode(code: String) {
        updateIsWaitingForCode(false)
        loginCodeChannel.trySend(code)
    }

    suspend fun start() = mutex.withLock {
        if (_status.value == ConnectionStatus.RUNNING || _status.value == ConnectionStatus.VALIDATING) {
            return@withLock
        }

        val attemptId = System.currentTimeMillis()
        activeAttemptId.set(attemptId)
        notifyStatusChanged(appContext, ConnectionStatus.STARTING)

        try {
            val config = AetherConfigRepository.getInstance(getSettings(PlatformContext(appContext))).config.value.effectiveZeroTrustConfig()
            var effectiveConfig = config
            if (CloakController.isSupported(config)) {
                val cloakStarted = runNativeBounded(30000L, "Cloak.start") { CloakController.start(appContext, config) } == true
                if (cloakStarted && CloakController.isRunning()) {
                    effectiveConfig = config.copy(peer = CloakController.getEffectivePeer(config))
                    LogRepository.i("[Controller] Cloak active, routing MASQUE via ${effectiveConfig.peer}")
                }
            }
            if (PsiphonController.isSupported(effectiveConfig)) {
                runNativeBounded<Unit>(30000L, "Psiphon.start") { PsiphonController.start(appContext, effectiveConfig, upstream = config.upstreamProxy.takeIf { config.upstreamProxyEnabled && it.isNotBlank() }) }
                if (PsiphonController.isRunning()) {
                    var waitPsiphon = 0
                    while (waitPsiphon < 30 && !PsiphonController.isConnected()) {
                        delay(1000.milliseconds)
                        waitPsiphon++
                    }
                    var stableWait = 0
                    while (stableWait < 25 && !PsiphonController.stableFor(10000)) {
                        delay(1000.milliseconds)
                        stableWait++
                    }
                    LogRepository.i("[Controller] Psiphon settled (stable), proceeding to chain core")
                    val desiredProto = effectiveConfig.protocol
                    if (desiredProto != AetherProtocol.MASQUE) {
                        LogRepository.i("[Controller] Psiphon SOCKS5 carries only MASQUE (TCP); forcing MASQUE outer instead of $desiredProto. Disable Psiphon to use $desiredProto directly.")
                    }
                    effectiveConfig = effectiveConfig.copy(protocol = AetherProtocol.MASQUE, upstreamProxy = PsiphonController.getUpstreamProxy())
                    LogRepository.i("[Controller] Psiphon active, chaining via ${effectiveConfig.upstreamProxy} outer=${effectiveConfig.protocol}")
                    ActiveProxyProvider.psiphonProxyUrl = PsiphonController.getUpstreamProxy()
                } else {
                    ActiveProxyProvider.psiphonProxyUrl = null
                }
            }
            val bindHost = if (effectiveConfig.shareHotspot) "0.0.0.0" else "127.0.0.1"
            val bindAddress = "$bindHost:${effectiveConfig.socksPort}"

            isManualTraffic = false
            baseTx = TrafficStats.getUidTxBytes(Process.myUid()).coerceAtLeast(0)
            baseRx = TrafficStats.getUidRxBytes(Process.myUid()).coerceAtLeast(0)

            LogRepository.i("[Controller] Starting core at $bindAddress")
            runner.start(effectiveConfig, bindAddress, onCodeRequired = {
                updateIsWaitingForCode(true)
            }, inputProvider = {
                loginCodeChannel.receive()
            })

            val proxyPort = effectiveConfig.socksPort.toIntOrNull() ?: 1819

            val baseTimeout = (effectiveConfig.validateSecs.coerceAtLeast(0) + 10).seconds
            var dpDeadline = System.currentTimeMillis() + baseTimeout.inWholeMilliseconds
            var dpValidated = false
            while (currentCoroutineContext().isActive) {
                if (activeAttemptId.get() != attemptId) break
                val coreStatus = runner.connectionStatus.value
                if (coreStatus == ConnectionStatus.DATAPLANE_VALIDATED || coreStatus == ConnectionStatus.SOCKS_READY) {
                    dpValidated = true
                    break
                }
                if (coreStatus == ConnectionStatus.ERROR) throw IllegalStateException("Core reported error during startup")
                if (coreStatus == ConnectionStatus.STOPPED) throw IllegalStateException("Core stopped unexpectedly during startup")
                if (isWaitingForCode.value) {
                    delay(1000.milliseconds)
                    continue
                }
                if (System.currentTimeMillis() > dpDeadline) {
                    throw IllegalStateException("Core data-plane validation timed out after ${baseTimeout.inWholeSeconds}s")
                }
                delay(250.milliseconds)
            }

            if (!dpValidated) {
                val coreStatus = runner.connectionStatus.value
                if (coreStatus == ConnectionStatus.ERROR) throw IllegalStateException("Core failed to start (error)")
                if (coreStatus == ConnectionStatus.STOPPED) throw IllegalStateException("Core stopped unexpectedly")
                throw IllegalStateException("Core data-plane validation timed out after ${baseTimeout.inWholeSeconds}s")
            }
            notifyStatusChanged(appContext, ConnectionStatus.DATAPLANE_VALIDATED)

            val socksReady = withTimeoutOrNull(60.seconds) {
                while (currentCoroutineContext().isActive) {
                    if (activeAttemptId.get() != attemptId) return@withTimeoutOrNull false
                    val coreStatus = runner.connectionStatus.value
                    if (coreStatus == ConnectionStatus.SOCKS_READY) return@withTimeoutOrNull true
                    if (coreStatus == ConnectionStatus.ERROR) throw IllegalStateException("Core reported error during startup")
                    if (coreStatus == ConnectionStatus.STOPPED) throw IllegalStateException("Core stopped unexpectedly during startup")
                    if (probeSocksReady("127.0.0.1", proxyPort)) return@withTimeoutOrNull true
                    delay(250.milliseconds)
                }
                false
            } ?: false

            if (!socksReady) {
                throw IllegalStateException("SOCKS proxy not ready (0x00 probe failed) after 60s")
            }
            notifyStatusChanged(appContext, ConnectionStatus.SOCKS_READY)

            if (!verifyPortListening("127.0.0.1", proxyPort)) {
                throw IllegalStateException("Proxy port $proxyPort is not listening")
            }

            delay(3000.milliseconds)

            notifyStatusChanged(appContext, ConnectionStatus.RUNNING)

            startTimer()
            LogRepository.i("[Controller] Core is active and validated on port $proxyPort")
        } catch (e: Exception) {
            val st = _status.value
            if (st == ConnectionStatus.STOPPED || st == ConnectionStatus.STOPPING) {
                runCatching { cleanup(attemptId) }
                return@withLock
            }
            if (runner.connectionStatus.value != ConnectionStatus.RUNNING) {
                LogRepository.e("[Controller] Startup failed: ${e.localizedMessage}")
                cleanup(attemptId)
                notifyStatusChanged(appContext, ConnectionStatus.ERROR)
            } else {
                LogRepository.w("[Controller] Startup check failed but core is running: ${e.localizedMessage}")
            }
        }
    }

    suspend fun stop() = mutex.withLock {
        if (_status.value == ConnectionStatus.STOPPED) {
            return@withLock
        }

        val attemptId = activeAttemptId.get()
        notifyStatusChanged(appContext, ConnectionStatus.STOPPING)
        LogRepository.i("[Controller] Stopping core")

        try {
            withTimeoutOrNull(10000.milliseconds) {
                withContext(Dispatchers.IO) {
                    runNativeBounded<Unit>(3000L, "Cloak.stop") { CloakController.stop() }
                    runNativeBounded<Unit>(3000L, "Psiphon.stop") { PsiphonController.stop() }
                }
                ActiveProxyProvider.psiphonProxyUrl = null
                stopTimer()
                runCatching { cleanup(attemptId) }
            } ?: LogRepository.w("[Controller] Stop teardown exceeded 10s safety bound")
        } catch (e: Exception) {
            LogRepository.e("[Controller] Stop teardown error: ${e.localizedMessage}")
        } finally {
            notifyStatusChanged(appContext, ConnectionStatus.STOPPED)
            LogRepository.i("[Controller] Core stopped")
        }
    }

    private suspend fun <T> runNativeBounded(timeoutMs: Long, label: String, block: () -> T): T? {
        return withContext(Dispatchers.IO) {
            val done = CompletableDeferred<T?>()
            val thread = Thread {
                try {
                    done.complete(block())
                } catch (_: Throwable) {
                    done.complete(null)
                }
            }
            thread.isDaemon = true
            thread.name = "native-$label"
            thread.start()
            withTimeoutOrNull(timeoutMs.milliseconds) { done.await() }
                ?: run { LogRepository.w("[Controller] $label did not finish within ${timeoutMs}ms; continuing"); null }
        }
    }

    private suspend fun cleanup(attemptId: Long) {
        if (activeAttemptId.get() == attemptId) {
            activeAttemptId.set(0)
        }
        runner.stop()
        updateIsWaitingForCode(false)
        delay(500.milliseconds)
    }

    private fun handleCoreStatus(coreStatus: ConnectionStatus) {
        _status.update { current ->
            if (current == ConnectionStatus.STOPPED) return@update current
            if (current == ConnectionStatus.STOPPING && coreStatus != ConnectionStatus.STOPPED) {
                return@update current
            }

            val next = when (coreStatus) {
                ConnectionStatus.ERROR -> {
                    if (current == ConnectionStatus.RUNNING || current == ConnectionStatus.RECONNECTING) {
                        LogRepository.e("[Controller] Core reported error")
                        stopTimer()
                        // Auto-reconnect on error when not user-initiated stop
                        scope.launch {
                            delay(2000.milliseconds)
                            if (_status.value == ConnectionStatus.ERROR) {
                                LogRepository.i("[Controller] Auto-reconnecting after error...")
                                start()
                            }
                        }
                        ConnectionStatus.RECONNECTING
                    } else {
                        LogRepository.e("[Controller] Core error during $current")
                        // Auto-reconnect on startup error
                        scope.launch {
                            delay(2000.milliseconds)
                            if (_status.value == ConnectionStatus.ERROR) {
                                LogRepository.i("[Controller] Auto-reconnecting after startup error...")
                                start()
                            }
                        }
                        ConnectionStatus.RECONNECTING
                    }
                }
                ConnectionStatus.STOPPED -> {
                    if (current == ConnectionStatus.RUNNING || current == ConnectionStatus.RECONNECTING) {
                        LogRepository.w("[Controller] Core stopped unexpectedly")
                        stopTimer()
                        // Auto-reconnect when core stops unexpectedly
                        scope.launch {
                            delay(2000.milliseconds)
                            if (_status.value == ConnectionStatus.ERROR || _status.value == ConnectionStatus.STOPPED) {
                                LogRepository.i("[Controller] Auto-reconnecting after unexpected stop...")
                                start()
                            }
                        }
                        ConnectionStatus.RECONNECTING
                    } else if (current == ConnectionStatus.STOPPING) {
                        ConnectionStatus.STOPPED
                    } else if (current == ConnectionStatus.STARTING || current == ConnectionStatus.VALIDATING) {
                        LogRepository.w("[Controller] Core stopped during $current")
                        // Auto-reconnect on startup failure
                        scope.launch {
                            delay(2000.milliseconds)
                            if (_status.value == ConnectionStatus.STOPPED || _status.value == ConnectionStatus.ERROR) {
                                LogRepository.i("[Controller] Auto-reconnecting after startup failure...")
                                start()
                            }
                        }
                        ConnectionStatus.RECONNECTING
                    } else {
                        current
                    }
                }
                ConnectionStatus.RECONNECTING -> {
                    if (current != ConnectionStatus.RECONNECTING) {
                        stopTimer()
                        ConnectionStatus.RECONNECTING
                    } else {
                        current
                    }
                }
                ConnectionStatus.SOCKS_READY -> {
                    if (current == ConnectionStatus.RECONNECTING) {
                        startTimer()
                        ConnectionStatus.RUNNING
                    } else {
                        current
                    }
                }
                ConnectionStatus.DATAPLANE_VALIDATED -> current
                ConnectionStatus.RUNNING -> {
                    if (current != ConnectionStatus.RUNNING) {
                        startTimer()
                        ConnectionStatus.RUNNING
                    } else {
                        current
                    }
                }
                else -> current
            }
            
            if (next != current) {
                notifyStatusChanged(appContext, next)
            }
            next
        }
    }

    private suspend fun isPortListening(host: String, port: Int): Boolean {
        return withContext(Dispatchers.IO) {
            runCatching {
                Socket().use { socket ->
                    socket.connect(InetSocketAddress(host, port), 1000)
                    true
                }
            }.getOrDefault(false)
        }
    }

    private suspend fun verifyPortListening(host: String, port: Int): Boolean {
        val deadline = System.currentTimeMillis() + 15000
        while (System.currentTimeMillis() < deadline) {
            if (isPortListening(host, port)) return true
            delay(500.milliseconds)
        }
        return false
    }

    private suspend fun probeSocksReady(host: String, port: Int): Boolean {
        return withContext(Dispatchers.IO) {
            runCatching {
                Socket().use { socket ->
                    socket.tcpNoDelay = true
                    socket.connect(InetSocketAddress(host, port), 3000)
                    socket.soTimeout = 6000
                    val ins = socket.getInputStream()
                    val out = socket.getOutputStream()
                    out.write(byteArrayOf(5, 1, 0))
                    out.flush()
                    val method = ByteArray(2)
                    if (!fillStream(ins, method)) return@runCatching false
                    if (method[0] != 5.toByte() || method[1] != 0.toByte()) return@runCatching false
                    val addr = InetAddress.getByName("1.1.1.1").address
                    val req = ByteArray(5 + 4 + 2)
                    req[0] = 5; req[1] = 1; req[2] = 0; req[3] = 1
                    System.arraycopy(addr, 0, req, 4, 4)
                    req[8] = (80 shr 8).toByte(); req[9] = 80.toByte()
                    out.write(req)
                    out.flush()
                    val hdr = ByteArray(4)
                    if (!fillStream(ins, hdr)) return@runCatching false
                    hdr[1].toInt() and 0xFF == 0
                }
            }.getOrDefault(false)
        }
    }

    private fun fillStream(ins: java.io.InputStream, buffer: ByteArray): Boolean {
        var offset = 0
        while (offset < buffer.size) {
            val n = ins.read(buffer, offset, buffer.size - offset)
            if (n <= 0) return false
            offset += n
        }
        return true
    }

    private fun startTimer() {
        if (timerJob?.isActive == true) return
        durationSeconds = 0L
        _elapsedSeconds.value = 0L
        timerJob = scope.launch {
            while (isActive) {
                delay(1000.milliseconds)
                durationSeconds++
                _elapsedSeconds.value = durationSeconds
                Bridge.elapsedOverride.value = durationSeconds
                if (!isManualTraffic) {
                    updateTrafficFromStats()
                }
            }
        }
    }

    private fun stopTimer() {
        timerJob?.cancel()
        timerJob = null
        durationSeconds = 0L
        _elapsedSeconds.value = 0L
        Bridge.elapsedOverride.value = 0L
        _sessionTraffic.value = SessionTraffic()
        Bridge.trafficOverride.value = SessionTraffic()
        isManualTraffic = false
        lastManualTx = 0L
        lastManualRx = 0L
    }

    private fun updateTrafficFromStats() {
        val currentTx = TrafficStats.getUidTxBytes(Process.myUid()).coerceAtLeast(0)
        val currentRx = TrafficStats.getUidRxBytes(Process.myUid()).coerceAtLeast(0)
        
        val diffTx = (currentTx - baseTx).coerceAtLeast(0)
        val diffRx = (currentRx - baseRx).coerceAtLeast(0)
        
        _sessionTraffic.value = SessionTraffic(diffTx, diffRx)
        Bridge.trafficOverride.value = SessionTraffic(diffTx, diffRx)
    }

    fun setTraffic(tx: Long, rx: Long) {
        if (tx > lastManualTx || rx > lastManualRx || (tx == 0L && rx == 0L && !isManualTraffic)) {
            isManualTraffic = true
            lastManualTx = tx.coerceAtLeast(lastManualTx)
            lastManualRx = rx.coerceAtLeast(lastManualRx)
            val traffic = SessionTraffic(lastManualTx, lastManualRx)
            _sessionTraffic.value = traffic
            Bridge.trafficOverride.value = traffic
        }
    }
}
