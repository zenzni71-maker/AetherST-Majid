package io.github.immaghzbad.aetherst.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.net.VpnService
import android.os.Build
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import io.github.immaghzbad.aetherst.MainActivity
import io.github.immaghzbad.aetherst.R
import io.github.immaghzbad.aetherst.core.ConnectionController
import io.github.immaghzbad.aetherst.core.DnsMap
import io.github.immaghzbad.aetherst.core.HevEngineSettings
import io.github.immaghzbad.aetherst.core.HevTun2SocksConfig
import io.github.immaghzbad.aetherst.core.HevTun2SocksEngine
import io.github.immaghzbad.aetherst.core.HevTun2SocksNative
import io.github.immaghzbad.aetherst.core.PsiphonController
import io.github.immaghzbad.aetherst.core.RoutingEngine
import io.github.immaghzbad.aetherst.platform.PlatformContext
import io.github.immaghzbad.aetherst.platform.getSettings
import io.github.immaghzbad.aetherst.core.SocksTunBridge
import io.github.immaghzbad.aetherst.shared.data.AetherConfigRepository
import io.github.immaghzbad.aetherst.shared.data.LogRepository
import io.github.immaghzbad.aetherst.shared.model.*
import kotlinx.coroutines.*
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds

@Suppress("VpnServicePolicy")
class AetherVpnService : VpnService() {

    private var vpnInterface: ParcelFileDescriptor? = null
    private var hevEngine: HevTun2SocksEngine? = null
    private var socksBridge: SocksTunBridge? = null
    private var routingEngine: RoutingEngine? = null
    private var activeTunnelEngine: TunnelEngine? = null

    private var wakeLock: PowerManager.WakeLock? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val stateMutex = Mutex()
    private val activeAttemptId = AtomicLong(0)
    private val commandCounter = AtomicLong(0)
    private var startupJob: Job? = null
    private var statsJob: Job? = null

    private var isUserInitiatedStop = false
    private var wasEverRunning = false

    companion object {
        const val ACTION_START = "io.github.immaghzbad.aetherst.ACTION_START"
        const val ACTION_STOP = "io.github.immaghzbad.aetherst.ACTION_STOP"
        const val CHANNEL_ID = "aether_vpn_status_v2"
        const val ALERT_CHANNEL_ID = "aether_vpn_alerts"
        const val NOTIFICATION_ID = 1001
        const val ALERT_NOTIFICATION_ID = 1003

        fun startVpn(context: Context): Boolean = runCatching {
            val intent = Intent(context, AetherVpnService::class.java).apply { action = ACTION_START }
            context.startForegroundService(intent)
            true
        }.getOrElse {
            LogRepository.e("[VpnService] Start failed: ${it.localizedMessage}")
            false
        }

        fun stopVpn(context: Context): Boolean = runCatching {
            val intent = Intent(context, AetherVpnService::class.java).apply { action = ACTION_STOP }
            context.startService(intent)
            true
        }.getOrElse {
            LogRepository.e("[VpnService] Stop failed: ${it.localizedMessage}")
            false
        }

        val serviceState: StateFlow<ConnectionStatus> get() = ConnectionController.status
    }

    private fun getController() = ConnectionController.getInstance(this)

    override fun onCreate() {
        super.onCreate()
        LogRepository.initialize(getSettings(PlatformContext(this)))
        PsiphonController.setVpnService(this)
        createNotificationChannel()
        
        val pm = getSystemService(POWER_SERVICE) as PowerManager
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "AetherST:VpnWakeLock")

        scope.launch {
            ConnectionController.status.collect { status ->
                updateNotification()
                handleTunLifecycle(status)
            }
        }

        scope.launch {
            AetherConfigRepository.getInstance(getSettings(PlatformContext(this@AetherVpnService))).config.collect {
                routingEngine?.clearCache()
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                isUserInitiatedStop = false
                showInitialNotification()
                startAttempt(commandCounter.incrementAndGet())
            }
            ACTION_STOP -> {
                isUserInitiatedStop = true
                stopVpnService(commandCounter.incrementAndGet())
            }
            else -> {
                if (!isUserInitiatedStop) {
                    LogRepository.i("[VpnService] System-initiated start (always-on / restart) -> starting tunnel")
                    isUserInitiatedStop = false
                    showInitialNotification()
                    startAttempt(commandCounter.incrementAndGet())
                }
            }
        }
        return START_STICKY
    }

    override fun onRevoke() {
        isUserInitiatedStop = false
        LogRepository.w("[VpnService] VPN revoked by system or other app")
        val config = AetherConfigRepository.getInstance(getSettings(PlatformContext(this))).config.value
        
        scope.launch {
            val attemptId = activeAttemptId.getAndSet(0)
            startupJob?.cancelAndJoin()
            stopStatsJob()
            
            
            
            stateMutex.withLock {
                hevEngine?.requestStop()
                hevEngine = null
                socksBridge?.stop()
                socksBridge = null
                closeVpnInterface(attemptId)
                activeTunnelEngine = null
            }

            if (config.connectionMode != ConnectionMode.PROXY_ONLY) {
                getController().stop()
            } else {
                LogRepository.i("[VpnService] Revoked but keeping core alive for Proxy Mode")
            }
            
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf()
        }
        super.onRevoke()
    }

    private fun startAttempt(commandId: Long) {
        startupJob = scope.launch {
            if (commandCounter.get() != commandId) return@launch

            val attemptId = stateMutex.withLock {
                if (commandCounter.get() != commandId) return@launch
                val current = ConnectionController.status.value
                // Skip if already running or connecting - avoid duplicate connection attempts
                if (current == ConnectionStatus.RUNNING || 
                    current == ConnectionStatus.VALIDATING || 
                    current == ConnectionStatus.STARTING ||
                    current == ConnectionStatus.SOCKS_READY ||
                    current == ConnectionStatus.DATAPLANE_VALIDATED ||
                    current == ConnectionStatus.RECONNECTING) return@launch
                
                val id = System.currentTimeMillis()
                activeAttemptId.set(id)
                id
            }

            runCatching { wakeLock?.acquire(4 * 60 * 60 * 1000L) }

            try {
                val config = AetherConfigRepository.getInstance(getSettings(PlatformContext(this@AetherVpnService))).config.value

                routingEngine = RoutingEngine(config.routingRules)

                val effectiveEngine = if (
                    config.tunnelEngine == TunnelEngine.HEV_TUN2SOCKS &&
                    (config.routingRules.isNotEmpty() || config.blockedPackages.isNotEmpty()) &&
                    !config.tunnelAllApps
                ) {
                    TunnelEngine.SOCKS_TUN_BRIDGE
                } else {
                    config.tunnelEngine
                }
                activeTunnelEngine = effectiveEngine

                if (!establishVpnTun(attemptId, effectiveEngine)) throw IllegalStateException("TUN establishment failed")
                ensureCurrentAttempt(attemptId)
                val descriptor = vpnInterface ?: throw IllegalStateException("TUN descriptor unavailable")

                if (effectiveEngine == TunnelEngine.HEV_TUN2SOCKS) {
                    if (!HevTun2SocksNative.isAvailable) throw IllegalStateException("HEV Native library not available")

                    hevEngine = HevTun2SocksEngine()

                    val hevSettings = HevEngineSettings(
                        logLevel = config.hevLogLevel,
                        connectTimeoutMs = config.hevConnectTimeoutMs,
                        readWriteTimeoutMs = config.hevReadWriteTimeoutMs,
                        maxSessionCount = config.hevMaxSessionCount,
                        mapdnsCacheSize = config.hevMapdnsCacheSize
                    )
                    LogRepository.i(
                        "[VpnService] HEV settings: log=${hevSettings.logLevel} connectTimeout=${hevSettings.connectTimeoutMs}ms " +
                                "rwTimeout=${hevSettings.readWriteTimeoutMs}ms maxSessions=${if (hevSettings.maxSessionCount == 0) "unlimited" else hevSettings.maxSessionCount.toString()} mapdnsCache=${hevSettings.mapdnsCacheSize} udp=${config.hevUdpMode}"
                    )

                    val ok = hevEngine?.start(
                        tunPfd = descriptor,
                        socksAddress = config.socksHost,
                        socksPort = config.socksPort.toIntOrNull() ?: 1819,
                        mtu = 1280,
                        attemptId = attemptId,
                        settings = hevSettings,
                        udpMode = config.hevUdpMode
                    ) == true
                    if (!ok) throw IllegalStateException("HEV engine failed to start")
                } else {
                    socksBridge = SocksTunBridge(
                        vpnService = this@AetherVpnService,
                        tunDescriptor = descriptor,
                        socksHost = config.socksHost,
                        socksPort = config.socksPort.toIntOrNull() ?: 1819,
                        mtu = 1280,
                        blockedPackagesProvider = { if (config.tunnelAllApps) emptySet() else config.blockedPackages },
                        routingEngine = routingEngine!!
                    ).apply { start() }
                }

                getController().start()
                if (ConnectionController.status.value != ConnectionStatus.RUNNING) {
                    throw IllegalStateException("Core failed to start")
                }
                ensureCurrentAttempt(attemptId)

                val socksPort = config.socksPort.toIntOrNull() ?: 1819
                runCatching {
                    val domainCode = probeCoreSocks5(config.socksHost, socksPort, domainTarget = "www.cloudflare.com", ipLiteralTarget = null)
                    val ipCode = probeCoreSocks5(config.socksHost, socksPort, domainTarget = null, ipLiteralTarget = "1.1.1.1")
                    LogRepository.i("[VpnService] Core proxy probe: domain-reply=$domainCode ip-literal-reply=$ipCode (0x00=granted)")
                }.onFailure {
                    LogRepository.e("[VpnService] Core proxy probe failed: ${it.localizedMessage}")
                }

                LogRepository.i("[VpnService] VPN tunnel active")
                wasEverRunning = true
                startStatsJob()
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (throwable: Throwable) {
                if (activeAttemptId.get() == attemptId && commandCounter.get() == commandId) {
                    rollback(attemptId, throwable.localizedMessage ?: "Startup failed")
                }
            }
        }
    }

    private fun ensureCurrentAttempt(attemptId: Long) {
        if (activeAttemptId.get() != attemptId) throw IllegalStateException("Connection attempt invalidated")
    }

    private fun establishVpnTun(attemptId: Long, engine: TunnelEngine): Boolean = runCatching {
        val pendingFlags = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        val builder = Builder()
            .addAddress("198.18.0.1", 24)
            .addAddress("fd00::1", 120)
            .addRoute("0.0.0.0", 0)
            .setMtu(1280)
            .setSession("AetherST Tunnel")
            .setConfigureIntent(PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), pendingFlags))
        runCatching {
            val bypassIps = mutableSetOf<String>()
            val cfg = AetherConfigRepository.getInstance(getSettings(PlatformContext(this))).config.value
            if (cfg.peer.isNotEmpty()) {
                Regex("""\d+\.\d+\.\d+\.\d+""").find(cfg.peer)?.value?.let { bypassIps.add(it) }
            }
            filesDir.listFiles()?.filter { it.name.contains("lastconn") }?.forEach { f ->
                try {
                    Regex("""\d+\.\d+\.\d+\.\d+""").find(f.readText())?.value?.let { bypassIps.add(it) }
                } catch (_: Exception) {}
            }
            bypassIps.add("162.159.198.39")
            bypassIps.add("162.159.198.2")
            bypassIps.add("162.159.192.1")
            bypassIps.add("188.114.96.1")
            for (ip in bypassIps) {
                try {
                    val s = Socket()
                    protect(s)
                    s.connect(InetSocketAddress(ip, 443), 200)
                    s.close()
                } catch (_: Exception) {}
            }
        }

        if (engine == TunnelEngine.HEV_TUN2SOCKS) {
            builder.addDnsServer(HevTun2SocksConfig.MAP_DNS_ADDRESS)
        } else {
            builder.addDnsServer("1.1.1.1")
            builder.addDnsServer("8.8.8.8")
            builder.addDnsServer("2606:4700:4700::1111")
            builder.addDnsServer("2001:4860:4860::8888")
        }

        val config = AetherConfigRepository.getInstance(getSettings(PlatformContext(this))).config.value
        if (config.ipv6Leak && Build.VERSION.SDK_INT > Build.VERSION_CODES.O_MR1) {
            runCatching { builder.addRoute("::", 0) }
        }

        if (config.tunnelAllApps) {
            builder.addDisallowedApplication(packageName)
        } else {
            if (config.tunneledPackages.isNotEmpty()) {
                var added = 0
                config.tunneledPackages
                    .asSequence()
                    .filterNot { it == packageName }
                    .forEach { pkg ->
                        try {
                            builder.addAllowedApplication(pkg)
                            added++
                        } catch (_: PackageManager.NameNotFoundException) {
                            LogRepository.w("[Tun] Ignoring uninstalled package: $pkg")
                        }
                    }
                LogRepository.i("[Tun] Bypass-default: $added apps tunneled, rest bypass (allowed mode)")
            } else {
                builder.addDisallowedApplication(packageName)
                try {
                    val pm = packageManager
                    val allPkgs = pm.getInstalledApplications(PackageManager.GET_META_DATA)
                    var disallowed = 0
                    for (app in allPkgs) {
                        val pkg = app.packageName
                        if (pkg == packageName) continue
                        try {
                            builder.addDisallowedApplication(pkg)
                            disallowed++
                        } catch (_: PackageManager.NameNotFoundException) {}
                    }
                    LogRepository.i("[Tun] Bypass-default: 0 tunneled, $disallowed apps bypassed (all bypass)")
                } catch (e: Exception) {
                    LogRepository.w("[Tun] Failed to enumerate apps for all-bypass: ${e.message}")
                }
            }
        }

        vpnInterface = builder.establish() ?: return false
        LogRepository.i("[Tun] [attempt=$attemptId] Established")
        true
    }.getOrElse {
        LogRepository.e("[Tun] [attempt=$attemptId] Failed: ${it.localizedMessage}")
        false
    }

    private suspend fun rollback(attemptId: Long, reason: String) {
        LogRepository.e("[VpnService] Rollback: $reason")
        stopStatsJob()
        val status = ConnectionController.status.value
        val pauseOnly = !isUserInitiatedStop &&
                (status == ConnectionStatus.RECONNECTING || status == ConnectionStatus.DATAPLANE_VALIDATED || status == ConnectionStatus.SOCKS_READY)
        if (wasEverRunning && !isUserInitiatedStop) {
            showDisconnectionAlert(reason)
        }
        cleanupResources(attemptId)
        if (!pauseOnly) {
            getController().stop()
        }
    }

    private fun stopVpnService(commandId: Long) {
        scope.launch {
            val attemptId = activeAttemptId.getAndSet(0)
            startupJob?.cancelAndJoin()
            stopStatsJob()

            cleanupResources(attemptId)
            runCatching { getController().stop() }.onFailure {
                LogRepository.e("[VpnService] Controller stop failed: ${it.localizedMessage}")
            }

            if (commandCounter.get() == commandId) {
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
    }

    private suspend fun cleanupResources(attemptId: Long, forceTeardown: Boolean = false) {
        val status = ConnectionController.status.value
        val pauseOnly = !forceTeardown && !isUserInitiatedStop &&
                (status == ConnectionStatus.RECONNECTING || status == ConnectionStatus.DATAPLANE_VALIDATED || status == ConnectionStatus.SOCKS_READY)
        stateMutex.withLock {
            if (pauseOnly) {
                LogRepository.i("[VpnService] Reconnect in progress; pausing TUN instead of tearing down")
                stopStatsJob()
                hevEngine?.pause()
            } else {
                hevEngine?.requestStop()
                hevEngine = null
                socksBridge?.stop()
                socksBridge = null
                closeVpnInterface(attemptId)
                activeTunnelEngine = null
                DnsMap.clear()
            }
            runCatching { if (wakeLock?.isHeld == true) wakeLock?.release() }
        }
    }

    private suspend fun handleTunLifecycle(status: ConnectionStatus) {
        when (status) {
            ConnectionStatus.RECONNECTING -> {
                if (!wasEverRunning) return
                stopStatsJob()
                if (PsiphonController.isConnected()) {
                    hevEngine?.pause()
                    LogRepository.i("[VpnService] RECONNECTING: TUN paused, interface kept open")
                } else {
                    LogRepository.e("[VpnService] RECONNECTING but Psiphon upstream down; tearing down TUN")
                    val attemptId = activeAttemptId.get()
                    stateMutex.withLock {
                        hevEngine?.requestStop()
                        hevEngine = null
                        socksBridge?.stop()
                        socksBridge = null
                        closeVpnInterface(attemptId)
                        activeTunnelEngine = null
                    }
                    getController().stop()
                }
            }
            ConnectionStatus.DATAPLANE_VALIDATED, ConnectionStatus.SOCKS_READY -> {
                if (!wasEverRunning) return
                stopStatsJob()
                hevEngine?.pause()
            }
            ConnectionStatus.RUNNING, ConnectionStatus.TUN_ACTIVE -> {
                if (!wasEverRunning) return
                hevEngine?.resume()
                if (statsJob == null) startStatsJob()
            }
            else -> {}
        }
    }

    private fun closeVpnInterface(attemptId: Long) {
        vpnInterface?.let {
            runCatching { it.close() }
            vpnInterface = null
            LogRepository.i("[Tun] [attempt=$attemptId] Closed")
        }
    }

    private fun startStatsJob() {
        statsJob?.cancel()
        statsJob = scope.launch {
            while (isActive) {
                delay(1000.milliseconds)
                updateTraffic()
            }
        }
    }

    private fun stopStatsJob() {
        statsJob?.cancel()
        statsJob = null
    }

    private fun updateTraffic() {
        if (activeTunnelEngine == TunnelEngine.HEV_TUN2SOCKS) {
            hevEngine?.stats?.value?.let {
                getController().setTraffic(it.txBytes, it.rxBytes)
                logPeriodicTraffic("[VpnService] TUN stats (HEV): txBytes=${it.txBytes} rxBytes=${it.rxBytes} txPackets=${it.txPackets} rxPackets=${it.rxPackets}")
            }
        } else {
            socksBridge?.getStats()?.let {
                getController().setTraffic(it.txBytes, it.rxBytes)
                logPeriodicTraffic("[VpnService] TUN stats (Bridge): txBytes=${it.txBytes} rxBytes=${it.rxBytes}")
            }
        }
    }

    private var trafficLogTick = 0L

    private fun logPeriodicTraffic(message: String) {
        trafficLogTick++
        if (trafficLogTick % 5 == 0L) LogRepository.i(message)
    }

    private fun probeCoreSocks5(socksHost: String, socksPort: Int, domainTarget: String?, ipLiteralTarget: String?): Int {
        val socket = Socket()
        protect(socket)
        try {
            socket.tcpNoDelay = true
            socket.connect(InetSocketAddress(socksHost, socksPort), 3000)
            socket.soTimeout = 6000
            val ins = socket.getInputStream()
            val out = socket.getOutputStream()

            out.write(byteArrayOf(5, 1, 0))
            out.flush()
            val method = ByteArray(2)
            if (!fillStream(ins, method)) return -255
            if (method[0] != 5.toByte() || method[1] != 0.toByte()) return -254

            val addrPart: ByteArray = if (domainTarget != null) {
                val d = domainTarget.toByteArray()
                val buf = ByteArray(1 + d.size)
                buf[0] = d.size.toByte()
                System.arraycopy(d, 0, buf, 1, d.size)
                buf
            } else {
                InetAddress.getByName(requireNotNull(ipLiteralTarget)).address
            }
            val atyp: Byte = if (domainTarget != null) 3 else 1

            val req = ByteArray(5 + addrPart.size + 2)
            req[0] = 5
            req[1] = 1
            req[2] = 0
            req[3] = atyp
            System.arraycopy(addrPart, 0, req, 4, addrPart.size)
            req[4 + addrPart.size] = (80 shr 8).toByte()
            req[5 + addrPart.size] = 80.toByte()
            out.write(req)
            out.flush()

            val hdr = ByteArray(4)
            if (!fillStream(ins, hdr)) return -253
            when (hdr[3].toInt() and 0xFF) {
                1 -> if (!fillStream(ins, ByteArray(6))) return -252
                4 -> if (!fillStream(ins, ByteArray(18))) return -252
                3 -> {
                    val len = ins.read()
                    if (len <= 0 || !fillStream(ins, ByteArray(len + 2))) return -252
                }
            }
            return hdr[1].toInt() and 0xFF
        } finally {
            runCatching { socket.close() }
        }
    }

    private fun fillStream(ins: InputStream, buffer: ByteArray): Boolean {
        var offset = 0
        while (offset < buffer.size) {
            val n = ins.read(buffer, offset, buffer.size - offset)
            if (n <= 0) return false
            offset += n
        }
        return true
    }

    private fun showInitialNotification() {
        val notification = buildNotification("Connecting...")
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun updateNotification() {
        val status = ConnectionController.status.value
        if (status == ConnectionStatus.STOPPED) return
        val text = when (status) {
            ConnectionStatus.RUNNING, ConnectionStatus.TUN_ACTIVE -> "VPN connected"
            ConnectionStatus.STARTING, ConnectionStatus.VALIDATING, ConnectionStatus.DATAPLANE_VALIDATED, ConnectionStatus.SOCKS_READY -> "Connecting..."
            ConnectionStatus.RECONNECTING -> "Reconnecting..."
            ConnectionStatus.STOPPING -> "Disconnecting..."
            ConnectionStatus.ERROR, ConnectionStatus.FAILED -> "Connection error"
        }
        val manager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, buildNotification(text))
    }

    private fun buildNotification(statusText: String): Notification {
        val flags = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        val contentIntent = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), flags)
        val stopIntent = PendingIntent.getService(this, 1, Intent(this, AetherVpnService::class.java).apply { action = ACTION_STOP }, flags)

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("AetherST Tunnel")
            .setContentText(statusText)
            .setSmallIcon(R.drawable.ic_stat_aether)
            .setOngoing(true)
            .setContentIntent(contentIntent)
            .addAction(android.R.drawable.ic_menu_close_clear_cancel, "Disconnect", stopIntent)
            .setOnlyAlertOnce(true)
            .setSilent(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setPriority(if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.O_MR1) NotificationCompat.PRIORITY_MAX else NotificationCompat.PRIORITY_DEFAULT)
            .build()
    }

    private fun showDisconnectionAlert(reason: String) {
        try {
            val flags = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            val contentIntent = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), flags)
            val reconnectIntent = PendingIntent.getService(
                this, 2,
                Intent(this, AetherVpnService::class.java).apply { action = ACTION_START },
                flags
            )

            val notification = NotificationCompat.Builder(this, ALERT_CHANNEL_ID)
                .setContentTitle("⚠️ VPN Disconnected")
                .setContentText("Connection lost unexpectedly. Tap to reconnect.")
                .setStyle(
                    NotificationCompat.BigTextStyle()
                        .bigText("AetherST Tunnel was disconnected unexpectedly.\nReason: $reason\n\nTap 'Reconnect' to restore your secure connection.")
                )
                .setSmallIcon(R.drawable.ic_stat_aether)
                .setAutoCancel(true)
                .setContentIntent(contentIntent)
                .addAction(android.R.drawable.ic_popup_sync, "Reconnect", reconnectIntent)
                .setCategory(NotificationCompat.CATEGORY_MESSAGE)
                .setPriority(NotificationCompat.PRIORITY_HIGH)
                .setColor(0xFFFF3B30.toInt())
                .build()

            val manager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
            manager.notify(ALERT_NOTIFICATION_ID, notification)
            LogRepository.i("[VpnService] Disconnection alert notification sent")
        } catch (e: Exception) {
            LogRepository.e("[VpnService] Failed to send disconnection alert: ${e.message}")
        }
    }

    private fun createNotificationChannel() {
        val statusChannel = NotificationChannel(CHANNEL_ID, "AetherST Tunnel", NotificationManager.IMPORTANCE_DEFAULT).apply {
            setSound(null, null)
            enableVibration(false)
            enableLights(false)
            setShowBadge(false)
        }
        val alertChannel = NotificationChannel(ALERT_CHANNEL_ID, "AetherST Alerts", NotificationManager.IMPORTANCE_HIGH).apply {
            description = "Notifications for unexpected disconnections"
            enableVibration(true)
            enableLights(true)
            setShowBadge(true)
        }
        val manager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        manager.createNotificationChannel(statusChannel)
        manager.createNotificationChannel(alertChannel)
    }

    override fun onDestroy() {
        scope.cancel()
        super.onDestroy()
    }
}
