package dev.zebridge

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * The AAR on a real device against the dev stack (scripts/device-test.sh): NATS reached
 * through `adb reverse tcp:4222`, as the dev principal `omar`, whose creds the script
 * copies into the TEST apk's assets. Only the public API, the way an app uses it.
 */
@RunWith(AndroidJUnit4::class)
class ZeBridgeDeviceTest {
    private val ctx = InstrumentationRegistry.getInstrumentation().targetContext
    private val args = InstrumentationRegistry.getArguments()
    private val creds = InstrumentationRegistry.getInstrumentation().context.assets.open("omar.creds").bufferedReader().readText()

    private fun options(extra: Map<String, Any?> = emptyMap()) = mapOf(
        "natsUrl" to "nats://127.0.0.1:4222",
        "creds" to creds,
        "principal" to "omar",
        "clientId" to "android-device-test",
        "tables" to listOf("test_types"),
    ) + extra

    @Test
    fun abiAndGrammar() {
        assertEquals(ZB_ABI, Native.abiVersion())
        args.getString("grammarHash")?.let { assertEquals("the bridge's grammar", it, ZeBridge.grammarHash()) }
    }

    @Test
    fun seedQueryWriteEchoDelete() {
        val settled = CountDownLatch(2) // the INSERT's verdict, then the DELETE's
        ctx.deleteDatabase("zebridge.sqlite3")
        val zb = ZeBridge.connect(options(), ctx, listener = { r -> repeat(r.optInt("settled")) { settled.countDown() } })
        try {
            assertEquals(listOf("kilo"), zb.tenants)
            val live = zb.query("SELECT count(*) AS n FROM test_types WHERE deleted_at IS NULL")[0]["n"] as Number
            args.getString("expectedLive")?.let { assertEquals("live rows, as PostgreSQL counts them", it.toLong(), live.toLong()) }

            // UTF-8 both ways: an emoji is outside the BMP, which JNI strings would mangle.
            val text = "écrit sur Android 🌍 — ok"
            assertEquals(text, zb.query("SELECT ? AS s", text)[0]["s"])

            try {
                zb.query("DELETE FROM test_types")
                fail("a write through query must be refused")
            } catch (e: ZeBridgeException) { /* read-only connection */ }

            val uid = UUID.randomUUID().toString()
            val now = Instant.now().truncatedTo(ChronoUnit.MICROS).toString()
            val q = zb.mutate("test_types", "INSERT", mapOf("uid" to uid), mapOf(
                "uid" to uid, "some_text" to text, "age" to 29, "is_true" to true,
                "tenant_id" to zb.tenants[0], "inserted_at" to now, "updated_at" to now,
            ))
            assertTrue("queued: $q", q.has("msgId"))
            // The loop sends and settles on its own; wait for the echo from PostgreSQL.
            var row: Map<String, Any?>? = null
            val until = System.currentTimeMillis() + 10_000
            while (row == null && System.currentTimeMillis() < until) {
                row = zb.query("SELECT some_text, last_writer FROM test_types WHERE uid = ?", uid).firstOrNull()
                    ?.takeIf { it["last_writer"] == "android-device-test" }
                if (row == null) Thread.sleep(100)
            }
            assertEquals("the echo, emoji intact", text, row?.get("some_text"))

            zb.mutate("test_types", "delete", mapOf("uid" to uid)) // lowercase: libzb's core normalises it (§10jm)
            assertTrue("both verdicts reach the listener", settled.await(10, TimeUnit.SECONDS))
            val until2 = System.currentTimeMillis() + 10_000
            var left = 1L
            while (left > 0 && System.currentTimeMillis() < until2) {
                left = (zb.query("SELECT count(*) AS n FROM test_types WHERE uid = ? AND deleted_at IS NULL", uid)[0]["n"] as Number).toLong()
                if (left > 0) Thread.sleep(100)
            }
            assertEquals("deleted, locally too", 0L, left)
            assertFalse(zb.revoked)
        } finally {
            zb.close()
            zb.close() // twice is safe
        }
    }

    @Test
    fun missingEngineSaysWhy() {
        try {
            ZeBridge.connect(options(mapOf("engine" to "duckdb", "dbPath" to ctx.getDatabasePath("zb-test.duckdb").path)))
            fail("a phone has no libduckdb")
        } catch (e: ZeBridgeException) {
            assertTrue(e.message, e.message!!.contains("engine 'duckdb'"))
        }
    }
}
