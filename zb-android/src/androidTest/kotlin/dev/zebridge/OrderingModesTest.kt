package dev.zebridge

import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assume.assumeNotNull
import org.junit.Test
import java.util.UUID

class OrderingModesTest {
    @Test fun predecessorAndIndependentCrossJni() {
        val url = InstrumentationRegistry.getArguments().getString("orderingUrl")
        assumeNotNull(url)
        val ctx = InstrumentationRegistry.getInstrumentation().targetContext
        val db = ctx.getDatabasePath("ordering-${UUID.randomUUID()}.sqlite3")
        try {
            ZeBridge(mapOf("natsUrl" to url, "tables" to listOf("wr_rows"), "principal" to "tester",
                "clientId" to "android", "dbPath" to db.path), ctx).use { client ->
                fun write(uid: String, after: String? = null, independent: Boolean = false): String {
                    val key = mapOf("uid" to uid)
                    return client.mutate("wr_rows", "INSERT", key, key,
                        after = after, independent = independent).getString("msgId")
                }
                val parent = write("parent")
                write("unrelated")
                val child = write("child", after = parent)
                write("independent", independent = true)
                write("tail")
                val rows = client.query("SELECT ordered, predecessor FROM _zebridge_outbox ORDER BY created_at")
                assertEquals(5, rows.size)
                assertEquals(parent, rows[2]["predecessor"])
                assertEquals(0, (rows[3]["ordered"] as Number).toInt())
                assertNull(rows[3]["predecessor"])
                assertEquals(child, rows[4]["predecessor"])
            }
        } finally {
            ctx.deleteDatabase(db.name)
            java.io.File("${db.path}.lock").delete()
        }
    }
}
