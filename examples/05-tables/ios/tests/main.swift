import Foundation

let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
defer { try? FileManager.default.removeItem(at: dir) }
let client = ZeBridge(options: ["natsUrl": CommandLine.arguments[1], "principal": "tester",
                                "tables": ["wr_rows"], "clientId": "swift", "dbPath": dir.appendingPathComponent("replica.sqlite3").path])
client.sync()
@discardableResult
func write(_ uid: String, after: String? = nil, independent: Bool = false) throws -> String {
    try client.mutate(table: "wr_rows", op: "INSERT", key: ["uid": uid], values: ["uid": uid],
                      after: after, independent: independent)
}
let parent = try write("parent")
try write("unrelated")
let child = try write("child", after: parent)
try write("independent", independent: true)
try write("tail")
let rows = try client.query(sql: "SELECT ordered, predecessor FROM _zebridge_outbox ORDER BY created_at")
precondition(rows.count == 5 && rows[2]["predecessor"] as? String == parent &&
             rows[3]["ordered"] as? Int == 0 && rows[3]["predecessor"] is NSNull &&
             rows[4]["predecessor"] as? String == child, "Unexpected outbox: \(rows)")
