import Combine
import Foundation

struct PollReport: Codable {
    var applied: Int
    var settled: Int
    var changed_tables: [String]?
    var seeded: [String]?
}

class ZeBridge: ObservableObject {
    private var handle: UInt64 = 0
    @Published var tenant: String = "—"
    
    init(options: [String: Any]) {
        guard let jsonData = try? JSONSerialization.data(withJSONObject: options),
              let jsonStr = String(data: jsonData, encoding: .utf8) else {
            return
        }
        
        jsonStr.withCString { cStr in
            self.handle = zb_client_connect(cStr)
        }
        
        if self.handle == 0 {
            print("Failed to open ZeBridge client")
        }
    }
    
    func sync() {
        guard handle != 0 else { return }
        
        if let resPtr = zb_client_sync(handle) {
            let resStr = String(cString: resPtr)
            zb_free(resPtr)
            
            if let data = resStr.data(using: .utf8),
               let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let t = dict["tenant"] as? String {
                DispatchQueue.main.async {
                    self.tenant = t
                }
            }
        }
    }
    
    func query(sql: String, params: [Any] = []) throws -> [[String: Any]] {
        guard handle != 0 else { return [] }
        
        let paramsData = try JSONSerialization.data(withJSONObject: params)
        let paramsStr = String(data: paramsData, encoding: .utf8) ?? "[]"
        
        var resultStr: String?
        
        sql.withCString { sqlC in
            paramsStr.withCString { paramsC in
                if let resPtr = zb_client_query(handle, sqlC, paramsC) {
                    resultStr = String(cString: resPtr)
                    zb_free(resPtr)
                }
            }
        }
        
        guard let resStr = resultStr, let data = resStr.data(using: .utf8) else { return [] }
        let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any] ?? [:]
        
        if let err = dict["error"] as? String {
            throw NSError(domain: "ZeBridge", code: 1, userInfo: [NSLocalizedDescriptionKey: err])
        }
        
        guard let columns = dict["columns"] as? [String],
              let rows = dict["rows"] as? [[Any]] else {
            return []
        }
        
        return rows.map { row in
            var map: [String: Any] = [:]
            for (i, col) in columns.enumerated() {
                if i < row.count {
                    map[col] = row[i]
                }
            }
            return map
        }
    }
    
    @discardableResult
    func mutate(table: String, op: String, key: [String: Any], values: [String: Any]? = nil,
                after: String? = nil, independent: Bool = false) throws -> String {
        func json(_ object: [String: Any]) throws -> String {
            String(decoding: try JSONSerialization.data(withJSONObject: object), as: UTF8.self)
        }
        var options: [String: Any] = ["independent": independent]
        options["after"] = after
        guard let ptr = zb_client_mutate_opts(handle, table, op, try json(key),
                                              try values.map(json), try json(options)) else {
            throw NSError(domain: "ZeBridge", code: 1, userInfo: [NSLocalizedDescriptionKey: "No mutation reply"])
        }
        defer { zb_free(ptr) }
        let reply = try JSONSerialization.jsonObject(with: Data(String(cString: ptr).utf8)) as? [String: Any]
        guard let id = reply?["msgId"] as? String else {
            throw NSError(domain: "ZeBridge", code: 1, userInfo: [NSLocalizedDescriptionKey:
                reply?["error"] as? String ?? "Missing msgId in mutation reply"])
        }
        return id
    }

    func poll(waitMs: UInt64) -> PollReport? {
        guard handle != 0 else { return nil }
        
        var report: PollReport?
        if let resPtr = zb_client_poll(handle, waitMs) {
            let resStr = String(cString: resPtr)
            zb_free(resPtr)
            
            if let data = resStr.data(using: .utf8) {
                report = try? JSONDecoder().decode(PollReport.self, from: data)
            }
        }
        return report
    }
    
    func flush(waitMs: UInt64) {
        guard handle != 0 else { return }
        if let resPtr = zb_client_flush_outbox(handle, waitMs) {
            zb_free(resPtr)
        }
    }
    
    deinit {
        if handle != 0 {
            zb_client_close(handle)
        }
    }
}
