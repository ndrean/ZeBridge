import SwiftUI

struct ContentView: View {
    @StateObject private var zb = ZeBridge(options: [
        "url": "ws://localhost:8080",
        "dbPath": "zb.sqlite3",
        "principal": "alice",
        "tables": ["counter_public", "counter_tenant", "app_users", "app_orders"],
        "clientId": "ios-client"
    ])
    
    @State private var counterPublicValue: Int = 0
    @State private var counterPublicDate: String = "—"
    
    @State private var counterTenantValue: Int = 0
    @State private var counterTenantDate: String = "—"
    
    @State private var formUserName: String = ""
    @State private var formItem: String = ""
    @State private var formCount: String = "1"
    @State private var formNote: String = ""
    
    @State private var users: [[String: Any]] = []
    @State private var orders: [[String: Any]] = []
    
    @State private var sqlText: String = "SELECT count, note, updated_at, last_writer FROM app_orders ORDER BY updated_at DESC LIMIT 3"
    @State private var sqlLive: Bool = true
    @State private var sqlRows: [[String: Any]] = []
    @State private var sqlError: String = ""
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {
                    Text("Principal: alice · Tenant: \(zb.tenant)")
                        .font(.subheadline)
                        .foregroundColor(.gray)
                    
                    // Counters
                    VStack(alignment: .leading) {
                        Text("Two Counters").font(.title2).bold()
                        HStack {
                            counterCard(title: "counter_public", value: counterPublicValue, date: counterPublicDate, onBump: bumpPublicCounter)
                            counterCard(title: "counter_tenant", value: counterTenantValue, date: counterTenantDate, onBump: bumpTenantCounter)
                        }
                    }
                    
                    Divider()
                    
                    // Users -> Orders
                    VStack(alignment: .leading, spacing: 10) {
                        Text("Users ⟶ Orders").font(.title2).bold()
                        
                        VStack(spacing: 12) {
                            TextField("User", text: $formUserName).textFieldStyle(RoundedBorderTextFieldStyle())
                            TextField("Item", text: $formItem).textFieldStyle(RoundedBorderTextFieldStyle())
                            HStack {
                                TextField("Count", text: $formCount).keyboardType(.numberPad).textFieldStyle(RoundedBorderTextFieldStyle())
                                TextField("Note", text: $formNote).textFieldStyle(RoundedBorderTextFieldStyle())
                            }
                            Button(action: createOrder) {
                                Text("CREATE ORDER")
                                    .frame(maxWidth: .infinity)
                                    .padding()
                                    .background(Color.blue)
                                    .foregroundColor(.white)
                                    .cornerRadius(8)
                            }
                        }
                        
                        Text("Orders List").font(.headline).padding(.top)
                        ForEach(0..<orders.count, id: \.self) { i in
                            let o = orders[i]
                            let item = o["item"] as? String ?? ""
                            let count = o["count"] as? Int ?? 1
                            let userId = o["user_id"] as? String ?? ""
                            let note = o["note"] as? String
                            let uName = users.first(where: { ($0["uid"] as? String) == userId })?["name"] as? String ?? userId
                            
                            VStack(alignment: .leading) {
                                Text("\(item) x \(count)").bold()
                                Text("\(uName)\(note != nil ? " · " + note! : "")").foregroundColor(.gray)
                            }
                            .padding(.vertical, 4)
                            Divider()
                        }
                    }
                    
                    // SQL Console
                    VStack(alignment: .leading, spacing: 10) {
                        Text("SQL Console").font(.title2).bold()
                        TextEditor(text: $sqlText)
                            .frame(height: 80)
                            .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.gray.opacity(0.5)))
                        
                        HStack {
                            Button("RUN") { runSql(query: sqlText) }
                                .padding(.horizontal, 20)
                                .padding(.vertical, 10)
                                .background(Color.purple)
                                .foregroundColor(.white)
                                .cornerRadius(8)
                            
                            Toggle("Live", isOn: $sqlLive)
                        }
                        
                        if !sqlError.isEmpty {
                            Text(sqlError).foregroundColor(.red)
                        }
                        
                        if !sqlRows.isEmpty {
                            ScrollView(.horizontal) {
                                VStack(alignment: .leading) {
                                    // Basic tabular display
                                    let keys = Array(sqlRows[0].keys).sorted()
                                    HStack {
                                        ForEach(keys, id: \.self) { k in
                                            Text(k).bold().frame(width: 100, alignment: .leading)
                                        }
                                    }
                                    Divider()
                                    ForEach(0..<sqlRows.count, id: \.self) { i in
                                        HStack {
                                            ForEach(keys, id: \.self) { k in
                                                Text("\(sqlRows[i][k] ?? "")").frame(width: 100, alignment: .leading)
                                            }
                                        }
                                        Divider()
                                    }
                                }
                            }
                        }
                    }
                }
                .padding()
            }
            .navigationTitle("ZeBridge iOS")
            .onAppear {
                zb.sync()
                refreshAll()
                startPolling()
            }
        }
    }
    
    func counterCard(title: String, value: Int, date: String, onBump: @escaping (Int) -> Void) -> some View {
        VStack {
            Text(title).font(.subheadline)
            HStack {
                Button("-") { onBump(-1) }.font(.title).frame(width: 30)
                Text("\(value)").font(.title).bold().frame(minWidth: 40)
                Button("+") { onBump(1) }.font(.title).frame(width: 30)
            }
            Text("v: \(date)").font(.caption2).foregroundColor(.gray)
        }
        .padding()
        .background(Color(UIColor.secondarySystemBackground))
        .cornerRadius(8)
    }
    
    func refreshAll(changedTables: [String]? = nil) {
        if changedTables == nil || changedTables!.contains("counter_public") {
            if let res = try? zb.query(sql: "SELECT value, updated_at FROM counter_public WHERE uid = '00000000-0000-4000-8000-00000000c0de'"), !res.isEmpty {
                counterPublicValue = res[0]["value"] as? Int ?? 0
                counterPublicDate = res[0]["updated_at"] as? String ?? "—"
            }
        }
        if changedTables == nil || changedTables!.contains("counter_tenant") {
            if let res = try? zb.query(sql: "SELECT value, updated_at FROM counter_tenant LIMIT 1"), !res.isEmpty {
                counterTenantValue = res[0]["value"] as? Int ?? 0
                counterTenantDate = res[0]["updated_at"] as? String ?? "—"
            }
        }
        if changedTables == nil || changedTables!.contains("app_users") {
            if let res = try? zb.query(sql: "SELECT uid, name FROM app_users WHERE deleted_at IS NULL ORDER BY name") {
                users = res
            }
        }
        if changedTables == nil || changedTables!.contains("app_orders") {
            if let res = try? zb.query(sql: "SELECT * FROM app_orders WHERE deleted_at IS NULL ORDER BY updated_at DESC") {
                orders = res
            }
        }
        if sqlLive {
            runSql(query: sqlText)
        }
    }
    
    func bumpPublicCounter(delta: Int) {
        let uid = "00000000-0000-4000-8000-00000000c0de"
        let v = ISO8601DateFormatter().string(from: Date())
        
        do {
            if (try? zb.query(sql: "SELECT 1 FROM counter_public WHERE uid = ?", params: [uid]))?.isEmpty != false {
                try zb.mutate(table: "counter_public", op: "INSERT", key: ["uid": uid], values: ["uid": uid, "value": delta, "inserted_at": v, "updated_at": v])
            } else {
                try zb.mutate(table: "counter_public", op: "UPDATE", key: ["uid": uid], values: ["value": counterPublicValue + delta])
            }
            refreshAll(changedTables: ["counter_public"])
        } catch { print(error) }
    }
    
    func bumpTenantCounter(delta: Int) {
        let uid = "00000000-0000-4000-8000-8b0b2beac0df" // Hardcoded for alice
        let v = ISO8601DateFormatter().string(from: Date())
        
        do {
            if (try? zb.query(sql: "SELECT 1 FROM counter_tenant WHERE uid = ?", params: [uid]))?.isEmpty != false {
                try zb.mutate(table: "counter_tenant", op: "INSERT", key: ["uid": uid], values: ["uid": uid, "tenant_id": zb.tenant, "value": delta, "inserted_at": v, "updated_at": v])
            } else {
                try zb.mutate(table: "counter_tenant", op: "UPDATE", key: ["uid": uid], values: ["value": counterTenantValue + delta])
            }
            refreshAll(changedTables: ["counter_tenant"])
        } catch { print(error) }
    }
    
    func createOrder() {
        let userTrim = formUserName.trimmingCharacters(in: .whitespaces)
        let itemTrim = formItem.trimmingCharacters(in: .whitespaces)
        if userTrim.isEmpty || itemTrim.isEmpty { return }
        
        var userId = ""
        if let existing = users.first(where: { ($0["name"] as? String) == userTrim }) {
            userId = existing["uid"] as? String ?? ""
        } else {
            userId = "\(Int(Date().timeIntervalSince1970 * 1000))-user"
            let v = ISO8601DateFormatter().string(from: Date())
            try? zb.mutate(table: "app_users", op: "INSERT", key: ["uid": userId], values: ["uid": userId, "name": userTrim, "tenant_id": zb.tenant, "inserted_at": v, "updated_at": v])
        }
        
        let count = Int(formCount) ?? 1
        let noteTrim = formNote.trimmingCharacters(in: .whitespaces)
        let note: String? = noteTrim.isEmpty ? nil : noteTrim
        let v = ISO8601DateFormatter().string(from: Date())
        
        try? zb.mutate(table: "app_orders", op: "INSERT", key: ["user_id": userId, "item": itemTrim], values: ["user_id": userId, "item": itemTrim, "count": count, "note": note ?? NSNull(), "tenant_id": zb.tenant, "inserted_at": v, "updated_at": v])
        
        formUserName = ""
        formItem = ""
        formCount = "1"
        formNote = ""
        refreshAll(changedTables: ["app_users", "app_orders"])
    }
    
    func runSql(query: String) {
        if query.trimmingCharacters(in: .whitespaces).isEmpty { return }
        do {
            sqlRows = try zb.query(sql: query)
            sqlError = ""
        } catch {
            sqlError = error.localizedDescription
            sqlRows = []
        }
    }
    
    func startPolling() {
        DispatchQueue.global(qos: .background).async {
            while true {
                if let report = zb.poll(waitMs: 1000) {
                    var needsRefresh = false
                    var changed: [String] = []
                    
                    if let seeded = report.seeded, !seeded.isEmpty {
                        needsRefresh = true
                        changed.append(contentsOf: seeded)
                    }
                    if let ct = report.changed_tables, !ct.isEmpty {
                        needsRefresh = true
                        changed.append(contentsOf: ct)
                    }
                    
                    if needsRefresh {
                        DispatchQueue.main.async {
                            refreshAll(changedTables: changed)
                        }
                    }
                }
                zb.flush(waitMs: 0)
                Thread.sleep(forTimeInterval: 0.05)
            }
        }
    }
}
