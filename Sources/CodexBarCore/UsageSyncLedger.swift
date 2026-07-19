import Foundation
#if canImport(SQLite3)
import SQLite3
#elseif canImport(CSQLite3)
import CSQLite3
#endif

/// Versioned, privacy-preserving event sent by local usage collectors to a sync service.
///
/// Callers must provide opaque account, machine, session, and project identifiers. Do not put
/// credentials, prompts, filesystem paths, or raw provider logs in this payload.
public struct UsageSyncEvent: Codable, Equatable, Sendable, Identifiable {
    public static let currentSchemaVersion = 1

    public enum Source: String, Codable, Sendable {
        case codexBar
        case codeburn
        case providerAPI
    }

    public let schemaVersion: Int
    public let idempotencyKey: String
    public let source: Source
    public let provider: UsageProvider
    public let accountID: String
    public let machineID: String
    public let occurredAt: Date
    public let sessionID: String?
    public let projectID: String?
    public let model: String?
    public let inputTokens: Int?
    public let outputTokens: Int?
    public let cacheReadTokens: Int?
    public let cacheWriteTokens: Int?
    public let estimatedCost: Decimal?
    public let currencyCode: String?

    public var id: String { self.idempotencyKey }

    public init(
        idempotencyKey: String,
        source: Source,
        provider: UsageProvider,
        accountID: String,
        machineID: String,
        occurredAt: Date,
        sessionID: String? = nil,
        projectID: String? = nil,
        model: String? = nil,
        inputTokens: Int? = nil,
        outputTokens: Int? = nil,
        cacheReadTokens: Int? = nil,
        cacheWriteTokens: Int? = nil,
        estimatedCost: Decimal? = nil,
        currencyCode: String? = nil,
        schemaVersion: Int = UsageSyncEvent.currentSchemaVersion)
    {
        self.schemaVersion = schemaVersion
        self.idempotencyKey = idempotencyKey
        self.source = source
        self.provider = provider
        self.accountID = accountID
        self.machineID = machineID
        self.occurredAt = occurredAt
        self.sessionID = sessionID
        self.projectID = projectID
        self.model = model
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.cacheReadTokens = cacheReadTokens
        self.cacheWriteTokens = cacheWriteTokens
        self.estimatedCost = estimatedCost
        self.currencyCode = currencyCode
    }

    public func validated() throws -> Self {
        guard self.schemaVersion == Self.currentSchemaVersion else {
            throw UsageSyncLedgerError.unsupportedSchemaVersion(self.schemaVersion)
        }
        for value in [self.idempotencyKey, self.accountID, self.machineID] {
            guard Self.isOpaqueIdentifier(value) else {
                throw UsageSyncLedgerError.invalidIdentifier
            }
        }
        for value in [self.inputTokens, self.outputTokens, self.cacheReadTokens, self.cacheWriteTokens] {
            guard value.map({ $0 >= 0 }) ?? true else {
                throw UsageSyncLedgerError.negativeTokenCount
            }
        }
        guard self.estimatedCost.map({ $0 >= 0 }) ?? true else {
            throw UsageSyncLedgerError.negativeCost
        }
        if let currencyCode = self.currencyCode {
            let normalized = currencyCode.trimmingCharacters(in: .whitespacesAndNewlines)
            guard normalized.count == 3, normalized.allSatisfy(\.isLetter) else {
                throw UsageSyncLedgerError.invalidCurrencyCode
            }
        }
        return self
    }

    private static func isOpaqueIdentifier(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed.count <= 256 && !trimmed.contains("/")
    }
}

public enum UsageSyncLedgerError: Error, Equatable, Sendable {
    case unavailable
    case cannotOpenDatabase
    case databaseFailure
    case encodingFailure
    case unsupportedSchemaVersion(Int)
    case invalidIdentifier
    case negativeTokenCount
    case negativeCost
    case invalidCurrencyCode
}

/// Durable local outbox for opt-in usage synchronization.
///
/// The caller owns transport and credentials; this store only persists validated events until the
/// server acknowledges their idempotency keys.
public actor UsageSyncLedger {
    private let databaseURL: URL

    public init(databaseURL: URL) throws {
        #if canImport(SQLite3) || canImport(CSQLite3)
        self.databaseURL = databaseURL
        try Self.prepareDatabase(at: databaseURL)
        #else
        throw UsageSyncLedgerError.unavailable
        #endif
    }

    public func enqueue(_ events: [UsageSyncEvent]) throws {
        #if canImport(SQLite3) || canImport(CSQLite3)
        guard !events.isEmpty else { return }
        let encoded = try events.map { event -> (String, Data) in
            let validated = try event.validated()
            guard let data = try? Self.encoder.encode(validated) else {
                throw UsageSyncLedgerError.encodingFailure
            }
            return (validated.idempotencyKey, data)
        }
        try self.withDatabase { database in
            try Self.execute(database, sql: "BEGIN IMMEDIATE TRANSACTION")
            do {
                let statement = try Self.statement(
                    database,
                    sql: "INSERT OR IGNORE INTO usage_sync_outbox (idempotency_key, payload) VALUES (?1, ?2)")
                defer { sqlite3_finalize(statement) }
                for (key, payload) in encoded {
                    sqlite3_reset(statement)
                    sqlite3_clear_bindings(statement)
                    try Self.bind(key, to: statement, index: 1)
                    try Self.bind(payload, to: statement, index: 2)
                    guard sqlite3_step(statement) == SQLITE_DONE else {
                        throw UsageSyncLedgerError.databaseFailure
                    }
                }
                try Self.execute(database, sql: "COMMIT")
            } catch {
                _ = try? Self.execute(database, sql: "ROLLBACK")
                throw error
            }
        }
        #else
        throw UsageSyncLedgerError.unavailable
        #endif
    }

    public func pending(limit: Int = 100) throws -> [UsageSyncEvent] {
        #if canImport(SQLite3) || canImport(CSQLite3)
        let boundedLimit = max(1, min(limit, 1_000))
        return try self.withDatabase { database in
            let statement = try Self.statement(
                database,
                sql: "SELECT payload FROM usage_sync_outbox ORDER BY sequence LIMIT ?1")
            defer { sqlite3_finalize(statement) }
            guard sqlite3_bind_int(statement, 1, Int32(boundedLimit)) == SQLITE_OK else {
                throw UsageSyncLedgerError.databaseFailure
            }
            var events: [UsageSyncEvent] = []
            while sqlite3_step(statement) == SQLITE_ROW {
                guard let bytes = sqlite3_column_blob(statement, 0) else {
                    throw UsageSyncLedgerError.databaseFailure
                }
                let length = Int(sqlite3_column_bytes(statement, 0))
                let data = Data(bytes: bytes, count: length)
                guard let event = try? Self.decoder.decode(UsageSyncEvent.self, from: data) else {
                    throw UsageSyncLedgerError.databaseFailure
                }
                events.append(event)
            }
            return events
        }
        #else
        throw UsageSyncLedgerError.unavailable
        #endif
    }

    public func acknowledge(idempotencyKeys: [String]) throws {
        #if canImport(SQLite3) || canImport(CSQLite3)
        guard !idempotencyKeys.isEmpty else { return }
        try self.withDatabase { database in
            let statement = try Self.statement(
                database,
                sql: "DELETE FROM usage_sync_outbox WHERE idempotency_key = ?1")
            defer { sqlite3_finalize(statement) }
            for key in Set(idempotencyKeys) {
                sqlite3_reset(statement)
                sqlite3_clear_bindings(statement)
                try Self.bind(key, to: statement, index: 1)
                guard sqlite3_step(statement) == SQLITE_DONE else {
                    throw UsageSyncLedgerError.databaseFailure
                }
            }
        }
        #else
        throw UsageSyncLedgerError.unavailable
        #endif
    }
}

#if canImport(SQLite3) || canImport(CSQLite3)
private extension UsageSyncLedger {
    static let encoder: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }()

    static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }()

    static func prepareDatabase(at databaseURL: URL) throws {
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true)
        try self.withDatabase(at: databaseURL) { database in
            try self.execute(database, sql: """
            CREATE TABLE IF NOT EXISTS usage_sync_outbox (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL UNIQUE,
                payload BLOB NOT NULL
            )
            """)
        }
    }

    func withDatabase<T>(_ body: (OpaquePointer) throws -> T) throws -> T {
        try Self.withDatabase(at: self.databaseURL, body)
    }

    static func withDatabase<T>(at url: URL, _ body: (OpaquePointer) throws -> T) throws -> T {
        var database: OpaquePointer?
        guard sqlite3_open_v2(url.path, &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK,
              let database
        else {
            if let database {
                sqlite3_close(database)
            }
            throw UsageSyncLedgerError.cannotOpenDatabase
        }
        defer { sqlite3_close(database) }
        sqlite3_busy_timeout(database, 1_000)
        return try body(database)
    }

    static func execute(_ database: OpaquePointer, sql: String) throws {
        guard sqlite3_exec(database, sql, nil, nil, nil) == SQLITE_OK else {
            throw UsageSyncLedgerError.databaseFailure
        }
    }

    static func statement(_ database: OpaquePointer, sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
              let statement
        else {
            throw UsageSyncLedgerError.databaseFailure
        }
        return statement
    }

    static func bind(_ value: String, to statement: OpaquePointer, index: Int32) throws {
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        guard sqlite3_bind_text(statement, index, value, -1, transient) == SQLITE_OK else {
            throw UsageSyncLedgerError.databaseFailure
        }
    }

    static func bind(_ value: Data, to statement: OpaquePointer, index: Int32) throws {
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        let result = value.withUnsafeBytes { bytes in
            sqlite3_bind_blob(statement, index, bytes.baseAddress, Int32(value.count), transient)
        }
        guard result == SQLITE_OK else {
            throw UsageSyncLedgerError.databaseFailure
        }
    }
}
#endif
