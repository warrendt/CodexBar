import Foundation
import Testing
@testable import CodexBarCore

struct UsageSyncLedgerTests {
    @Test
    func `ledger preserves order deduplicates and acknowledges events`() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let ledger = try UsageSyncLedger(databaseURL: directory.appendingPathComponent("outbox.sqlite"))
        let first = Self.event(key: "event-one")
        let second = Self.event(key: "event-two")

        try await ledger.enqueue([first, second, first])

        #expect(try await ledger.pending().map(\.idempotencyKey) == ["event-one", "event-two"])
        try await ledger.acknowledge(idempotencyKeys: ["event-one"])
        #expect(try await ledger.pending().map(\.idempotencyKey) == ["event-two"])
    }

    @Test
    func `ledger rejects invalid privacy identifiers and negative values`() {
        let event = UsageSyncEvent(
            idempotencyKey: "event",
            source: .codexBar,
            provider: .codex,
            accountID: "account/path",
            machineID: "machine",
            occurredAt: Date(),
            inputTokens: -1)

        #expect(throws: UsageSyncLedgerError.invalidIdentifier) {
            try event.validated()
        }
    }

    private static func event(key: String) -> UsageSyncEvent {
        UsageSyncEvent(
            idempotencyKey: key,
            source: .codexBar,
            provider: .codex,
            accountID: "account-hash",
            machineID: "machine-hash",
            occurredAt: Date(timeIntervalSince1970: 1),
            sessionID: "session-hash",
            projectID: "project-hash",
            inputTokens: 10,
            outputTokens: 20,
            cacheReadTokens: 3,
            cacheWriteTokens: 4,
            estimatedCost: 0.15,
            currencyCode: "USD")
    }
}
