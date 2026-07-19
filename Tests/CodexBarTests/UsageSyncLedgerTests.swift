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
    func `ledger rejects unsafe opaque identifiers`() {
        for event in [
            Self.event(key: "event-account", accountID: "account/path"),
            Self.event(key: "event-machine", machineID: "machine/path"),
            Self.event(key: "event-session", sessionID: "session/path"),
            Self.event(key: "event-project", projectID: "project/path"),
        ] {
            #expect(throws: UsageSyncLedgerError.invalidIdentifier) {
                try event.validated()
            }
        }
    }

    private static func event(
        key: String,
        accountID: String = "account-hash",
        machineID: String = "machine-hash",
        sessionID: String? = "session-hash",
        projectID: String? = "project-hash") -> UsageSyncEvent
    {
        UsageSyncEvent(
            idempotencyKey: key,
            source: .codexBar,
            provider: .codex,
            accountID: accountID,
            machineID: machineID,
            occurredAt: Date(timeIntervalSince1970: 1),
            sessionID: sessionID,
            projectID: projectID,
            inputTokens: 10,
            outputTokens: 20,
            cacheReadTokens: 3,
            cacheWriteTokens: 4,
            estimatedCost: 0.15,
            currencyCode: "USD")
    }
}
