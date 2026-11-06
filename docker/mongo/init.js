// Payment events emitted by the payments service (append-only).
db = db.getSiblingDB("factoring");
db.createCollection("payment_events");
db.payment_events.createIndex({ event_ts: 1 });
db.payment_events.createIndex({ payment_id: 1 }, { unique: true });

db.payment_events.insertMany([
  {
    payment_id: "PAY-9000001",
    type: "payment.received",
    invoice: { id: "INV-900001" },
    amount: NumberDecimal("12500.00"),
    method: "ach",
    paid_at: new Date("2026-11-01T14:00:00Z"),
    event_ts: new Date("2026-11-01T14:05:00Z"),
  },
  {
    payment_id: "PAY-9000002",
    type: "payment.received",
    invoice: { id: "INV-900002" },
    amount: NumberDecimal("4000.00"),
    method: "wire",
    paid_at: new Date("2026-11-01T16:30:00Z"),
    event_ts: new Date("2026-11-01T16:31:00Z"),
  },
]);
