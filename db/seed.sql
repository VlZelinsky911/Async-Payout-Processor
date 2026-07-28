-- Create the users table
CREATE TABLE "users" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "balance" DECIMAL(10, 2) DEFAULT 0.00,
    "multiplier" DECIMAL(5, 2) DEFAULT 1.00
);

-- Create the payout_ledgers table
CREATE TABLE "payout_ledgers" (
    "id" SERIAL PRIMARY KEY,
    "user_id" INTEGER REFERENCES "users"("id"),
    "event_id" VARCHAR(255) NOT NULL,
    "offer_id" VARCHAR(255) NOT NULL,
    "payout_amount" DECIMAL(10, 2),
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Idempotency: the same Redis Stream event can never be booked twice.
    CONSTRAINT "payout_ledgers_event_id_unique" UNIQUE ("event_id")
);

-- Insert a few sample users
INSERT INTO "users" (name, balance, multiplier) VALUES ('John Doe', 100.00, 1.2);
INSERT INTO "users" (name, balance, multiplier) VALUES ('Jane Smith', 50.00, 1.0);
INSERT INTO "users" (name, balance, multiplier) VALUES ('Peter Jones', 200.00, 1.5);
