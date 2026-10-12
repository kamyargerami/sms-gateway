CREATE TABLE users
(
    id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    balance    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sms_records
(
    id         VARCHAR(36) DEFAULT (UUID()) PRIMARY KEY,
    user_id    INT UNSIGNED NOT NULL,
    to_number  VARCHAR(20) NOT NULL,
    text       TEXT        NOT NULL,
    status     ENUM('PENDING', 'DELIVERED', 'FAILED', 'EXPIRED') NOT NULL DEFAULT 'PENDING',
    is_express BOOLEAN     NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMP(3) NULL DEFAULT NULL,
    created_at TIMESTAMP            DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP            DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    INDEX      idx_user_created (user_id, created_at DESC)
);

CREATE TABLE credits
(
    id         VARCHAR(36) DEFAULT (UUID()) PRIMARY KEY,
    user_id    INT UNSIGNED NOT NULL,
    amount     BIGINT      NOT NULL, -- signed: SUM(amount) per user equals users.balance
    type       ENUM('TOPUP', 'SMS_SENT', 'REFUND') NOT NULL,
    created_at TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    INDEX      idx_user_created (user_id, created_at DESC),
    CONSTRAINT chk_credit_amount_sign CHECK (
        (type = 'SMS_SENT' AND amount < 0) OR (type IN ('TOPUP', 'REFUND') AND amount > 0)
    )
);

-- Insert a test user
INSERT INTO users (id, balance)
VALUES (1, 0);
