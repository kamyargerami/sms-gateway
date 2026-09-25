CREATE TABLE users (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    balance INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sms_records (
    id VARCHAR(36) PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    to_number VARCHAR(20) NOT NULL,
    text TEXT NOT NULL,
    status ENUM('PENDING', 'DELIVERED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    is_express BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_user_created (user_id, created_at DESC)
);

CREATE TABLE credits (
    id VARCHAR(36) DEFAULT (UUID()) PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    amount INT NOT NULL,
    type VARCHAR(20) NOT NULL, -- 'TOPUP' or 'SMS_SENT' or 'REFUND'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_user_created (user_id, created_at DESC)
);

-- Insert a test user
INSERT INTO users (id, balance) VALUES (1, 0);
