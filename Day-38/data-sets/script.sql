CREATE TABLE Customer (
    CustomerID VARCHAR(10) PRIMARY KEY,
    CustomerName NVARCHAR(100),
    Email NVARCHAR(100),
    PhoneNumber NVARCHAR(20)
);

INSERT INTO Customer (CustomerID, CustomerName, Email, PhoneNumber) VALUES
('C001', 'Alice Johnson', 'alice.johnson@example.com', '9876543210'),
('C002', 'Bob Smith', 'bob.smith@example.com', '8765432109'),
('C003', 'Charlie Brown', 'charlie.brown@example.com', '7654321098'),
('C004', 'Diana Lee', 'diana.lee@example.com', '6543210987');
