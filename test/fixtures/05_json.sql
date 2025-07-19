-- JSON test data
INSERT INTO json (ID, Data, Metadata) VALUES
('json-001', '{"name": "John Doe", "age": 30, "address": {"city": "Tokyo", "country": "Japan"}}', '{"tags": ["admin", "developer"], "active": true, "score": 95.5}'),
('json-002', '{"name": "Jane Smith", "age": 25, "preferences": {"theme": "dark", "notifications": true}}', '{"tags": ["user"], "active": false, "score": 87}'),
('json-003', 'null', '{}'),
('json-004', '{"products": [{"id": 1, "name": "Laptop"}, {"id": 2, "name": "Mouse"}]}', '{"created_at": "2024-01-01T00:00:00Z", "version": 1.0}'),
('json-005', '{"empty_object": {}, "empty_array": [], "boolean_values": [true, false, null]}', '{"test": true}');