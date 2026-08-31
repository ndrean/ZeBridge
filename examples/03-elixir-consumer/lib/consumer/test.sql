INSERT INTO test_types (age, temperature, price, is_true, some_text, tags, matrix, metadata) VALUES
(
    29,
    123456.78901234,
    36.6,
    true,
    'Sample text with special characters: ñ, ü, 漢字',
    ARRAY['foo', 'bar', 'hello world'],
    ARRAY[[1,2,3], [4,5,6]],
    '{"name": "Alice", "age": 30, "active": true, "scores": [95, 87, 92]}'
)

INSERT INTO test_types (age, temperature, price, is_true, some_text, tags, matrix, metadata) VALUES
(
    29,
    -0.00000001,
    -12345.6789,
    false,
    'Another text with newline:\nSecond line here.',
    ARRAY[]::TEXT[],
    ARRAY[[0]],
    '{"nested": {"deep": {"value": 42}}, "list": [1, "two", null, false]}'
);

INSERT INTO test_types (age, temperature, price, is_true, some_text, tags, matrix, metadata) VALUES
(
  30,
    0.0,
    999999999999.99999999,
    true,
    'Text with quotes: "To be, or not to be."',
    ARRAY['single'],
    NULL,
    'null'
);

INSERT INTO test_types (age, temperature, price, is_true, some_text, tags, matrix, metadata) VALUES
(
    0,
    0,
    -40.0,
    false,
    'Some text here',
    ARRAY['a', 'b', 'c', NULL, 'e'],
    ARRAY[[10,20], [30,40], [50,60]],
    '{"emoji": "🎉", "quote": "he said \"hello\"", "newline": "line1\nline2"}'
);

UPDATE test_types SET price = 0.12345, temperature = 99.9 WHERE id = 1;
DELETE FROM test_types WHERE age = 30;