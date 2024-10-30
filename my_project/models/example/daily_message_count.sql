-- models/daily_message_count.sql
WITH daily_counts AS (
    SELECT
        channel,
        DATE(created_at) AS date,
        COUNT(*) AS message_count
    FROM telegram_messages
    GROUP BY channel, DATE(created_at)
)
SELECT * FROM daily_message_count
