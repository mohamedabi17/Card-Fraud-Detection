-- Analytics queries for Credit Card Fraud Detection

-- 1. Daily fraud summary
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) as fraud_rate_percent,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(SUM(CASE WHEN class = 1 THEN amount ELSE 0 END), 2) as fraud_amount,
    ROUND(AVG(CASE WHEN class = 0 THEN amount END), 2) as avg_normal_amount,
    ROUND(AVG(CASE WHEN class = 1 THEN amount END), 2) as avg_fraud_amount
FROM credit_card_transactions
GROUP BY DATE(processed_at)
ORDER BY date DESC;

-- 2. Fraud by hour of day
SELECT 
    FLOOR(hour_of_day) as hour,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) as fraud_rate_percent
FROM credit_card_transactions
GROUP BY FLOOR(hour_of_day)
ORDER BY hour;

-- 3. Fraud by amount category
SELECT 
    amount_category,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount
FROM credit_card_transactions
GROUP BY amount_category
ORDER BY 
    CASE amount_category
        WHEN 'zero' THEN 1
        WHEN 'low' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'high' THEN 4
        WHEN 'very_high' THEN 5
    END;

-- 4. Top 10 highest fraud amounts
SELECT 
    transaction_id,
    amount,
    hour_of_day,
    amount_category,
    day_of_dataset
FROM credit_card_transactions
WHERE class = 1
ORDER BY amount DESC
LIMIT 10;

-- 5. Fraud distribution by day of dataset
SELECT 
    day_of_dataset,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) as fraud_rate_percent
FROM credit_card_transactions
GROUP BY day_of_dataset
ORDER BY day_of_dataset;

-- 6. Statistical analysis of amount by class
SELECT 
    class,
    COUNT(*) as count,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(STDDEV(amount), 2) as std_amount,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount), 2) as median_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM credit_card_transactions
GROUP BY class
ORDER BY class;
