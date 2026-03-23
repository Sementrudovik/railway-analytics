-- db/init/04_create_functions.sql
-- Функция для заполнения таблицы дат
CREATE OR REPLACE FUNCTION railway.fill_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO railway.dim_date (
            date_id,
            full_date,
            year,
            quarter,
            month,
            month_name,
            week,
            day_of_week,
            day_name,
            is_weekend
        ) VALUES (
            EXTRACT(YEAR FROM current_date) * 10000 + 
            EXTRACT(MONTH FROM current_date) * 100 + 
            EXTRACT(DAY FROM current_date),
            current_date,
            EXTRACT(YEAR FROM current_date),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(MONTH FROM current_date),
            TO_CHAR(current_date, 'Month'),
            EXTRACT(WEEK FROM current_date),
            EXTRACT(DOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DOW FROM current_date) IN (0, 6)
        )
        ON CONFLICT (full_date) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;