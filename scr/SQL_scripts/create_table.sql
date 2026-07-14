CREATE TABLE IF NOT EXISTS health_data (
    year UInt16,
    gender String,
    age UInt8,
    location String,
    race_AfricanAmerican UInt8,
    race_Asian UInt8,
    race_Caucasian UInt8,
    race_Hispanic UInt8,
    race_Other UInt8,
    hypertension UInt8,
    heart_disease UInt8,
    smoking_history String,
    bmi Float32,
    hbA1c_level Float32,
    blood_glucose_level UInt16,
    diabetes UInt8
) ENGINE = MergeTree()
ORDER BY (year, location, age);