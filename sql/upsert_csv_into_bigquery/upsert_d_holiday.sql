MERGE INTO raw.d_holiday AS target
USING staging.d_holiday AS source
ON target.date = source.date AND target.name = source.name
WHEN NOT MATCHED BY TARGET THEN
  INSERT (date, name)
  VALUES (source.date, source.name);