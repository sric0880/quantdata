CREATE TABLE IF NOT EXISTS ticks_{tablename} AS
    SELECT * FROM read_csv('{csv_path}.csv', header=true, columns = {{
        "dt":"TIMESTAMP",
        "last_price":"FLOAT",
        "volume":"UINTEGER",
        "amount":"UINTEGER",
        "open_interest":"UINTEGER",
        "bid_price1":"FLOAT",
        "ask_price1":"FLOAT",
        "bid_volume1":"UINTEGER",
        "ask_volume1":"UINTEGER",
    }});