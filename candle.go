package tickstore

import (
	"fmt"
	"time"
)

// CandleData represents OHLC candle data
type CandleData struct {
	InstrumentToken uint32
	TimeStamp       time.Time
	Open            float64
	High            float64
	Low             float64
	Close           float64
}

// Candles is an array of CandleData
type Candles []CandleData

// Creates OHLC candle from tickdata
func (c *Client) FetchCandle(instrumentToken int, startTime time.Time, endTime time.Time) (Candles, error) {
	startT := startTime.Format("2006-01-02 15:04:05")

	endT := endTime.Format("2006-01-02 15:04:05")

	// DB query to calculate OHLC between StartTime and EndTime for given instrument_token based on tickdata
	candleQueryStmt := fmt.Sprintf(`SELECT
			instrument_token,
			time_minute,
			groupArray(price)[1] AS open,
			max(price) AS high,
			min(price) AS low,
			groupArray(price)[-1] AS close
		FROM
		(
			SELECT
				instrument_token,
				toStartOfMinute(timestamp) AS time_minute,
				price
			FROM tickdata
			WHERE (instrument_token = %d) AND
			(timestamp >= '%s') AND
			(timestamp <= '%s')
		)
		GROUP BY (instrument_token, time_minute)
		ORDER BY time_minute ASC`, instrumentToken, startT, endT)

	rows, err := c.dbClient.Query(candleQueryStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candleArray Candles
	for rows.Next() {
		var (
			token       uint32
			time_minute time.Time
			open        float64
			high        float64
			low         float64
			close       float64
		)
		if err := rows.Scan(&token, &time_minute, &open, &high, &low, &close); err != nil {
			return nil, err
		}
		candle := CandleData{
			InstrumentToken: token,
			TimeStamp:       time_minute,
			Open:            open,
			High:            high,
			Low:             low,
			Close:           close,
		}
		candleArray = append(candleArray, candle)
	}

	return candleArray, nil

}

// Create n minute candles from tickdata
func (c *Client) Fetch3MinuteCandle(instrumentToken int, startTime time.Time, endTime time.Time) (CandleData, error) {
	startT := startTime.Format("2006-01-02 15:04:05")

	endT := endTime.Format("2006-01-02 15:04:05")

	// DB query to calculate n minute candles between StartTime and EndTime for given instrument_token based on tickdata
	candleQueryStmt := fmt.Sprintf(`WITH price_select AS (SELECT price
		 	FROM tickdata
		 	FINAL
		 	WHERE (instrument_token = %d) AND
			(timestamp >= toDateTime('%s')) AND
			(timestamp <= toDateTime('%s'))
			ORDER BY timestamp ASC)
	 	SELECT groupArray(price)[1] AS open,
		 	max(price) AS high,
		 	min(price) AS low,
		 	groupArray(price)[-1] AS close FROM price_select;`, instrumentToken, startT, endT)

	rows, err := c.dbClient.Query(candleQueryStmt)
	if err != nil {
		return CandleData{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			open  float64
			high  float64
			low   float64
			close float64
		)
		if err := rows.Scan(&open, &high, &low, &close); err != nil {
			return CandleData{}, err
		}
		candle := CandleData{
			InstrumentToken: uint32(instrumentToken),
			TimeStamp:       startTime,
			Open:            open,
			High:            high,
			Low:             low,
			Close:           close,
		}
		return candle, nil
	}
	return CandleData{}, nil

}

// WITH w AS (
//     SELECT open, high, low, close,
//     time, intDiv(toUnixTimestamp(toDateTime('2024-04-08 19:30:00')) - toUnixTimestamp(time), 60*3(in minutes of interval)) as grp
//     FROM tickdata
//     WHERE (instrument_token = 109122823) AND
//     (timestamp >= toDateTime('2024-04-08 19:30:00')) AND
//     (timestamp <= toDateTime('2024-04-08 19:40:00')) order by time asc
// )
// SELECT
//     first_value(time) as time, first_value(open) as open,
//     max(high) as high, min(low) as low,
//     last_value(close) as close
// FROM
//     w
// GROUP BY grp ORDER BY time asc;

//query to get 3 minute candles
// WITH price_select AS (SELECT price
// 	FROM tickdata
// 	FINAL
// 	WHERE (instrument_token = 109122823) AND
// 	(timestamp >= toDateTime('2024-04-08 19:30:00')) AND
// 	(timestamp <= toDateTime('2024-04-08 19:33:00'))
// 	ORDER BY timestamp ASC)
// 	SELECT groupArray(price)[1] AS open,
// 	max(price) AS high,
// 	min(price) AS low,
// 	groupArray(price)[-1] AS close FROM price_select;
