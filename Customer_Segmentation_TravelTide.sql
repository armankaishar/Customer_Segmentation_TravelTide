WITH -- limiting sessions on Elena's suggested timeframe (After Jan 4 2023)
  sessions_2023 AS (
    SELECT *
    FROM sessions s
    where s.session_start > '2023-01-04'
  ),

  -- all users with more than 7 sessions in 2023
  filtered_users AS (
    SELECT user_id, COUNT(*)
    FROM sessions_2023 s
    GROUP BY user_id
    HAVING COUNT(*) > 7
  ),

  -- Main session base table
  -- Each row is a browsing session on the TravelTide App
  -- Sessions have trips connected to them when they were booked or canceled
  session_base AS (
    select
      s.session_id, s.user_id, s.trip_id, s.session_start, s.session_end,
      EXTRACT(EPOCH FROM s.session_end - s.session_start) as session_duration,
      s.page_clicks, s.flight_discount, s.flight_discount_amount, s.hotel_discount,
      s.hotel_discount_amount, s.flight_booked, s.hotel_booked, s.cancellation,
      u.birthdate, u.gender, u.married, u.has_children, u.home_country, u.home_city,
      u.home_airport, u.home_airport_lat, u.home_airport_lon, u.sign_up_date,
      f.origin_airport, f.destination, f.destination_airport, f.seats,
      f.return_flight_booked, f.departure_time, f.return_time, f.checked_bags,
      f.trip_airline, f.destination_airport_lat, f.destination_airport_lon,
      f.base_fare_usd, h.hotel_name,
      CASE WHEN h.nights < 0 THEN 1 ELSE h.nights END AS nights,
      h.rooms, h.check_in_time, h.check_out_time,
      h.hotel_per_room_usd AS hotel_price_per_room_night_usd
    from
      sessions_2023 s
      left join users u on s.user_id = u.user_id
      left join flights f on s.trip_id = f.trip_id
      left join hotels h on s.trip_id = h.trip_id
    WHERE
      s.user_id IN (
        SELECT user_id
        FROM filtered_users)
  ),

  -- CTE returns the ids of all trips that have been canceled through a session
  -- This list is to filter all canceled sessions in the next CTE
  canceled_trips AS (
    SELECT DISTINCT trip_id
    FROM session_base
    WHERE cancellation = TRUE
  ),

  -- All sessions without trips, all canceled trips have been removed
  -- Each row represents a trip that a user did
  not_canceled_trips AS (
    SELECT *
    FROM session_base
    WHERE
      trip_id IS NOT NULL
      AND trip_id NOT IN(
        SELECT trip_id
        FROM canceled_trips
      )
  ),

  -- To aggregate user behaviour into metrics (a row per user)
  -- CTE contains metrics that have to do with the browsing behaviour
  -- ALL SESSION within our cohort get aggregated
  user_base_session AS (
    SELECT user_id, SUM(page_clicks) AS num_clicks,
      COUNT(DISTINCT session_id) AS num_sessions,
      AVG(session_duration) AS avg_session_duration
    FROM session_base
    GROUP BY user_id
  ),
  
  -- To aggregate user behaviour into metrics (row per user)
  -- CTE contains metrics that have the travel behavious
  -- Only rows with VALID trips within our cohort get aggregated
  user_base_trip AS (
    SELECT user_id, COUNT(DISTINCT trip_id) AS num_trips,
      SUM(CASE WHEN (flight_booked = TRUE)
          AND (return_flight_booked = TRUE) THEN 2
          WHEN (flight_booked = TRUE)
          OR (return_flight_booked = TRUE) THEN 1
          ELSE 0 END) AS num_flights,
            AVG(seats) AS avg_seats,
        SUM(nights) AS num_nights,
      SUM((hotel_price_per_room_night_usd * nights * rooms) * (
           1 - (CASE WHEN hotel_discount_amount IS NULL THEN 0
                ELSE hotel_discount_amount END))) AS hotel_expenses,
      SUM(base_fare_usd * (1 - flight_discount_amount)) AS flight_fare,
      AVG(EXTRACT(DAY FROM departure_time - session_end)) AS time_after_booking,
      AVG(haversine_distance(
          home_airport_lat,
          home_airport_lon,
          destination_airport_lat,
          destination_airport_lon)) AS avg_km_flown,
      AVG(checked_bags) as avg_bags
    FROM not_canceled_trips
    GROUP BY user_id
  ),
  
  -- User metrics table - the session metrics, trip metrics and general user information joined
  -- Using a left join, each row for each user from original cohort codition (7+ browsing sessions in 2023)
  -- inner join avoided not to get rid of users that have not actually travelled
  user_metrics AS (
    SELECT b.user_id, b.num_sessions, EXTRACT(YEAR FROM AGE (u.birthdate)) AS age,
      u.gender, u.married, u.has_children, u.home_country, u.home_city, u.home_airport,
      COALESCE(t.num_trips, 0) AS num_trips,
      COALESCE(t.num_flights, 0) AS num_flights,
        ROUND(COALESCE(t.avg_seats, 0), 2) AS avg_seats,
        ROUND(COALESCE(t.flight_fare, 0), 2) AS flight_fare,
        COALESCE(t.num_nights, 0) AS num_nights,
      ROUND(COALESCE(t.hotel_expenses, 0), 2) AS hotel_expenses,
      ROUND(COALESCE(t.time_after_booking, 0), 2) AS time_after_booking
    FROM
      user_base_session b
      LEFT JOIN users u ON b.user_id = u.user_id
      LEFT JOIN user_base_trip t ON b.user_id = t.user_id
  ),
  
  user_perk_metrics AS(
    SELECT *,
        CASE WHEN num_trips >= 5 AND num_flights >= 10 AND num_nights >= 10 THEN '1.golden guests'
        WHEN num_trips > 0 THEN
                (CASE WHEN age < 60 THEN
                                (CASE WHEN (has_children) OR avg_seats > 2 THEN '3.family & friends'
                                ELSE '4.valued members'
                        END)
                ELSE '2.honorable seniors'
                END)
        ELSE '5.nobel newcomers'
        END AS perk_catagory
        FROM    user_metrics
        )

SELECT
  user_id, perk_catagory,
  -- 1. golden guests (trips more than 3 and flights more than 5)
  CASE WHEN perk_catagory = '1.golden guests' THEN 'VIP lounge at the airport and 1 night at the hotel with free meal voucher'
  -- 2. honored/senior customers (age more than or equal 60)
  WHEN perk_catagory = '2.honorable seniors' THEN 'Complimentary meal at the airport / the hotel with travel guide'
  -- 3. family customers (with children)
  WHEN perk_catagory = '3.family & friends' THEN '1 free check-in bag with a complimentary meal'
  -- 4. valued member
  WHEN perk_catagory = '4.valued members' THEN 'Complimentary meal at the airport / the hotel'
  -- 5. nobel newcomers
  WHEN perk_catagory = '5.nobel newcomers' THEN '30% off first travel'
  END AS perk
FROM user_perk_metrics
;
