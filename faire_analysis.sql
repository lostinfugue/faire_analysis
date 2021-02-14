/************
SQL analysis for Faire Direct Exercise
Jeff Li
This is code to reproduce the figures 1-2 in my analysis (done originally in Tableau).
I used sqlite3, which was slightly different for me (I'm used to Amazon Redshift).
*************/

-- Import data into faire_direct sqlite database
.mode csv
drop table if exists faire_direct;
.import "/Users/jeffli/Downloads/elevate_analytics_case_data.csv" faire_direct

-- Clean Data
-- since we imported from csv, sqlite interprets blanks as empty strings rather than nulls. we must set nulls:
UPDATE faire_direct SET brand_id = NULL WHERE brand_id = '';
UPDATE faire_direct SET retailer_id = NULL WHERE retailer_id = '';
UPDATE faire_direct SET brand_relationship_id = NULL WHERE brand_relationship_id = '';
UPDATE faire_direct SET brand_relationship_created_at = NULL WHERE brand_relationship_created_at = '';
UPDATE faire_direct SET brand_relationship_confirmed_at = NULL WHERE brand_relationship_confirmed_at = '';
UPDATE faire_direct SET confirmation_reason = NULL WHERE confirmation_reason = '';
UPDATE faire_direct SET normalized_referer = NULL WHERE normalized_referer = '';
UPDATE faire_direct SET outgoing_email_id = NULL WHERE outgoing_email_id = '';
UPDATE faire_direct SET email_sent_at = NULL WHERE email_sent_at = '';
UPDATE faire_direct SET brand_relationship_order_id = NULL WHERE brand_relationship_order_id = '';
UPDATE faire_direct SET retailer_signed_up_at = NULL WHERE retailer_signed_up_at = '';
UPDATE faire_direct SET retailer_placed_first_order_at = NULL WHERE retailer_placed_first_order_at = '';
UPDATE faire_direct SET retailer_placed_first_confirmed_order_at = NULL WHERE retailer_placed_first_confirmed_order_at = '';
UPDATE faire_direct SET power_retailer_converted_at = NULL WHERE power_retailer_converted_at = '';
UPDATE faire_direct SET retailer_gmv = NULL WHERE retailer_gmv = '';
UPDATE faire_direct SET retailer_business_type = NULL WHERE retailer_business_type = '';
UPDATE faire_direct SET brand_stockist_count = NULL WHERE brand_stockist_count = '';
UPDATE faire_direct SET brand_first_active_at = NULL WHERE brand_first_active_at = '';
UPDATE faire_direct SET brand_adopted_elevate_at = NULL WHERE brand_adopted_elevate_at = '';
UPDATE faire_direct SET power_maker_converted_at = NULL WHERE power_maker_converted_at = '';
UPDATE faire_direct SET account_owner = NULL WHERE account_owner = '';


/*******************
Reproduce Figure 1
*******************/

-- count acquired retailers per week
drop table if exists acquired_retailers_by_week;
create temp table acquired_retailers_by_week as
select 
DATE(DATE(retailer_placed_first_confirmed_order_at, 'weekday 0'), '-6 day')  as acquisition_week
--, min(retailer_placed_first_confirmed_order_at)
--, max(retailer_placed_first_confirmed_order_at)
, count(distinct retailer_id) as acquired_retailers
from faire_direct
where retailer_placed_first_confirmed_order_at is not null
group by 1
order by 1
;

-- compute first week's acquired retailers as a baseline for timeseries analysis.
drop table if exists acquired_retailers_by_week_2;
create temp table acquired_retailers_by_week_2 as
select acquisition_week
, acquired_retailers
, FIRST_VALUE(acquired_retailers) OVER(ORDER BY acquisition_week ROWS UNBOUNDED PRECEDING) as baseline_acquired_retailers
FROM acquired_retailers_by_week
where acquisition_week >= '2019-02-25'
and acquisition_week <= '2019-06-16'
;

-- Reproduce Figure 1
select acquisition_week
, acquired_retailers
, (acquired_retailers - baseline_acquired_retailers)*1.0
    / MAX(baseline_acquired_retailers, 0.001) as pct_change_from_baseline
FROM acquired_retailers_by_week_2
;

/*******************
Reproduce Figure 2
*******************/

-- count unique clicks reaching each step in conversion funnel.
drop table if exists conversion_funnel;
create temp table conversion_funnel as
select 
DATE(DATE(brand_relationship_created_at, 'weekday 0'), '-6 day')  as acquisition_week
, count(distinct brand_relationship_id) as unique_clicks
, count(distinct case when retailer_signed_up_at is not null then brand_relationship_id end) as unique_clicks_signed_up
, count(distinct case when retailer_placed_first_order_at is not null then brand_relationship_id end) as unique_clicks_placed_first_order
, count(distinct case when retailer_placed_first_confirmed_order_at is not null then brand_relationship_id end) as unique_clicks_acquired
from faire_direct
where brand_relationship_created_at is not null
group by 1
order by 1
;


-- Reproduce Figure 2
select acquisition_week
, unique_clicks
, unique_clicks_signed_up*1.0 / max(unique_clicks, 0.001) as pct_conversion_step_4
, unique_clicks_signed_up
, unique_clicks_placed_first_order*1.0 / max(unique_clicks_signed_up, 0.001) as pct_conversion_step_5
, unique_clicks_placed_first_order
, unique_clicks_acquired*1.0 / max(unique_clicks_placed_first_order, 0.001) as pct_conversion_step_6
, unique_clicks_acquired
from conversion_funnel
where acquisition_week >= '2019-02-25'
and acquisition_week <= '2019-06-16'
;

