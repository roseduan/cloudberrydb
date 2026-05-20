-- TPC-DS basic functional smoke for Iceberg AM
--
-- Covers the full 24-table TPC-DS schema with a tiny synthetic dataset
-- (about 6k rows across all tables) and exercises a breadth of SQL
-- features against CREATE ICEBERG TABLE: simple scan, multi-dim star
-- join, group-by + having, window function, CTE, correlated subquery,
-- NOT EXISTS, UPDATE, DELETE, UNION ALL, EXPLAIN.

\i ../../../lib/sql/common_setup.sql

-- ===== Catalog (builtin) + Volume (S3/MinIO @ minio:9000) =====
DROP SERVER IF EXISTS am_tpcds_cat_srv CASCADE;
CREATE SERVER am_tpcds_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER am_tpcds_cat_srv;
CREATE FOREIGN CATALOG am_tpcds_cat SERVER am_tpcds_cat_srv
    OPTIONS (default_namespace 'public');
SET iceberg_default_catalog = 'am_tpcds_cat';

DROP SERVER IF EXISTS am_tpcds_vol_srv CASCADE;
CREATE SERVER am_tpcds_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER am_tpcds_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME am_tpcds_vol SERVER am_tpcds_vol_srv
    OPTIONS (base_path '/iceberg_am_tpcds/', allow_writes 'true');
SET iceberg_default_volume = 'am_tpcds_vol';

-- ===== Schema (24 TPC-DS tables, iceberg AM) =====

DROP TABLE IF EXISTS call_center CASCADE;
DROP TABLE IF EXISTS catalog_page CASCADE;
DROP TABLE IF EXISTS catalog_returns CASCADE;
DROP TABLE IF EXISTS catalog_sales CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS customer_address CASCADE;
DROP TABLE IF EXISTS customer_demographics CASCADE;
DROP TABLE IF EXISTS date_dim CASCADE;
DROP TABLE IF EXISTS household_demographics CASCADE;
DROP TABLE IF EXISTS income_band CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS promotion CASCADE;
DROP TABLE IF EXISTS reason CASCADE;
DROP TABLE IF EXISTS ship_mode CASCADE;
DROP TABLE IF EXISTS store CASCADE;
DROP TABLE IF EXISTS store_returns CASCADE;
DROP TABLE IF EXISTS store_sales CASCADE;
DROP TABLE IF EXISTS time_dim CASCADE;
DROP TABLE IF EXISTS warehouse CASCADE;
DROP TABLE IF EXISTS web_page CASCADE;
DROP TABLE IF EXISTS web_returns CASCADE;
DROP TABLE IF EXISTS web_sales CASCADE;
DROP TABLE IF EXISTS web_site CASCADE;

-- Small dims
CREATE ICEBERG TABLE region (r_regionkey INT, r_name VARCHAR(25)); -- helper (not TPC-DS but simplifies joins; unused, remove)
DROP TABLE region;

CREATE ICEBERG TABLE date_dim (
    d_date_sk       INT,  d_date_id   VARCHAR(16), d_date    DATE,
    d_month_seq     INT,  d_week_seq  INT,         d_quarter_seq INT,
    d_year          INT,  d_dow       INT,         d_moy     INT,
    d_dom           INT,  d_qoy       INT,         d_fy_year INT,
    d_fy_quarter_seq INT, d_fy_week_seq INT,
    d_day_name      VARCHAR(9),  d_quarter_name VARCHAR(6),
    d_holiday       VARCHAR(1),  d_weekend      VARCHAR(1),
    d_following_holiday VARCHAR(1), d_first_dom INT, d_last_dom INT,
    d_same_day_ly   INT, d_same_day_lq INT,
    d_current_day   VARCHAR(1), d_current_week VARCHAR(1),
    d_current_month VARCHAR(1), d_current_quarter VARCHAR(1),
    d_current_year  VARCHAR(1));

CREATE ICEBERG TABLE time_dim (
    t_time_sk  INT, t_time_id VARCHAR(16), t_time INT, t_hour INT,
    t_minute   INT, t_second  INT,         t_am_pm VARCHAR(2),
    t_shift    VARCHAR(20),  t_sub_shift VARCHAR(20), t_meal_time VARCHAR(20));

CREATE ICEBERG TABLE item (
    i_item_sk INT, i_item_id VARCHAR(16), i_rec_start_date DATE, i_rec_end_date DATE,
    i_item_desc VARCHAR(200), i_current_price DECIMAL(7,2), i_wholesale_cost DECIMAL(7,2),
    i_brand_id INT, i_brand VARCHAR(50), i_class_id INT, i_class VARCHAR(50),
    i_category_id INT, i_category VARCHAR(50), i_manufact_id INT, i_manufact VARCHAR(50),
    i_size VARCHAR(20), i_formulation VARCHAR(20), i_color VARCHAR(20),
    i_units VARCHAR(10), i_container VARCHAR(10), i_manager_id INT, i_product_name VARCHAR(50));

CREATE ICEBERG TABLE customer (
    c_customer_sk INT, c_customer_id VARCHAR(16),
    c_current_cdemo_sk INT, c_current_hdemo_sk INT, c_current_addr_sk INT,
    c_first_shipto_date_sk INT, c_first_sales_date_sk INT,
    c_salutation VARCHAR(10), c_first_name VARCHAR(20), c_last_name VARCHAR(30),
    c_preferred_cust_flag VARCHAR(1), c_birth_day INT, c_birth_month INT, c_birth_year INT,
    c_birth_country VARCHAR(20), c_login VARCHAR(13),
    c_email_address VARCHAR(50), c_last_review_date VARCHAR(10));

CREATE ICEBERG TABLE customer_demographics (
    cd_demo_sk INT, cd_gender VARCHAR(1), cd_marital_status VARCHAR(1),
    cd_education_status VARCHAR(20), cd_purchase_estimate INT,
    cd_credit_rating VARCHAR(10), cd_dep_count INT,
    cd_dep_employed_count INT, cd_dep_college_count INT);

CREATE ICEBERG TABLE household_demographics (
    hd_demo_sk INT, hd_income_band_sk INT, hd_buy_potential VARCHAR(15),
    hd_dep_count INT, hd_vehicle_count INT);

CREATE ICEBERG TABLE customer_address (
    ca_address_sk INT, ca_address_id VARCHAR(16),
    ca_street_number VARCHAR(10), ca_street_name VARCHAR(60),
    ca_street_type VARCHAR(15), ca_suite_number VARCHAR(10),
    ca_city VARCHAR(60), ca_county VARCHAR(30), ca_state VARCHAR(2),
    ca_zip VARCHAR(10), ca_country VARCHAR(20),
    ca_gmt_offset DECIMAL(5,2), ca_location_type VARCHAR(20));

CREATE ICEBERG TABLE store (
    s_store_sk INT, s_store_id VARCHAR(16), s_rec_start_date DATE, s_rec_end_date DATE,
    s_closed_date_sk INT, s_store_name VARCHAR(50), s_number_employees INT, s_floor_space INT,
    s_hours VARCHAR(20), s_manager VARCHAR(40), s_market_id INT,
    s_geography_class VARCHAR(100), s_market_desc VARCHAR(100), s_market_manager VARCHAR(40),
    s_division_id INT, s_division_name VARCHAR(50), s_company_id INT, s_company_name VARCHAR(50),
    s_street_number VARCHAR(10), s_street_name VARCHAR(60), s_street_type VARCHAR(15),
    s_suite_number VARCHAR(10), s_city VARCHAR(60), s_county VARCHAR(30),
    s_state VARCHAR(2), s_zip VARCHAR(10), s_country VARCHAR(20),
    s_gmt_offset DECIMAL(5,2), s_tax_precentage DECIMAL(5,2));

CREATE ICEBERG TABLE promotion (
    p_promo_sk INT, p_promo_id VARCHAR(16),
    p_start_date_sk INT, p_end_date_sk INT, p_item_sk INT,
    p_cost DECIMAL(15,2), p_response_target INT, p_promo_name VARCHAR(50),
    p_channel_dmail VARCHAR(1), p_channel_email VARCHAR(1),
    p_channel_catalog VARCHAR(1), p_channel_tv VARCHAR(1),
    p_channel_radio VARCHAR(1), p_channel_press VARCHAR(1),
    p_channel_event VARCHAR(1), p_channel_demo VARCHAR(1),
    p_channel_details VARCHAR(100), p_purpose VARCHAR(15),
    p_discount_active VARCHAR(1));

CREATE ICEBERG TABLE warehouse (
    w_warehouse_sk INT, w_warehouse_id VARCHAR(16),
    w_warehouse_name VARCHAR(20), w_warehouse_sq_ft INT,
    w_street_number VARCHAR(10), w_street_name VARCHAR(60),
    w_street_type VARCHAR(15), w_suite_number VARCHAR(10),
    w_city VARCHAR(60), w_county VARCHAR(30), w_state VARCHAR(2),
    w_zip VARCHAR(10), w_country VARCHAR(20),
    w_gmt_offset DECIMAL(5,2));

CREATE ICEBERG TABLE ship_mode (
    sm_ship_mode_sk INT, sm_ship_mode_id VARCHAR(16),
    sm_type VARCHAR(30), sm_code VARCHAR(10), sm_carrier VARCHAR(20),
    sm_contract VARCHAR(20));

CREATE ICEBERG TABLE reason (
    r_reason_sk INT, r_reason_id VARCHAR(16), r_reason_desc VARCHAR(100));

CREATE ICEBERG TABLE income_band (
    ib_income_band_sk INT, ib_lower_bound INT, ib_upper_bound INT);

CREATE ICEBERG TABLE call_center (
    cc_call_center_sk INT, cc_call_center_id VARCHAR(16),
    cc_rec_start_date DATE, cc_rec_end_date DATE,
    cc_closed_date_sk INT, cc_open_date_sk INT,
    cc_name VARCHAR(50), cc_class VARCHAR(50),
    cc_employees INT, cc_sq_ft INT, cc_hours VARCHAR(20),
    cc_manager VARCHAR(40), cc_mkt_id INT, cc_mkt_class VARCHAR(50),
    cc_mkt_desc VARCHAR(100), cc_market_manager VARCHAR(40),
    cc_division INT, cc_division_name VARCHAR(50),
    cc_company INT, cc_company_name VARCHAR(50),
    cc_street_number VARCHAR(10), cc_street_name VARCHAR(60),
    cc_street_type VARCHAR(15), cc_suite_number VARCHAR(10),
    cc_city VARCHAR(60), cc_county VARCHAR(30),
    cc_state VARCHAR(2), cc_zip VARCHAR(10), cc_country VARCHAR(20),
    cc_gmt_offset DECIMAL(5,2), cc_tax_percentage DECIMAL(5,2));

CREATE ICEBERG TABLE web_page (
    wp_web_page_sk INT, wp_web_page_id VARCHAR(16),
    wp_rec_start_date DATE, wp_rec_end_date DATE,
    wp_creation_date_sk INT, wp_access_date_sk INT,
    wp_autogen_flag VARCHAR(1), wp_customer_sk INT,
    wp_url VARCHAR(100), wp_type VARCHAR(50),
    wp_char_count INT, wp_link_count INT,
    wp_image_count INT, wp_max_ad_count INT);

CREATE ICEBERG TABLE catalog_page (
    cp_catalog_page_sk INT, cp_catalog_page_id VARCHAR(16),
    cp_start_date_sk INT, cp_end_date_sk INT,
    cp_department VARCHAR(50), cp_catalog_number INT,
    cp_catalog_page_number INT, cp_description VARCHAR(100),
    cp_type VARCHAR(100));

CREATE ICEBERG TABLE web_site (
    web_site_sk INT, web_site_id VARCHAR(16),
    web_rec_start_date DATE, web_rec_end_date DATE,
    web_name VARCHAR(50), web_open_date_sk INT, web_close_date_sk INT,
    web_class VARCHAR(50), web_manager VARCHAR(40),
    web_mkt_id INT, web_mkt_class VARCHAR(50),
    web_mkt_desc VARCHAR(100), web_market_manager VARCHAR(40),
    web_company_id INT, web_company_name VARCHAR(50),
    web_street_number VARCHAR(10), web_street_name VARCHAR(60),
    web_street_type VARCHAR(15), web_suite_number VARCHAR(10),
    web_city VARCHAR(60), web_county VARCHAR(30),
    web_state VARCHAR(2), web_zip VARCHAR(10), web_country VARCHAR(20),
    web_gmt_offset DECIMAL(5,2), web_tax_percentage DECIMAL(5,2));

-- Facts
CREATE ICEBERG TABLE store_sales (
    ss_sold_date_sk INT, ss_sold_time_sk INT, ss_item_sk INT, ss_customer_sk INT,
    ss_cdemo_sk INT, ss_hdemo_sk INT, ss_addr_sk INT, ss_store_sk INT,
    ss_promo_sk INT, ss_ticket_number BIGINT, ss_quantity INT,
    ss_wholesale_cost DECIMAL(7,2), ss_list_price DECIMAL(7,2),
    ss_sales_price DECIMAL(7,2), ss_ext_discount_amt DECIMAL(7,2),
    ss_ext_sales_price DECIMAL(7,2), ss_ext_wholesale_cost DECIMAL(7,2),
    ss_ext_list_price DECIMAL(7,2), ss_ext_tax DECIMAL(7,2),
    ss_coupon_amt DECIMAL(7,2), ss_net_paid DECIMAL(7,2),
    ss_net_paid_inc_tax DECIMAL(7,2), ss_net_profit DECIMAL(7,2));

CREATE ICEBERG TABLE store_returns (
    sr_returned_date_sk INT, sr_return_time_sk INT, sr_item_sk INT,
    sr_customer_sk INT, sr_cdemo_sk INT, sr_hdemo_sk INT, sr_addr_sk INT,
    sr_store_sk INT, sr_reason_sk INT, sr_ticket_number BIGINT, sr_return_quantity INT,
    sr_return_amt DECIMAL(7,2), sr_return_tax DECIMAL(7,2),
    sr_return_amt_inc_tax DECIMAL(7,2), sr_fee DECIMAL(7,2),
    sr_return_ship_cost DECIMAL(7,2), sr_refunded_cash DECIMAL(7,2),
    sr_reversed_charge DECIMAL(7,2), sr_store_credit DECIMAL(7,2),
    sr_net_loss DECIMAL(7,2));

CREATE ICEBERG TABLE catalog_sales (
    cs_sold_date_sk INT, cs_sold_time_sk INT, cs_ship_date_sk INT,
    cs_bill_customer_sk INT, cs_bill_cdemo_sk INT, cs_bill_hdemo_sk INT,
    cs_bill_addr_sk INT, cs_ship_customer_sk INT, cs_ship_cdemo_sk INT,
    cs_ship_hdemo_sk INT, cs_ship_addr_sk INT, cs_call_center_sk INT,
    cs_catalog_page_sk INT, cs_ship_mode_sk INT, cs_warehouse_sk INT,
    cs_item_sk INT, cs_promo_sk INT, cs_order_number BIGINT, cs_quantity INT,
    cs_wholesale_cost DECIMAL(7,2), cs_list_price DECIMAL(7,2),
    cs_sales_price DECIMAL(7,2), cs_ext_discount_amt DECIMAL(7,2),
    cs_ext_sales_price DECIMAL(7,2), cs_ext_wholesale_cost DECIMAL(7,2),
    cs_ext_list_price DECIMAL(7,2), cs_ext_tax DECIMAL(7,2),
    cs_coupon_amt DECIMAL(7,2), cs_ext_ship_cost DECIMAL(7,2),
    cs_net_paid DECIMAL(7,2), cs_net_paid_inc_tax DECIMAL(7,2),
    cs_net_paid_inc_ship DECIMAL(7,2), cs_net_paid_inc_ship_tax DECIMAL(7,2),
    cs_net_profit DECIMAL(7,2));

CREATE ICEBERG TABLE catalog_returns (
    cr_returned_date_sk INT, cr_returned_time_sk INT, cr_item_sk INT,
    cr_refunded_customer_sk INT, cr_refunded_cdemo_sk INT,
    cr_refunded_hdemo_sk INT, cr_refunded_addr_sk INT,
    cr_returning_customer_sk INT, cr_returning_cdemo_sk INT,
    cr_returning_hdemo_sk INT, cr_returning_addr_sk INT,
    cr_call_center_sk INT, cr_catalog_page_sk INT, cr_ship_mode_sk INT,
    cr_warehouse_sk INT, cr_reason_sk INT, cr_order_number BIGINT,
    cr_return_quantity INT, cr_return_amount DECIMAL(7,2),
    cr_return_tax DECIMAL(7,2), cr_return_amt_inc_tax DECIMAL(7,2),
    cr_fee DECIMAL(7,2), cr_return_ship_cost DECIMAL(7,2),
    cr_refunded_cash DECIMAL(7,2), cr_reversed_charge DECIMAL(7,2),
    cr_store_credit DECIMAL(7,2), cr_net_loss DECIMAL(7,2));

CREATE ICEBERG TABLE web_sales (
    ws_sold_date_sk INT, ws_sold_time_sk INT, ws_ship_date_sk INT,
    ws_item_sk INT, ws_bill_customer_sk INT, ws_bill_cdemo_sk INT,
    ws_bill_hdemo_sk INT, ws_bill_addr_sk INT, ws_ship_customer_sk INT,
    ws_ship_cdemo_sk INT, ws_ship_hdemo_sk INT, ws_ship_addr_sk INT,
    ws_web_page_sk INT, ws_web_site_sk INT, ws_ship_mode_sk INT,
    ws_warehouse_sk INT, ws_promo_sk INT, ws_order_number BIGINT,
    ws_quantity INT, ws_wholesale_cost DECIMAL(7,2),
    ws_list_price DECIMAL(7,2), ws_sales_price DECIMAL(7,2),
    ws_ext_discount_amt DECIMAL(7,2), ws_ext_sales_price DECIMAL(7,2),
    ws_ext_wholesale_cost DECIMAL(7,2), ws_ext_list_price DECIMAL(7,2),
    ws_ext_tax DECIMAL(7,2), ws_coupon_amt DECIMAL(7,2),
    ws_ext_ship_cost DECIMAL(7,2), ws_net_paid DECIMAL(7,2),
    ws_net_paid_inc_tax DECIMAL(7,2), ws_net_paid_inc_ship DECIMAL(7,2),
    ws_net_paid_inc_ship_tax DECIMAL(7,2), ws_net_profit DECIMAL(7,2));

CREATE ICEBERG TABLE web_returns (
    wr_returned_date_sk INT, wr_returned_time_sk INT, wr_item_sk INT,
    wr_refunded_customer_sk INT, wr_refunded_cdemo_sk INT,
    wr_refunded_hdemo_sk INT, wr_refunded_addr_sk INT,
    wr_returning_customer_sk INT, wr_returning_cdemo_sk INT,
    wr_returning_hdemo_sk INT, wr_returning_addr_sk INT,
    wr_web_page_sk INT, wr_reason_sk INT, wr_order_number BIGINT,
    wr_return_quantity INT, wr_return_amt DECIMAL(7,2),
    wr_return_tax DECIMAL(7,2), wr_return_amt_inc_tax DECIMAL(7,2),
    wr_fee DECIMAL(7,2), wr_return_ship_cost DECIMAL(7,2),
    wr_refunded_cash DECIMAL(7,2), wr_reversed_charge DECIMAL(7,2),
    wr_account_credit DECIMAL(7,2), wr_net_loss DECIMAL(7,2));

CREATE ICEBERG TABLE inventory (
    inv_date_sk INT, inv_item_sk INT, inv_warehouse_sk INT,
    inv_quantity_on_hand INT);

-- ===== Load (tiny synthetic) =====

-- date_dim: 1 year, d_date_sk starts at 2451000 to match TPC-DS ranges
INSERT INTO date_dim
SELECT 2451000 + g,  'D-' || g,
       (DATE '1998-01-01' + g)::DATE,
       (1998 - 1900) * 12 + ((g / 30) % 12), g / 7, g / 90,
       1998 + (g / 365), (g % 7) + 1, ((g / 30) % 12) + 1, (g % 30) + 1,
       ((g / 90) % 4) + 1, 1998 + (g / 365),
       g / 90, g / 7,
       (ARRAY['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])[((g % 7) + 1)],
       'Q' || (((g / 90) % 4) + 1),
       CASE WHEN g % 30 = 0 THEN 'Y' ELSE 'N' END,
       CASE WHEN (g % 7) IN (5,6) THEN 'Y' ELSE 'N' END,
       'N', 1, 30, g - 365, g - 90,
       'N','N','N','N','N'
FROM generate_series(0, 364) g;

-- time_dim: 24 hours
INSERT INTO time_dim
SELECT g, 'T-' || g, g * 3600, g, 0, 0,
       CASE WHEN g < 12 THEN 'AM' ELSE 'PM' END,
       CASE WHEN g < 6 THEN 'third' WHEN g < 14 THEN 'first' ELSE 'second' END,
       'sub', CASE WHEN g BETWEEN 6 AND 9 THEN 'breakfast'
                   WHEN g BETWEEN 11 AND 13 THEN 'lunch'
                   WHEN g BETWEEN 18 AND 20 THEN 'dinner'
                   ELSE '' END
FROM generate_series(0, 23) g;

-- item: 200 items, 5 categories x 10 brands x 4 managers
INSERT INTO item
SELECT g, 'I-' || lpad(g::text, 8, '0'),
       DATE '1990-01-01', DATE '2999-01-01',
       'item_desc_' || g,
       ((g * 17 % 5000) / 100.0)::decimal(7,2),
       ((g * 11 % 2000) / 100.0)::decimal(7,2),
       (g % 10) + 1, 'Brand#' || ((g % 10) + 1),
       (g % 15) + 1, 'Class_' || ((g % 15) + 1),
       (g % 5) + 1, (ARRAY['Sports','Books','Home','Electronics','Music'])[((g % 5) + 1)],
       (g % 4) + 1, 'Manufact_' || ((g % 4) + 1),
       'N/A', 'N/A', 'N/A', 'Each', 'box',
       (g % 4) + 1, 'prod_' || g
FROM generate_series(1, 200) g;

-- customer_demographics: 100 rows (gender x marital x education)
INSERT INTO customer_demographics
SELECT g, (ARRAY['M','F'])[((g % 2) + 1)],
       (ARRAY['S','M','D','W'])[((g % 4) + 1)],
       (ARRAY['College','Primary','Secondary','4 yr Degree','2 yr Degree','Advanced Degree','Unknown'])[((g % 7) + 1)],
       (g * 10) + 500, 'Good', g % 5, g % 3, g % 2
FROM generate_series(1, 100) g;

-- household_demographics: 50
INSERT INTO household_demographics
SELECT g, (g % 10) + 1,
       (ARRAY['>10000','5001-10000','1001-5000','501-1000','Unknown'])[((g % 5) + 1)],
       g % 5, g % 3
FROM generate_series(1, 50) g;

-- income_band: 10
INSERT INTO income_band
SELECT g, (g - 1) * 10000, g * 10000
FROM generate_series(1, 10) g;

-- customer_address: 100 addresses
INSERT INTO customer_address
SELECT g, 'CA-' || lpad(g::text, 8, '0'),
       g::varchar, 'street ' || g, 'St',
       'apt ' || (g % 20),
       (ARRAY['NYC','LA','CHI','HOU','PHX','SEA','BOS','MIA','DEN','ATL'])[((g % 10) + 1)],
       'county', (ARRAY['NY','CA','IL','TX','AZ','WA','MA','FL','CO','GA'])[((g % 10) + 1)],
       lpad((10000 + g)::text, 5, '0'), 'USA',
       (g % 12)::decimal(5,2), 'single family'
FROM generate_series(1, 100) g;

-- customer: 100 customers
INSERT INTO customer
SELECT g, 'C-' || lpad(g::text, 8, '0'),
       ((g - 1) % 100) + 1,  -- cdemo_sk
       ((g - 1) % 50) + 1,   -- hdemo_sk
       ((g - 1) % 100) + 1,  -- addr_sk
       2451000 + ((g * 3) % 100),
       2451000 + ((g * 5) % 100),
       'Mr', 'first_' || g, 'last_' || g, 'Y',
       (g % 28) + 1, (g % 12) + 1, 1970 + (g % 50),
       'United States', 'login_' || g, 'email_' || g || '@x', '2000-01-01'
FROM generate_series(1, 100) g;

-- store: 10 stores
INSERT INTO store
SELECT g, 'S-' || lpad(g::text, 8, '0'), DATE '1990-01-01', DATE '2999-01-01', NULL,
       'store_' || g, 50 + g, 5000 + g * 100, '10:00-22:00', 'mgr_' || g,
       (g % 5) + 1, 'geo', 'mkt', 'mkt_mgr',
       (g % 3) + 1, 'div', (g % 2) + 1, 'company',
       g::varchar, 'street', 'St', 'apt',
       (ARRAY['NYC','LA','CHI','HOU','PHX','SEA','BOS','MIA','DEN','ATL'])[((g % 10) + 1)],
       'county', (ARRAY['NY','CA','IL','TX','AZ','WA','MA','FL','CO','GA'])[((g % 10) + 1)],
       lpad((20000 + g)::text, 5, '0'), 'USA',
       (g % 12)::decimal(5,2), 0.08::decimal(5,2)
FROM generate_series(1, 10) g;

-- promotion: 20
INSERT INTO promotion
SELECT g, 'P-' || lpad(g::text, 8, '0'),
       2451000 + (g % 300), 2451000 + (g % 300) + 30,
       ((g - 1) % 200) + 1,
       ((g * 29) % 100000 / 100.0)::decimal(15,2), 1000, 'promo ' || g,
       CASE WHEN g % 2 = 0 THEN 'Y' ELSE 'N' END,
       CASE WHEN g % 3 = 0 THEN 'Y' ELSE 'N' END,
       CASE WHEN g % 4 = 0 THEN 'Y' ELSE 'N' END,
       'N','N','N','N','N','details','promotional','Y'
FROM generate_series(1, 20) g;

-- warehouse: 5
INSERT INTO warehouse
SELECT g, 'W-' || lpad(g::text, 8, '0'), 'wh_' || g, 10000 + g * 500,
       g::varchar, 'wh street', 'St', 'apt',
       'city', 'county', 'CA', lpad((30000 + g)::text, 5, '0'), 'USA',
       0::decimal(5,2)
FROM generate_series(1, 5) g;

-- ship_mode: 10
INSERT INTO ship_mode
SELECT g, 'SM-' || lpad(g::text, 8, '0'),
       (ARRAY['EXPRESS','REGULAR','TWO DAY','LIBRARY','OVERNIGHT','NEXT DAY','GROUND','AIR','SHIP','TRUCK'])[((g % 10) + 1)],
       'code_' || g, 'carrier_' || g, 'contract'
FROM generate_series(1, 10) g;

-- reason: 10
INSERT INTO reason
SELECT g, 'R-' || lpad(g::text, 8, '0'), 'reason_desc_' || g
FROM generate_series(1, 10) g;

-- call_center: 5
INSERT INTO call_center
SELECT g, 'CC-' || lpad(g::text, 8, '0'),
       DATE '1990-01-01', DATE '2999-01-01', NULL, 2451000 + g,
       'call_' || g, 'class', 50, 5000, '24hr', 'mgr', 1, 'mkt', 'desc', 'mkt_mgr',
       1, 'div', 1, 'company',
       g::varchar, 'street', 'St', 'apt', 'NYC', 'county', 'NY',
       lpad((40000 + g)::text, 5, '0'), 'USA',
       0::decimal(5,2), 0.08::decimal(5,2)
FROM generate_series(1, 5) g;

-- web_page: 20
INSERT INTO web_page
SELECT g, 'WP-' || lpad(g::text, 8, '0'),
       DATE '1990-01-01', DATE '2999-01-01', 2451000, 2451000, 'Y',
       ((g - 1) % 100) + 1, 'http://x/' || g,
       (ARRAY['html','static','dynamic','unknown'])[((g % 4) + 1)],
       g * 100, g * 10, g * 2, g
FROM generate_series(1, 20) g;

-- catalog_page: 20
INSERT INTO catalog_page
SELECT g, 'CP-' || lpad(g::text, 8, '0'),
       2451000, 2451000 + 30, 'electronics', g % 5, g, 'desc_' || g,
       (ARRAY['spring','summer','fall','winter'])[((g % 4) + 1)]
FROM generate_series(1, 20) g;

-- web_site: 5
INSERT INTO web_site
SELECT g, 'WS-' || lpad(g::text, 8, '0'), DATE '1990-01-01', DATE '2999-01-01',
       'site_' || g, 2451000 + g, NULL, 'class', 'mgr',
       1, 'mkt', 'desc', 'mkt_mgr', 1, 'company',
       g::varchar, 'street', 'St', 'apt', 'NYC', 'county', 'NY',
       lpad((50000 + g)::text, 5, '0'), 'USA',
       0::decimal(5,2), 0.08::decimal(5,2)
FROM generate_series(1, 5) g;

-- store_sales: 2000 tickets = 200 items × 10 days (sk 2451000..2451009)
INSERT INTO store_sales
SELECT 2451000 + (g / 200),                            -- sold_date_sk: day 0..9
       (g * 7 % 24),                                   -- sold_time_sk
       ((g % 200) + 1),                                -- item_sk
       ((g % 100) + 1),                                -- customer_sk
       ((g % 100) + 1),                                -- cdemo_sk
       ((g % 50) + 1),                                 -- hdemo_sk
       ((g % 100) + 1),                                -- addr_sk
       ((g % 10) + 1),                                 -- store_sk
       ((g % 20) + 1),                                 -- promo_sk
       g::bigint,                                      -- ticket_number
       ((g % 10) + 1),                                 -- quantity
       ((g * 11 % 1000) / 100.0)::decimal(7,2),
       ((g * 17 % 2000) / 100.0)::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 17 % 2000) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * 0.08)::decimal(7,2),
       0::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.08)::decimal(7,2),
       (((g * 13 % 1500 - g * 11 % 1000) / 100.0) * ((g % 10) + 1))::decimal(7,2)
FROM generate_series(1, 2000) g;

-- store_returns: 200 (about 10% of sales)
INSERT INTO store_returns
SELECT 2451000 + (g / 20), (g % 24),
       ((g % 200) + 1), ((g % 100) + 1),
       ((g % 100) + 1), ((g % 50) + 1), ((g % 100) + 1),
       ((g % 10) + 1), ((g % 10) + 1), g::bigint, (g % 5) + 1,
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 0.08) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 1.08) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0 * 0.5)::decimal(7,2)
FROM generate_series(1, 200) g;

-- catalog_sales: 1000
INSERT INTO catalog_sales
SELECT 2451000 + (g / 100), (g % 24), 2451000 + (g / 100) + 3,
       ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1), ((g % 100) + 1),
       ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1), ((g % 100) + 1),
       ((g % 5) + 1), ((g % 20) + 1), ((g % 10) + 1), ((g % 5) + 1),
       ((g % 200) + 1), ((g % 20) + 1), g::bigint, (g % 10) + 1,
       ((g * 11 % 1000) / 100.0)::decimal(7,2),
       ((g * 17 % 2000) / 100.0)::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 17 % 2000) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * 0.08)::decimal(7,2),
       0::decimal(7,2), ((g * 3 % 500) / 100.0)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.08)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.05)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.13)::decimal(7,2),
       (((g * 13 % 1500 - g * 11 % 1000) / 100.0) * ((g % 10) + 1))::decimal(7,2)
FROM generate_series(1, 1000) g;

-- catalog_returns: 100
INSERT INTO catalog_returns
SELECT 2451000 + (g / 10), (g % 24),
       ((g % 200) + 1), ((g % 100) + 1), ((g % 100) + 1),
       ((g % 50) + 1), ((g % 100) + 1),
       ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1), ((g % 100) + 1),
       ((g % 5) + 1), ((g % 20) + 1), ((g % 10) + 1), ((g % 5) + 1),
       ((g % 10) + 1), g::bigint, (g % 5) + 1,
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 0.08) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 1.08) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0 * 0.5)::decimal(7,2)
FROM generate_series(1, 100) g;

-- web_sales: 1000
INSERT INTO web_sales
SELECT 2451000 + (g / 100), (g % 24), 2451000 + (g / 100) + 3,
       ((g % 200) + 1), ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1),
       ((g % 100) + 1), ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1),
       ((g % 100) + 1), ((g % 20) + 1), ((g % 5) + 1),
       ((g % 10) + 1), ((g % 5) + 1), ((g % 20) + 1),
       g::bigint, (g % 10) + 1,
       ((g * 11 % 1000) / 100.0)::decimal(7,2),
       ((g * 17 % 2000) / 100.0)::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 17 % 2000) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * 0.08)::decimal(7,2),
       0::decimal(7,2), ((g * 3 % 500) / 100.0)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1))::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.08)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.05)::decimal(7,2),
       (((g * 13 % 1500) / 100.0) * ((g % 10) + 1) * 1.13)::decimal(7,2),
       (((g * 13 % 1500 - g * 11 % 1000) / 100.0) * ((g % 10) + 1))::decimal(7,2)
FROM generate_series(1, 1000) g;

-- web_returns: 100
INSERT INTO web_returns
SELECT 2451000 + (g / 10), (g % 24),
       ((g % 200) + 1), ((g % 100) + 1), ((g % 100) + 1),
       ((g % 50) + 1), ((g % 100) + 1),
       ((g % 100) + 1), ((g % 100) + 1), ((g % 50) + 1), ((g % 100) + 1),
       ((g % 20) + 1), ((g % 10) + 1), g::bigint, (g % 5) + 1,
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 0.08) / 100.0)::decimal(7,2),
       ((g * 13 % 1500 * 1.08) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0)::decimal(7,2),
       0::decimal(7,2), 0::decimal(7,2),
       ((g * 13 % 1500) / 100.0 * 0.5)::decimal(7,2)
FROM generate_series(1, 100) g;

-- inventory: 500 rows (5 warehouses × 10 items × 10 days)
INSERT INTO inventory
SELECT 2451000 + (g / 50),
       ((g % 10) + 1),
       ((g % 5) + 1),
       100 + (g % 100)
FROM generate_series(1, 500) g;

-- ANALYZE
ANALYZE;

-- ===== Functional checks =====

SELECT test_log('CHECK 1: all 24 tables count');
SELECT 'call_center' tbl, count(*) FROM call_center UNION ALL
SELECT 'catalog_page', count(*) FROM catalog_page UNION ALL
SELECT 'catalog_returns', count(*) FROM catalog_returns UNION ALL
SELECT 'catalog_sales', count(*) FROM catalog_sales UNION ALL
SELECT 'customer', count(*) FROM customer UNION ALL
SELECT 'customer_address', count(*) FROM customer_address UNION ALL
SELECT 'customer_demographics', count(*) FROM customer_demographics UNION ALL
SELECT 'date_dim', count(*) FROM date_dim UNION ALL
SELECT 'household_demographics', count(*) FROM household_demographics UNION ALL
SELECT 'income_band', count(*) FROM income_band UNION ALL
SELECT 'inventory', count(*) FROM inventory UNION ALL
SELECT 'item', count(*) FROM item UNION ALL
SELECT 'promotion', count(*) FROM promotion UNION ALL
SELECT 'reason', count(*) FROM reason UNION ALL
SELECT 'ship_mode', count(*) FROM ship_mode UNION ALL
SELECT 'store', count(*) FROM store UNION ALL
SELECT 'store_returns', count(*) FROM store_returns UNION ALL
SELECT 'store_sales', count(*) FROM store_sales UNION ALL
SELECT 'time_dim', count(*) FROM time_dim UNION ALL
SELECT 'warehouse', count(*) FROM warehouse UNION ALL
SELECT 'web_page', count(*) FROM web_page UNION ALL
SELECT 'web_returns', count(*) FROM web_returns UNION ALL
SELECT 'web_sales', count(*) FROM web_sales UNION ALL
SELECT 'web_site', count(*) FROM web_site
ORDER BY tbl;

SELECT test_log('CHECK 2: filter + projection on fact table');
SELECT count(*) AS ss_feb_rows
FROM store_sales, date_dim
WHERE ss_sold_date_sk = d_date_sk AND d_moy = 1;

SELECT test_log('CHECK 3: TPC-DS Q3-style star join (store_sales x item x date_dim)');
SELECT i_category, count(*) AS sale_count, sum(ss_ext_sales_price) AS revenue
FROM store_sales, item, date_dim
WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk
GROUP BY i_category
ORDER BY i_category;

SELECT test_log('CHECK 4: 5-way dim join on store_sales');
SELECT s_store_name, i_category, count(*) AS n
FROM store_sales, store, item, customer, date_dim
WHERE ss_store_sk = s_store_sk
  AND ss_item_sk = i_item_sk
  AND ss_customer_sk = c_customer_sk
  AND ss_sold_date_sk = d_date_sk
  AND i_category = 'Electronics'
GROUP BY s_store_name, i_category
ORDER BY s_store_name
LIMIT 5;

SELECT test_log('CHECK 5: GROUP BY + HAVING');
SELECT i_brand, sum(ss_quantity) AS qty
FROM store_sales, item
WHERE ss_item_sk = i_item_sk
GROUP BY i_brand
HAVING sum(ss_quantity) > 100
ORDER BY qty DESC
LIMIT 5;

SELECT test_log('CHECK 6: window function (Q12-style revenueratio)');
SELECT i_category, i_brand, sum(ss_ext_sales_price) AS rev,
       round((sum(ss_ext_sales_price) * 100
              / nullif(sum(sum(ss_ext_sales_price))
                       OVER (PARTITION BY i_category), 0))::numeric, 2) AS ratio
FROM store_sales, item
WHERE ss_item_sk = i_item_sk
GROUP BY i_category, i_brand
ORDER BY i_category, rev DESC
LIMIT 10;

SELECT test_log('CHECK 7: CTE (tests Sequence-node rewrite path)');
WITH daily_rev AS (
    SELECT ss_sold_date_sk AS dk,
           sum(ss_ext_sales_price) AS rev
    FROM store_sales
    GROUP BY ss_sold_date_sk
)
SELECT dk, rev
FROM daily_rev
WHERE rev = (SELECT max(rev) FROM daily_rev);

SELECT test_log('CHECK 8: correlated subquery in WHERE (Q2-style pattern)');
SELECT s_store_name, max_sale
FROM store,
     (SELECT ss_store_sk, max(ss_ext_sales_price) AS max_sale
      FROM store_sales GROUP BY ss_store_sk) t
WHERE s_store_sk = t.ss_store_sk
  AND t.max_sale = (SELECT max(ss_ext_sales_price) FROM store_sales)
ORDER BY s_store_name;

SELECT test_log('CHECK 9: NOT EXISTS (items never returned)');
SELECT count(*) AS never_returned_items
FROM item
WHERE NOT EXISTS (SELECT 1 FROM store_returns WHERE sr_item_sk = i_item_sk);

SELECT test_log('CHECK 10: NOT IN subquery (Q16-style shape)');
SELECT count(*) AS items_not_in_returns
FROM item
WHERE i_item_sk NOT IN (SELECT sr_item_sk FROM store_returns WHERE sr_item_sk IS NOT NULL);

SELECT test_log('CHECK 11: COUNT DISTINCT');
SELECT count(DISTINCT ss_customer_sk) AS distinct_customers,
       count(DISTINCT ss_item_sk)     AS distinct_items,
       count(DISTINCT ss_store_sk)    AS distinct_stores
FROM store_sales;

SELECT test_log('CHECK 12: UNION ALL across three fact tables');
SELECT channel, count(*) AS n, sum(ext_sales) AS total
FROM (
    SELECT 'store'   AS channel, ss_ext_sales_price AS ext_sales FROM store_sales
    UNION ALL
    SELECT 'catalog',           cs_ext_sales_price               FROM catalog_sales
    UNION ALL
    SELECT 'web',               ws_ext_sales_price               FROM web_sales
) u
GROUP BY channel ORDER BY channel;

SELECT test_log('CHECK 13: CASE + aggregate');
SELECT d_year, d_moy,
       sum(CASE WHEN ss_net_profit > 0 THEN ss_net_profit ELSE 0 END) AS profit,
       sum(CASE WHEN ss_net_profit < 0 THEN ss_net_profit ELSE 0 END) AS loss
FROM store_sales, date_dim
WHERE ss_sold_date_sk = d_date_sk
GROUP BY d_year, d_moy
ORDER BY d_year, d_moy
LIMIT 3;

SELECT test_log('CHECK 14: UPDATE + SELECT (ACID path)');
UPDATE store_sales SET ss_quantity = 999 WHERE ss_ticket_number = 1;
SELECT ss_ticket_number, ss_quantity FROM store_sales WHERE ss_ticket_number = 1;

SELECT test_log('CHECK 15: DELETE + SELECT');
DELETE FROM store_sales WHERE ss_ticket_number IN (1, 2, 3);
SELECT count(*) FROM store_sales WHERE ss_ticket_number IN (1, 2, 3);

SELECT test_log('CHECK 16: EXPLAIN shows Iceberg Scan on fact tables');
EXPLAIN (COSTS OFF)
SELECT count(*) FROM store_sales ss, item i, date_dim d
WHERE ss.ss_item_sk = i.i_item_sk AND ss.ss_sold_date_sk = d.d_date_sk
  AND i.i_category = 'Books' AND d.d_moy = 1;

-- ===== Extended coverage (CHECK 17 - 34) =====

SELECT test_log('CHECK 17: LEFT / RIGHT / FULL OUTER JOIN');
SELECT count(*) AS left_join_rows
FROM customer c
LEFT JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk;

SELECT count(*) AS right_join_rows
FROM store_sales ss
RIGHT JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk;

SELECT count(*) AS full_outer_rows
FROM customer c
FULL OUTER JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk;

SELECT test_log('CHECK 18: multiple window functions (rank, row_number, lag, running sum)');
SELECT i_category, i_brand, revenue,
       rank()       OVER (PARTITION BY i_category ORDER BY revenue DESC) AS rnk,
       row_number() OVER (PARTITION BY i_category ORDER BY revenue DESC) AS rn,
       lag(revenue) OVER (PARTITION BY i_category ORDER BY revenue DESC) AS prev_rev,
       sum(revenue) OVER (PARTITION BY i_category
                          ORDER BY revenue DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM (
    SELECT i_category, i_brand, sum(ss_ext_sales_price) AS revenue
    FROM store_sales, item
    WHERE ss_item_sk = i_item_sk
    GROUP BY i_category, i_brand
) t
ORDER BY i_category, rnk
LIMIT 10;

SELECT test_log('CHECK 19: GROUPING SETS');
SELECT i_category, i_brand, count(*) AS n
FROM store_sales, item
WHERE ss_item_sk = i_item_sk
GROUP BY GROUPING SETS ((i_category), (i_category, i_brand), ())
ORDER BY i_category NULLS LAST, i_brand NULLS LAST
LIMIT 15;

SELECT test_log('CHECK 20: DISTINCT ON (first sale per customer)');
SELECT DISTINCT ON (ss_customer_sk) ss_customer_sk, ss_ticket_number, ss_ext_sales_price
FROM store_sales
ORDER BY ss_customer_sk, ss_ticket_number
LIMIT 5;

SELECT test_log('CHECK 21: FILTER clause in aggregate');
SELECT
    count(*)                                        AS total_sales,
    count(*) FILTER (WHERE ss_net_profit > 0)       AS profitable_sales,
    count(*) FILTER (WHERE ss_net_profit < 0)       AS loss_sales,
    sum(ss_ext_sales_price) FILTER (WHERE ss_quantity >= 5) AS bulk_revenue
FROM store_sales;

SELECT test_log('CHECK 22: INTERSECT and EXCEPT');
SELECT count(*) AS items_sold_both_store_and_web FROM (
    SELECT DISTINCT ss_item_sk FROM store_sales
    INTERSECT
    SELECT DISTINCT ws_item_sk FROM web_sales
) t;

SELECT count(*) AS items_only_in_store_not_web FROM (
    SELECT DISTINCT ss_item_sk FROM store_sales
    EXCEPT
    SELECT DISTINCT ws_item_sk FROM web_sales
) t;

SELECT test_log('CHECK 23: LATERAL join');
SELECT c.c_customer_sk, recent.max_date, recent.max_sales
FROM customer c,
     LATERAL (SELECT max(ss_sold_date_sk) AS max_date,
                     max(ss_ext_sales_price) AS max_sales
              FROM store_sales
              WHERE ss_customer_sk = c.c_customer_sk) recent
WHERE recent.max_date IS NOT NULL
ORDER BY c.c_customer_sk
LIMIT 5;

SELECT test_log('CHECK 24: INSERT VALUES + SELECT');
INSERT INTO reason VALUES (11, 'R-9999', 'inserted-via-values');
SELECT r_reason_sk, r_reason_desc FROM reason WHERE r_reason_sk = 11;

SELECT test_log('CHECK 25: INSERT ... SELECT (copy rows between iceberg AM tables)');
CREATE ICEBERG TABLE store_sales_recent (LIKE store_sales);
INSERT INTO store_sales_recent
SELECT * FROM store_sales WHERE ss_sold_date_sk >= 2451008;
SELECT count(*) AS rows_copied FROM store_sales_recent;
DROP TABLE store_sales_recent;

SELECT test_log('CHECK 26: TRUNCATE then reload');
CREATE ICEBERG TABLE throwaway_tbl (id INT, v VARCHAR(20));
INSERT INTO throwaway_tbl SELECT g, 'val_' || g FROM generate_series(1, 50) g;
SELECT count(*) AS before_truncate FROM throwaway_tbl;
TRUNCATE throwaway_tbl;
SELECT count(*) AS after_truncate FROM throwaway_tbl;
INSERT INTO throwaway_tbl VALUES (1, 'reloaded');
SELECT count(*) AS after_reload FROM throwaway_tbl;
DROP TABLE throwaway_tbl;

SELECT test_log('CHECK 27: ALTER TABLE ADD COLUMN (iceberg schema evolution)');
ALTER TABLE reason ADD COLUMN r_note VARCHAR(50);
SELECT r_reason_sk, r_reason_desc, r_note
FROM reason
WHERE r_reason_sk IN (1, 11)
ORDER BY r_reason_sk;

SELECT test_log('CHECK 28: UPDATE new column then read');
UPDATE reason SET r_note = 'filled-in' WHERE r_reason_sk = 11;
SELECT r_reason_sk, r_note FROM reason
WHERE r_reason_sk IN (1, 11) ORDER BY r_reason_sk;

SELECT test_log('CHECK 29: ALTER TABLE DROP COLUMN');
ALTER TABLE reason DROP COLUMN r_note;
SELECT count(*) AS rows_after_drop_col FROM reason;

SELECT test_log('CHECK 30: date arithmetic and extraction');
SELECT extract(year FROM d_date) AS yr,
       extract(month FROM d_date) AS mo,
       date_trunc('quarter', d_date)::date AS qtr_start,
       (d_date + INTERVAL '7 day')::date AS plus_week,
       (d_date - DATE '1998-01-01') AS days_since_y2k_neg_2
FROM date_dim
WHERE d_date_sk BETWEEN 2451000 AND 2451005
ORDER BY d_date;

SELECT test_log('CHECK 31: string functions (substring, like, position, replace)');
SELECT i_item_id,
       substring(i_item_id FROM 1 FOR 5) AS prefix,
       position('-' IN i_item_id)         AS dash_pos,
       replace(i_item_id, '-', ':')       AS replaced,
       i_item_desc ILIKE '%item_desc_1%'  AS desc_match
FROM item
WHERE i_item_sk BETWEEN 1 AND 3
ORDER BY i_item_sk;

SELECT test_log('CHECK 32: NULL semantics (IS NULL, IS DISTINCT FROM, COALESCE)');
-- customer.c_login is always set; insert a null row to test
INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_login)
VALUES (9999, 'C-9999', 'null_guy', NULL);

SELECT count(*) FILTER (WHERE c_login IS NULL)      AS null_login_count,
       count(*) FILTER (WHERE c_login IS NOT NULL)  AS set_login_count
FROM customer;

SELECT count(*) AS distinct_from_null
FROM customer WHERE c_login IS DISTINCT FROM NULL;

SELECT count(*) AS coalesced
FROM customer WHERE COALESCE(c_login, 'DEFAULT') = 'DEFAULT';

DELETE FROM customer WHERE c_customer_sk = 9999;

SELECT test_log('CHECK 33: self-join on store_sales (items frequently co-bought in same ticket)');
SELECT a.ss_item_sk AS item_a, b.ss_item_sk AS item_b, count(*) AS pair_count
FROM store_sales a
JOIN store_sales b ON a.ss_ticket_number = b.ss_ticket_number
                    AND a.ss_item_sk < b.ss_item_sk
GROUP BY a.ss_item_sk, b.ss_item_sk
HAVING count(*) >= 1
ORDER BY pair_count DESC, item_a, item_b
LIMIT 5;

SELECT test_log('CHECK 34: VACUUM (iceberg compaction)');
VACUUM item;
VACUUM store_sales;
SELECT 'vacuum done' AS status;

SELECT test_log('iceberg_am_tpcds_basic smoke: all 34 checks executed');
