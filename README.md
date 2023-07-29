## Submission for super.com

### What's in this repo?

- Each tab in the gsheet can be found in `/seeds/`
- `models/super/staging/`: staging tables can be found here
- `models/super/marts/core/`: dim and fact tables can be found here
- `models/super/`: calendar table can be found here
- `find_line_average` macro can be found in the `/macros/`

### Question 1

#### Models

See ![DAG](https://github.com/maxlee85/super/blob/main/etc/dag.png)

Each of the seed files were used to create a stg_table where some columns were renamed, cast or added.

There are 3 dimension tables, customer, product and vendor. I introduced the use of -1 values for surrogate_keys to identify when those are not present. For the purposes of this exercise all dimensions are type 1, though things like customer and product should be type 2 dimensions so there needs to be a dim_customer_history, dim_product_history, etc... tables created to track changes over time. I added a ltv calculation and first order date onto dim_customer for easier slicing (and also because I assumed a customer can be created without an order, although the sample data the customer created date is equal to the first order date).

fct_order_line is setup as a transactional fact in a star schema with surrogate keys to the 3 dimension tables with the grain of 1 row per order line. This table can be queried directly or via a bi tool (ie looker) to enable slicing across dimensions. This table can be used to answer any questions Beth might have about orders.

For simplicity in calculating refunded amounts I assumed if there was a refund all items were refunded and created a net_price column which is equal to the total_price of each item.
I did not create a orders dimension and just placed all important columns in fct_order_line, but could see the need for a separate orders dimension table as additional data is generated.

A looker explore could be setup like this to enable slicing of the data:
```
explore: orders
    from: fct_order_line

    join: dim_customer { type: left_outer relationship: many_to_one sql_on: ${orders.customer_id = ${dim_customer.customer_id}} ;; }
    join: dim_product { type: xxx relationship: many_to_one sql_on: xxx ;; }
    join: dim_vendor { type: xxx relationship: many_to_one sql_on: xxx ;; }
```

Or a query like this to count orders by customer state:
```
select
    d.state,
    count(distinct f.order_id) as number_of_orders

from
    fct_order_line f
    join dim_customers d on f.customer_id = d.customer_id

group by
    1
```

fct_orders_by_month is a periodic snapshot with period of month to track kpis. See table below:

| month      | number_of_orders | number_of_refunds | gross_merchandise_value | average_order_value | average_basket_size | active_customers |
|------------|------------------|-------------------|-------------------------|---------------------|---------------------|------------------|
| 2021-01-01 | 3.0              | 0.0               | 171.38                  | 57.13               | 2.0                 | 3.0              |
| 2021-02-01 | 4.0              | 1.0               | 424.23                  | 106.06              | 2.25                | 4.0              |
| 2021-03-01 | 7.0              | 1.0               | 696.25                  | 99.46               | 3.14                | 7.0              |

This table can be generated in a bi tool and then sliced on corresponding dimensions or a new model could be created. IE fct_orders_by_state_by_month where the sql joins customer to orders and adds state to the select.



### Question 2

#### Customer Lifetime Value

In fct_customer_cohorts_by_month, I cohorted users into their first purchase month and joined it to the calendar table to enable easier reporting across months. The grain of this table is 1 row per customer per month.
net_price is the ltv for the month while lifetime_value is a rolling sum of all prior months.

An example for customer_id = 1:
| customer_cohort_id               | customer_id | date_month          | is_active | first_order_month | net_price | lifetime_value | cohort_number | cohort_month_number |
|----------------------------------|-------------|---------------------|-----------|-------------------|-----------|----------------|---------------|---------------------|
| 0d2ad5b7e21705eabb181395a4c2c782 | 1           | 2021-01-01T00:00:00 | true      | 2021-01-01        | 100.01    | 100.01         | 1             | 1                   |
| db7e20788d02cebd40802c7aab29501d | 1           | 2021-02-01T00:00:00 | true      | 2021-01-01        | 104.22    | 204.23         | 1             | 2                   |
| 882f6f4e7ae37efae11e3747c744c2f5 | 1           | 2021-03-01T00:00:00 | true      | 2021-01-01        | 134.6     | 338.83         | 1             | 3                   |
| 5cf635ed248224ca81ab832c73a4b06e | 1           | 2021-04-01T00:00:00 | false     | 2021-01-01        |           | 338.83         | 1             | 4                   |
| 835a11b66f96d36967a6e8650c94e22c | 1           | 2021-05-01T00:00:00 | false     | 2021-01-01        |           | 338.83         | 1             | 5                   |

Keeping with the star schema, you can join dim_customers to fct_customer_cohorts_by_month on customer_id to enable slicing across the customer dimension. This can be managed within a bi tool (example shown in question 1) or joining the two tables together while querying directly.

#### Customer Retention Rate

In fct_customer_cohorts_pivoted, I flattened the fct_customer_cohorts_by_month table into 1 row per customer with each month being a column with value 1 if that customer made a purchase in that month.

Here is an example query used to calculate retention rate using fct_customer_cohorts__pivoted:
```
select
    cohort_number,
    sum(is_cohort_month_1_customer) as month_1_customers,
    sum(is_cohort_month_2_customer) as month_2_customers,
    round(100.00*sum(is_cohort_month_2_customer)/nullif(sum(is_cohort_month_1_customer), 0), 2) as first_month_retention_rate,
    sum(is_cohort_month_3_customer) as month_3_customers,
    round(100.00*sum(is_cohort_month_3_customer)/nullif(sum(is_cohort_month_1_customer), 0), 2) as second_month_retention_rate

from
    fct_customer_cohorts__pivoted

group by
    1
```

| cohort_number | month_1_customers | month_2_customers | first_month_retention_rate | month_3_customers | second_month_retention_rate |
|---------------|-------------------|-------------------|----------------------------|-------------------|-----------------------------|
| 1             | 3                 | 1                 | 33.33                      | 1                 | 33.33                       |
| 2             | 3                 | 2                 | 66.67                      | 0                 | 0.0                         |
| 3             | 4                 | 0                 | 0.0                        | 0                 | 0.0                         |

### Question 3

To account for the introduction of a subscription model I would:

1. Add `subscription_id` to `fct_order_line` (assuming orders created via subscription will have a subscription_id) and booleans to indicate if an order is related to a subscription.
2. Create `dim_subscription` (type 1 scd) and `dim_subscription_history` (type 2 scd) models
3. Create a model to track monthly recurring revenue (ie fct_mrr, grain `subscription_id` x `month`)
