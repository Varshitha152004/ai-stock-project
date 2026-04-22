from sqlalchemy import text

FIELD_MAP = {
    "pe_ratio": {"table": "fundamentals", "column": "pe_ratio", "alias": "f"},
    "eps": {"table": "fundamentals", "column": "eps", "alias": "f"},
    "revenue": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "debt": {"table": "fundamentals", "column": "debt", "alias": "f"},
    "revenue": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "revenue_growth": {"table": "fundamentals", "column": "revenue_growth", "alias": "f"},
    "revenue_cagr": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "revenue_growth_calc": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "avg_revenue_growth": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "revenue_trend": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "consistent_growth": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "revenue_yoy_growth": {"table": "fundamentals", "column": "revenue", "alias": "f"},
    "price_change_1y": {"table": "fundamentals", "column": "price_change_1y", "alias": "f"},
    "market_cap": {"table": "fundamentals", "column": "market_cap", "alias": "f"},
    "sector": {"table": "symbols", "column": "sector", "alias": "s"},
    "reported_date": {"table": "fundamentals", "column": "reported_date", "alias": "f"}
}

SAFE_OPERATORS = {"<", "<=", ">", ">=", "="}
SAFE_FIELDS = set(FIELD_MAP.keys())


def compile_node(node, param_index=0):
    clauses = []
    params = {}

    for condition in node.conditions:

        #  If nested DSLNode
        if hasattr(condition, "conditions"):
            nested_clause, nested_params, param_index = compile_node(
                condition,
                param_index
            )
            clauses.append(f"({nested_clause})")
            params.update(nested_params)
            continue

        #  Normal Condition
        # Validate field
        if condition.field not in SAFE_FIELDS:
            raise ValueError(f"Invalid field: {condition.field}")

        field_info = FIELD_MAP[condition.field]

        param_name = f"value_{param_index}"
        param_index += 1

        alias = field_info["alias"]
        column = field_info["column"]
        
        # Validate operator
        if condition.operator not in SAFE_OPERATORS:
            raise ValueError(f"Invalid operator: {condition.operator}")
        
        #  QoQ growth
        if condition.field == "revenue_growth_calc":
            clause = f"revenue_growth_calc {condition.operator} :{param_name}"
        elif condition.field == "consistent_growth":
            clause = "revenue_growth_calc > 0"
        elif condition.field == "revenue_yoy_growth":
            clause = f"revenue_yoy_growth {condition.operator} :{param_name}"
        elif condition.field == "avg_revenue_growth":
            clause = f"avg_revenue_growth {condition.operator} :{param_name}"

        elif condition.field == "revenue_cagr":
            clause = f"revenue_cagr {condition.operator} :{param_name}"

        elif condition.field == "revenue_trend":
            clause = "revenue_trend_flag = 1"

        elif condition.field == "revenue_growth":
            clause = f"revenue_growth_calc {condition.operator} :{param_name}"

        #  normal fields
        else:
            clause = f"{alias}.{column} {condition.operator} :{param_name}"

        clauses.append(clause)

        params[param_name] = condition.value

    where_clause = f" {node.logic} ".join(clauses)

    return where_clause, params, param_index


def detect_tables(node):
    tables = set()

    for condition in node.conditions:

        # If nested DSLNode → recurse
        if hasattr(condition, "conditions"):
            nested_tables = detect_tables(condition)
            tables.update(nested_tables)
            continue

        # Normal condition
        tables.add(FIELD_MAP[condition.field]["table"])

    return tables

def apply_time_filter(where_clause, params, time_filter):

    if not time_filter:
        return where_clause, params

    if time_filter.type == "last_n_quarters":
        quarters = int(time_filter.value)
        months = quarters * 3

        where_clause += f" AND f.reported_date >= CURRENT_DATE - INTERVAL '{months} months'"
        

    elif time_filter.type == "year":
        where_clause += " AND EXTRACT(YEAR FROM f.reported_date) = :year"
        params["year"] = time_filter.value

    elif time_filter.type == "range":
        where_clause += " AND f.reported_date BETWEEN :from_date AND :to_date"
        params["from_date"] = time_filter.from_date
        params["to_date"] = time_filter.to_date

    return where_clause, params



def build_sql_from_dsl(dsl):

    where_clause, params, _ = compile_node(dsl.root)
    
    if not where_clause:
        raise ValueError("Empty WHERE clause not allowed")

    use_grouping = False
    
    
    tables_used = detect_tables(dsl.root)

    join_clause = ""

    if "fundamentals" in tables_used:

        if dsl.time_filter:
            #  TIME SERIES MODE (multiple quarters)
            join_clause += """
                JOIN fundamentals f
                    ON s.id = f.symbol_id
            """
        else:
            #  LATEST SNAPSHOT MODE (old behavior)
            join_clause += """
                JOIN fundamentals f
                    ON s.id = f.symbol_id
                    AND f.reported_date = (
                        SELECT MAX(f2.reported_date)
                        FROM fundamentals f2
                        WHERE f2.symbol_id = s.id
                    )
            """
    # DEFAULT QUERY 
    time_condition = ""

    if dsl.time_filter and dsl.time_filter.type == "last_n_quarters":
        months = int(dsl.time_filter.value) * 3
        time_condition = f" AND reported_date >= CURRENT_DATE - INTERVAL '{months} months'"
    query = f"""
SELECT *
FROM (
    SELECT 
        base.*,
        CASE 
            WHEN rev_q3 < rev_q2 AND rev_q2 < rev_q1 AND rev_q1 < revenue
            THEN 1 ELSE 0
        END AS revenue_trend_flag,
        base.revenue_growth_calc AS revenue_growth,
        AVG(revenue_growth_calc) OVER (PARTITION BY symbol) AS avg_revenue_growth,

        SUM(
            CASE 
                WHEN revenue_growth_calc > 0 THEN 1 
                ELSE 0 
            END
        ) OVER (PARTITION BY symbol) AS positive_growth_quarters,

        (
            POWER(
                revenue / NULLIF(FIRST_VALUE(revenue) OVER (
                    PARTITION BY symbol ORDER BY reported_date
                ), 0),
                1.0 / GREATEST(COUNT(*) OVER (PARTITION BY symbol), 1)
            ) - 1
        ) * 100 AS revenue_cagr

    FROM (
        SELECT
            s.symbol,
            s.sector,
            f.pe_ratio,
            f.eps,
            f.market_cap,
            f.reported_date,
            f.revenue,
            f.price_change_1y,
            
            LAG(f.revenue, 1) OVER (PARTITION BY f.symbol_id ORDER BY f.reported_date) AS rev_q1,
            LAG(f.revenue, 2) OVER (PARTITION BY f.symbol_id ORDER BY f.reported_date) AS rev_q2,
            LAG(f.revenue, 3) OVER (PARTITION BY f.symbol_id ORDER BY f.reported_date) AS rev_q3,
            
            (
                (f.revenue - LAG(f.revenue) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ))
                /
                NULLIF(LAG(f.revenue) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ), 0)
            ) * 100 AS revenue_growth,

            (
                (f.revenue - LAG(f.revenue) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ))
                /
                NULLIF(LAG(f.revenue) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ), 0)
            ) * 100 AS revenue_growth_calc,
              

            (
                (f.revenue - LAG(f.revenue, 4) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ))
                /
                NULLIF(LAG(f.revenue, 4) OVER (
                    PARTITION BY f.symbol_id ORDER BY f.reported_date
                ), 0)
            ) * 100 AS revenue_yoy_growth

        FROM symbols s
        JOIN fundamentals f ON s.id = f.symbol_id
    ) base
) sub

WHERE 1=1
{time_condition}
AND {where_clause.replace("f.", "")}
"""
    
    
    # Sorting
    if use_grouping:
        query += " ORDER BY symbol"

    elif dsl.time_filter:
        query += " ORDER BY symbol"

    elif dsl.sort_field and dsl.sort_field in FIELD_MAP:
        field_info = FIELD_MAP[dsl.sort_field]
        alias = field_info["alias"]
        column = field_info["column"]
        query += f" ORDER BY {alias}.{column} {dsl.sort_order.upper()}"

    if dsl.limit:
        if dsl.limit > 100:
            raise ValueError("Limit exceeds allowed maximum")

        query += " LIMIT :limit"
        params["limit"] = dsl.limit
    
    return text(query), params