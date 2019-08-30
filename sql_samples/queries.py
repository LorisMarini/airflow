"""
Description Here
"""

"""
Import statements HERE
"""

def activity_count_weekly(**context):
    """
    Resamples `activities_count_daily` weekly, and joins them with the
    activity type table to generate a breakdown of how many activities were
    recorded over time for each instance. Schema of output table:

        instance	    STRING	    NULLABLE
        action	        STRING	    NULLABLE
        direction	    STRING	    NULLABLE
        date	        TIMESTAMP	NULLABLE
        count	        INTEGER	    NULLABLE
        execution_date	TIMESTAMP	NULLABLE

    The result is saved to table `activity_count_weekly`.

    Parameters
    ----------
    context

    Returns
    -------

    """
    # Convert pendulum exec date to pd.Timestamp
    exec_date = exec_date_as_ts(**context)

    # Start 90 days in the past
    start_time = exec_date - pd.Timedelta(90, unit="d") + pd.Timedelta(1, unit="ms")

    # End right before end of execution date
    end_time = exec_date + pd.Timedelta(1, unit="d") - pd.Timedelta(1, unit="ms")

    # Extract GCP env variables
    gcp_project = env_get_gcp_project_id()
    gcp_dataset = env_get("GBQ_DATASET_NAME")
    gcp_location = env_get("GBQ_DATASET_LOCATION")

    # Prepare etl id for output
    output_etl_id = etlid_activity_count_weekly(**context)

    # Report actions
    report_message(level="info", log=True,
                   message=f"querying daily activity count from {start_time} to {end_time}")

    # Open client
    client = bigquery.Client(project=gcp_project)

    # Prepare parameters
    query_params = [bigquery.ScalarQueryParameter("time_start", "TIMESTAMP", start_time),
                    bigquery.ScalarQueryParameter("time_end", "TIMESTAMP", end_time)]

    query = f"""WITH
    aggs AS (SELECT instance, count, action,
             DATETIME_TRUNC(DATETIME(execution_date), WEEK(SUNDAY)) AS date
             FROM `{gcp_project}.{gcp_dataset}.activity_count_daily` t1
                WHERE t1.execution_date > @time_start
                AND t1.execution_date < @time_end
                ORDER BY instance, action, count DESC),

    types AS (SELECT * FROM `{gcp_project}.{gcp_dataset}.activity_type`
                WHERE execution_date = (SELECT max(execution_date)
                                          FROM `{gcp_project}.{gcp_dataset}.activity_type`)),

    joined AS (SELECT * FROM aggs
                LEFT JOIN types
                USING (action)
                WHERE direction IS NOT NULL)

    SELECT instance, action, direction, date, SUM(count) as count
    FROM joined
    WHERE direction IN ("from_user", "to_user")
    GROUP BY instance, action, direction, date
    ORDER BY instance, action  DESC;
    """

    # Configure query with parameters
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params

    # Location must match that of the gcp_dataset(s) referenced in the query.
    # API request - starts the query
    query_job = client.query(query, location=gcp_location, job_config=job_config)

    # Convert to dataframe
    output = query_job.to_dataframe()

    output["execution_date"] = exec_date_as_ts(**context)

    # Save to bucket
    df_to_parquet(df=output, path=output_etl_id.prod_path)

    if not output.empty:

        # Load to BigQuery
        gbq_load_parquet_from_gcs(gcs_path=output_etl_id.prod_path,
                                  project_id=env_get_gcp_project_id(),
                                  dataset_id=env_get("GBQ_DATASET_NAME"),
                                  table_id="activity_count_weekly",  # DO NOT CHANGE THIS
                                  part_column="execution_date")
    return True



def gbq_view_journey_description():
    """
    View of all latest Journey data (state + shapes + etc)
    """

    gcp_project = env_get_gcp_project_id()
    gcp_dataset = env_get("GBQ_DATASET_NAME")

    # Write query
    query = f"""
        WITH
          -- Index
          ALL_J_INDEX AS (SELECT * FROM `{gcp_project}.{gcp_dataset}.journey_index`),

          INDEX_1 AS (SELECT instance, max(execution_date) as execution_date
                      FROM ALL_J_INDEX
                      GROUP BY instance),

          J_INDEX AS (SELECT ALL_J_INDEX.instance, journeyId FROM INDEX_1
                      INNER JOIN ALL_J_INDEX
                      ON INDEX_1.instance=ALL_J_INDEX.instance
                      AND INDEX_1.execution_date = ALL_J_INDEX.execution_date),
          -- Shapes
          ALL_J_SHAPES AS (SELECT *  FROM `{gcp_project}.{gcp_dataset}.journey_shapes`),
          SHAPES_1 AS (SELECT journeyId, max(execution_date) as execution_date FROM ALL_J_SHAPES GROUP BY journeyId),
          J_SHAPES AS (SELECT ALL_J_SHAPES.* FROM SHAPES_1
                        INNER JOIN ALL_J_SHAPES
                        ON SHAPES_1.journeyId=ALL_J_SHAPES.journeyId
                        AND SHAPES_1.execution_date = ALL_J_SHAPES.execution_date),
          -- States
          ALL_J_STATE AS (SELECT * FROM `{gcp_project}.{gcp_dataset}.journey_state`),
          STATES_1 AS (SELECT journeyId, max(execution_date) as execution_date FROM ALL_J_STATE GROUP BY journeyId),
          J_STATES AS (SELECT ALL_J_STATE.* FROM STATES_1
                        INNER JOIN ALL_J_STATE
                        ON STATES_1.journeyId=ALL_J_STATE.journeyId
                        AND STATES_1.execution_date = ALL_J_STATE.execution_date)

        -- Journey Table
        SELECT * EXCEPT (id, description, execution_date) FROM J_STATES
        LEFT JOIN J_SHAPES USING (journeyId)
        LEFT JOIN J_INDEX USING (journeyId)
        """

    table = gbq_instantiate_table_object(name="v_journey_description", is_view=True)

    # Load query to define the view
    table.view_query = query

    # consolidate view (create if not existing)
    gbq_table_consolidate(view=table)

    return True
