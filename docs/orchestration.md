# Databricks Job setup

Use one master-notebook task. It keeps the execution order visible and makes a failed dependency stop later layers.

1. In Databricks, open **Workflows** in the left navigation and select **Create job**.
2. Set the job name to `smart-city-aqi-daily-pipeline`.
3. Under **Tasks**, select **Add task**. Choose **Notebook** and select `06_orchestrate`.
4. Under **Compute**, choose **Serverless**. Do not create a classic cluster.
5. Add task parameters: `catalog=smart_city_aqi`, `run_setup=false`, and `run_extraction=true`. Leave the date values blank to use the prior seven days, or provide `start_date` and `end_date` in `YYYY-MM-DD` format.
6. Select **Create**, then **Run now**. Open the run output and confirm every logged step has status `SUCCESS`.
7. For a demo, take a screenshot of the Job run graph and the `monitoring.pipeline_run_log` table filtered to the run ID.

If Databricks blocks outbound API access, set `run_extraction=false`, run the local extraction scripts, upload their raw output to the Unity Catalog Volume, and then rerun the job. Bronze through validation will proceed normally.
