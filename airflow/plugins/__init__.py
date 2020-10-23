from airflow.plugins_manager import AirflowPlugin

import operators


class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.CreateRedshiftClusterOperator,
        operators.SaveRedshiftHostOperator,
        operators.StageTripData,
        operators.StageWeatherData,
        operators.StageZoneData,
        operators.CheckNullValuesOperator,
        operators.CheckUniqueValuesOperator,
    ]
