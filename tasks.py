from utils import (
    app,
    sdk_get_anomaly_df,
    sdk_get_current_df,
    sdk_get_forecast_df,
    grubbs_t,
    lscp_t,
    spectral_residual_saliency,
    vr_forecast,
    health_score,
    mechanism_analysis,
    show_points,
)
from celery import group


@app.task
def chainTask(unit):
    print(f"Task{unit} is Running")
    task_group1 = group(
        sdk_get_anomaly_df.s(unit), sdk_get_forecast_df.s(unit), sdk_get_current_df.s(unit)
    )
    task_group1.apply_async()

    task_group2 = group(grubbs_t.s(unit), lscp_t.s(unit), spectral_residual_saliency.s(unit))
    result = task_group2.apply_async()
    health_score.apply_async((*result.get(), unit), countdown=1)

    task_group3 = group(
        vr_forecast.s(unit),
    )
    task_group3.apply_async()

    task_group4 = group(
        mechanism_analysis.s(unit),
        show_points.s(unit)
    )
    task_group4.apply_async()
