from dagster import AssetSelection, define_asset_job

from ..partitions import monthly_partition, weekly_partition

weekly_update_job = define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=AssetSelection.keys("trips_by_week")
)

trip_update_job = define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all() - AssetSelection.keys("trips_by_week") - AssetSelection.keys(["adhoc_request"])
)

adhoc_request_job = define_asset_job(
    name="adhoc_request_job",
    selection=AssetSelection.keys(["adhoc_request"])
)
