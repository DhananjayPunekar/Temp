# =====================================================================
# framework/gold_pipeline_executor.py   (compatibility shim, v3.0)
#
# The Silver and Gold executors were MERGED into framework/pipeline_executor.py.
# This file remains only so older imports keep working:
#     from framework.gold_pipeline_executor import GoldPipelineExecutor
# It re-exports the same class from the merged module. No logic here.
# =====================================================================
from framework.silver_pipeline_executor import GoldPipelineExecutor  # noqa: F401
