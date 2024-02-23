import argparse
import os
import time
from typing import Any, Dict, Iterable, Optional, Sequence

from dbt_rebalance_check.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    get_model_sqls,
    get_models, get_dbt_model_query,
)
from dbt_rebalance_check.tracking import dbtCheckpointTracking


def create_repartition_hint(partitions: str) -> str:
    return f"/*+ REBALANCE({partitions}) */"


def has_labels_key(
        paths: Sequence[str],
        manifest: Dict[str, Any],
        include_disabled: bool = False,
) -> int:
    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    for model in models:
        model_config = model.node.get("config")
        if "partition_by" in model_config:
            partition_col = model_config["partition_by"]
            if isinstance(partition_col, list):
                parition = ','.join(partition_col)
            else:
                parition = partition_col
            modelQuery = get_dbt_model_query(model)
            rebalanceHint = create_repartition_hint(parition)
            # print(parition)
            # print(rebalanceHint)
            if rebalanceHint in modelQuery:
                status_code = 0
            else:
                status_code = 1
                print("REBALANCE hint "+ rebalanceHint+" not found in model "+ model.model_name + " Partition column provided: " + parition)
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = has_labels_key(
        paths=args.filenames,
        manifest=manifest,
        include_disabled=args.include_disabled,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model has labels keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
