#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P  )"

display_usage() {
  echo "Available Commands"
  echo "  fetch_brazil_vms_data        Download BRAZIL VMS data to GCS."
  echo "  prepares_brazil_vms_data     Prepares the BRAZIL data using devices and messages get from the Brazilian API."
  echo "  load_brazil_vms_data         Load BRAZIL VMS data from GCS to BQ."
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi

case $1 in

  fetch_brazil_vms_data)
    echo "Running python -m pipe_vms_brazil.brazil_api_client ${@:2}"
    python -m pipe_vms_brazil.brazil_api_client ${@:2}
    ;;

  prepares_brazil_vms_data)
    echo "Running python -m pipe_vms_brazil.prepares_data ${@:2}"
    python -m pipe_vms_brazil.prepares_data ${@:2}
    ;;

  load_brazil_vms_data)
    ${THIS_SCRIPT_DIR}/gcs2bq.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 1
    ;;
esac
