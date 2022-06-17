#!/bin/bash

set -e
script_dir=$(cd "$(dirname "$0")" ; pwd -P)

goal_upload-data() {
    bucket_name="${1}"
    if [ -z "${bucket_name}" ]; then
      echo "BUCKET_NAME not supplied. Usage <func> bucket_name"
      exit 1
    fi

    staging_dir="${script_dir}/transformation-outputs"
    mkdir -p ${staging_dir}/CountryEmissionsVsTemperatures.parquet
    mkdir -p ${staging_dir}/EuropeBigThreeEmissions.parquet
    mkdir -p ${staging_dir}/GlobalEmissionsVsTemperatures.parquet
    mkdir -p ${staging_dir}/OceaniaEmissionsEdited.parquet

    curl -o "${staging_dir}/CountryEmissionsVsTemperatures.parquet/part-00000-c89aa3c9-cd2a-4f3e-be65-fc2a004087c9-c000.snappy.parquet" "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/output-data/CountryEmissionsVsTemperatures.parquet/part-00000-c89aa3c9-cd2a-4f3e-be65-fc2a004087c9-c000.snappy.parquet"
    curl -o "${staging_dir}/EuropeBigThreeEmissions.parquet/part-00000-5dde5362-0505-4032-a812-f44e89581a41-c000.snappy.parquet" "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/output-data/EuropeBigThreeEmissions.parquet/part-00000-5dde5362-0505-4032-a812-f44e89581a41-c000.snappy.parquet"
    curl -o "${staging_dir}/GlobalEmissionsVsTemperatures.parquet/part-00000-d00931d6-c620-4901-bece-1d9c2df719c5-c000.snappy.parquet" "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/output-data/GlobalEmissionsVsTemperatures.parquet/part-00000-d00931d6-c620-4901-bece-1d9c2df719c5-c000.snappy.parquet"
    curl -o "${staging_dir}/OceaniaEmissionsEdited.parquet/part-00000-c82e540c-bd0f-4ee1-b105-296567c77ecb-c000.snappy.parquet" "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/output-data/OceaniaEmissionsEdited.parquet/part-00000-c82e540c-bd0f-4ee1-b105-296567c77ecb-c000.snappy.parquet"

   aws s3 cp ${script_dir}/transformation-outputs/GlobalEmissionsVsTemperatures.parquet/ s3://${bucket_name}/data-transformation/GlobalEmissionsVsTemperatures.parquet/ --recursive
   aws s3 cp ${script_dir}/transformation-outputs/CountryEmissionsVsTemperatures.parquet/ s3://${bucket_name}/data-transformation/CountryEmissionsVsTemperatures.parquet/ --recursive
   aws s3 cp ${script_dir}/transformation-outputs/EuropeBigThreeEmissions.parquet/ s3://${bucket_name}/data-transformation/EuropeBigThreeEmissions.parquet/ --recursive
   aws s3 cp ${script_dir}/transformation-outputs/OceaniaEmissionsEdited.parquet/ s3://${bucket_name}/data-transformation/OceaniaEmissionsEdited.parquet/ --recursive

}

TARGET=${1:-}
if type -t "goal_${TARGET}" &>/dev/null; then
  "goal_${TARGET}" ${@:2}
else
  echo "Usage: $0 <goal>

goal:
    upload-data                    - Fetches and uploads data-transformation files to AWS S3 bucket
"
  exit 1
fi
