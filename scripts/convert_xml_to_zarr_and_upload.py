import json
import logging
from pathlib import Path
import xml.etree.ElementTree as ET

import click
import shutil
import requests
from pydantic import BaseSettings


from bia_integrator_tools.io import copy_local_zarr_to_s3
from bia_integrator_tools.conversion import run_zarr_conversion
from bia_integrator_core.integrator import load_and_annotate_study
from bia_integrator_core.models import BIAImageRepresentation
from bia_integrator_core.interface import persist_image_representation


logger = logging.getLogger(__file__)


def copy_uri_to_local(src_uri: str, dst_fpath: Path):
    """Copy the object at the given source URI to the local path specified by dst_fpath."""

    logger.info(f"Fetching {src_uri} to {dst_fpath}")

    with requests.get(src_uri, stream=True) as r:
        with open(dst_fpath, "wb") as fh:
            shutil.copyfileobj(r.raw, fh)


@click.command()
@click.option("--accession-id", help="Accession number of the study e.g. S-BIAD229")
@click.option("--study-ids", help="Comma separated list of IDs. 1st should be to xml")
@click.option("--output-id", help="ID to associate with output file")
@click.option("--save-to-file", is_flag=True, default=False, help="Save representation to file")
def main(accession_id, study_ids, output_id, save_to_file):

    logging.basicConfig(level=logging.INFO)

    bia_study = load_and_annotate_study(accession_id)

    dst_dir_basepath = Path("tmp/c2z")/accession_id
    dst_dir_basepath.mkdir(exist_ok=True, parents=True)

    for i, study_id in  enumerate(study_ids.split(",")):
        if study_id.startswith("IM"):
            study_file = bia_study.images[study_id]
        elif study_id.startswith("A"):
            study_file = bia_study.archive_files[study_id]
        elif study_id.startswith("O"):
            study_file = bia_study.other_files[study_id]
        else:
            raise Exception(f"study_id {study_id} expected to begin with 'IM', 'A', or 'O'! Exiting")

        # For XMLs, bioformats2raw expects image/data file with same name in 
        # same dir as XML file - check!!!
        suffix = study_file.original_relpath.suffix
        dst_fpath = dst_dir_basepath/f"{study_id}{suffix}"
        #dst_fpath = dst_dir_basepath/f"{study_file.original_relpath}"

        # FIXME - this should check for the correct representation type, not assume it's the first one
        src_uri = study_file.representations[0].uri
        # TODO - Also check size!!!
        if not dst_fpath.exists():
            copy_uri_to_local(src_uri, dst_fpath)

        # Save first Study ID as source for bioformats2raw
        if i == 0:
            b2raw_src_fpath = dst_fpath

    zarr_fpath = dst_dir_basepath/f"{output_id}.zarr"
    if not zarr_fpath.exists():
        run_zarr_conversion(b2raw_src_fpath, zarr_fpath)

    # Copy to S3
    zarr_image_uri = copy_local_zarr_to_s3(zarr_fpath, accession_id, output_id)

    # Save representation to local cache
    representation = BIAImageRepresentation(
        accession_id=accession_id,
        image_id=output_id,
        size=0,
        type="ome_ngff",
        uri=zarr_image_uri,
        dimensions=None,
        rendering=None,
        attributes={
            "study_artefacts": study_ids
        }
    )

    if not save_to_file:
        persist_image_representation(representation)
    else:
        fname = f"{accession_id}-{image_id}.json"
        logger.info(f"Saving to {fname}")
        with open(fname, "w") as fh:
            fh.write(representation.json(indent=2))
        



if __name__ == "__main__":
    main()
