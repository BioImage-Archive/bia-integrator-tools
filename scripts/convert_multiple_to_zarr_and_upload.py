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


def update_channel_names_zattrs(zattrs_path: Path, channel_names: list):
    """Update the names of channels in .zattrs

    """
    n_channels = len(channel_names)
    if not zattrs_path.is_file():
        logger.error(f"Could not find {zattrs_path}. Channel details not updated in .zattrs")
        return
        
    zattrs = json.loads(zattrs_path.read_text())
    try:
        n_zarr_channels = len(zattrs["omero"]["channels"])
    except KeyError:
        n_zarr_channels = 0
    except Exception as err:
        logger.error(f"An error occured. Channel details not updated in .zattrs. Error was {err}")
        return

    if n_zarr_channels != n_channels:
        logger.error(f"Expected {n_channels} channels, got {n_zarr_channels}: Channel details not updated in {zattrs_path}")
        return

    for channel, channel_name in enumerate(channel_names):
        zattrs["omero"]["channels"][channel]["label"] = channel_name

    zattrs_path.write_text(json.dumps(zattrs, indent=2))
    logger.info(f"Updated channel details in {zattrs_path}")

def update_channel_names_xml(xml_path: Path, channel_names: list):
    """Update the names of channels in OME xml file

    """
    n_channels = len(channel_names)
    if not xml_path.is_file():
        logger.error(f"Could not find {xml_path}. Channel details not updated.")
        return
        
    element_tree = ET.parse(xml_path)
    root = element_tree.getroot()
    xml_channels = [descendant for descendant in root.iter() if descendant.tag.endswith("Channel")]

    n_xml_channels = len(xml_channels)
    if n_xml_channels != n_channels:
        logger.error(f"Expected {n_channels} channels, got {n_xml_channels}: Channel details not updated in {xml_path}")
        return

    for xml_channel, channel_name in zip(xml_channels, channel_names):
        xml_channel.set("Name", channel_name)

    element_tree.write(xml_path)
    logger.info(f"Updated channel details in {xml_path}")
    

def copy_uri_to_local(src_uri: str, dst_fpath: Path):
    """Copy the object at the given source URI to the local path specified by dst_fpath."""

    logger.info(f"Fetching {src_uri} to {dst_fpath}")

    with requests.get(src_uri, stream=True) as r:
        with open(dst_fpath, "wb") as fh:
            shutil.copyfileobj(r.raw, fh)


@click.command()
@click.option("--accession-id", help="Accession number of the study e.g. S-BIAD229")
@click.option("--image-ids", help="Comma separated list of IDs to merge e.g. IM1,IM2,IM3")
@click.option("--pattern-file", help="Path to file containing pattern to base merge on")
@click.option("--output-id", help="ID to associate with merged file")
@click.option("--channel-details", help="Path to text file containing channel details - one per line")
@click.option("--save-to-file", is_flag=True, default=False, help="Save representation to file")
def main(accession_id, image_ids, pattern_file, channel_details, output_id, save_to_file):

    logging.basicConfig(level=logging.INFO)

    bia_study = load_and_annotate_study(accession_id)

    dst_dir_basepath = Path("tmp/c2z")/accession_id
    dst_dir_basepath.mkdir(exist_ok=True, parents=True)

    for image_id in  image_ids.split(","):
        image = bia_study.images[image_id]

        image_suffix = image.original_relpath.suffix
        # For the case of merging, we need to keep original names or ensure
        # Image IDs are in same order as original names. Option 2 makes
        # for an easier workflow - as patterns can be determined 
        # automatically.
        #dst_fpath = dst_dir_basepath/f"{image_id}{image_suffix}"
        dst_fpath = dst_dir_basepath/f"{image.original_relpath}"

        # FIXME - this should check for the correct representation type, not assume it's the first one
        src_uri = image.representations[0].uri
        if not dst_fpath.exists():
            copy_uri_to_local(src_uri, dst_fpath)

    # Copy pattern file to same dir as downloaded images
    pattern = Path(pattern_file).read_text().strip("\n")
    dst_fpath = dst_dir_basepath/Path(pattern_file).name
    dst_fpath.write_text(pattern)

    zarr_fpath = dst_dir_basepath/f"{output_id}.zarr"
    if not zarr_fpath.exists():
        run_zarr_conversion(dst_fpath, zarr_fpath)

    # Update channel info before copying to S3
    channel_names = [
        c.strip() for c in Path(channel_details).read_text().split("\n") if len(c) > 0
    ]
    update_channel_names_xml(zarr_fpath/"OME"/"METADATA.ome.xml", channel_names)
    update_channel_names_zattrs(zarr_fpath/"0"/".zattrs", channel_names)

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
            "study_artefacts": image_ids
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
